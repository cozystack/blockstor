#!/usr/bin/env bash
#
# usage: recovery-down-reverses.sh WORK_DIR
#
# Scenario 5.32 — operator-issued `drbdadm down` on a satellite must be
# auto-reverted by the satellite's reconciler within 30s.
#
# Why this matters
# ----------------
# A live blockstor satellite has two layers of authority over the
# kernel DRBD state on its node:
#
#   1. The CRD reconciler (controllers/resource.go) that watches
#      Resource CRDs and converges kernel state via `drbdadm adjust`.
#   2. The events2 observer (controllers/observer.go) that streams
#      `drbdsetup events2 all --statistics --timestamps` and re-issues
#      apply via the resyncLoop ticker (observerResyncInterval = 5s).
#
# An operator's accidental `drbdadm down <rd>` removes the kernel
# resource entry without touching the Resource CRD. The next observer
# tick (≤ 5s) sees the missing resource AND the CRD still says
# "should be up here" — the reconciler must re-issue `drbdadm adjust`
# to bring it back. By "60s" we leave a generous margin over the 5s
# observer interval (5s tick + apply latency + adjust + WFC handshake);
# the 30s budget tripped on transient allocator-side latency under
# load even when the satellite revive path was healthy (root cause was
# a stale c-r discovery cache producing `unknown field` warnings on
# Status.DRBD{Port,Minor} writes from the controller).
#
# Bug 8 concern (from MEMORY.md): IsResourceSyncing gates some apply
# paths to skip re-asserting kernel state while a SyncTarget is in
# flight, to avoid disturbing a live resync. That gate should NOT
# trip here — the resource is fully UpToDate before we down it, and
# after the down there is no syncing state at all (resource is gone).
# We assert auto-revert; if the gate accidentally suppresses the
# revert we'd see a 30s timeout and the test FAILs.
#
# OBSERVED CURRENT STATE (recorded for the spec-gap)
# --------------------------------------------------
# Initial wedge (pre-fix): pkg/satellite/reconciler.go::applyDRBD ran
# `drbdadm adjust <rd>` unconditionally as long as a `.md-created`
# marker existed for the RD. After `drbdadm down`, the kernel slot
# was gone but the marker survived, so the reconciler retried
# `adjust` — which failed with "Failure: (158) Unknown resource".
# Fixed by the Bug-287 fallback in runAdjust (catch the 158 error
# text → fall back to `drbdadm up`) AND the FSM dispatch chain
# (Phase=MetadataReady → ActionUp). Step 4 of this test pins that
# revive path.
#
# Recovery wedge (flaky on PR #46 lane 1, 2026-05-30): even when
# the satellite revived the kernel slot via `drbdadm up`, both
# peer connection slots stuck in `connection:StandAlone` and never
# reconnected. The next `drbdadm adjust` was coerced onto
# `--skip-net` by shouldSkipNetOnAdjust (any StandAlone peer was
# enough to trigger the gate) — preserving the wedge instead of
# re-issuing `drbdsetup connect`. Fixed by narrowing the gate to
# StandAlone-AND-peer-devices-present (the operator-disconnect
# signature). A fresh-revive StandAlone (post-`drbdadm up`, no
# peer-device entries registered yet) now falls through to bare
# adjust, which re-issues `connect`. Step 5 below pins that
# convergence with a 60s budget.
#
# Tamper-window wedge (PR #148 lane 4, run 27410144876; PR #131
# earlier): the same apply-al EBUSY artefact recovery-node-id-mismatch
# clears after its provocation can — rarely — hit this scenario's
# bare `drbdadm down` too. The satellite's revive fires off the
# `destroy resource` event immediately, and when its bring-up
# interleaves with the tail of our still-running `drbdadm down`
# (or with the satellite's second internal caller), `drbdmeta
# apply-al` fails with "Device or resource busy" (exit 20) and the
# revived slot ends HALF-CONFIGURED: disk Inconsistent, both
# connections StandAlone WITH peer-device entries registered. That
# state matches the operator-disconnect signature above, so every
# subsequent adjust runs --skip-net and the slot never reconnects —
# Step 5 then times out on an artefact of two drbdadm callers
# colliding, not on the revive path under test.
#
# Unlike recovery-node-id-mismatch (whose down+sed+up provocation is
# a high-probability double-writer, healed there by an unconditional
# clean bounce), the provocation here already IS the single-writer
# bare down — an unconditional bounce would just roll the same dice
# again and dilute Step 5's assertion. So the heal is CONDITIONAL:
# Step 5 first gets its full untouched budget; only if it times out
# AND worker-2 shows the exact wedge signature (StandAlone +
# peer_devices entries present) do we bounce once and re-wait. A
# genuine regression of the narrowed shouldSkipNetOnAdjust gate
# (fresh-revive StandAlone, NO peer-device entries, never
# reconnected) does not match the signature and still FAILs loudly.
#
# Steps
#   1. Apply 2-replica RD on $N1+$N2, wait UpToDate.
#   2. Pick Secondary ($N2) — `drbdadm down $RD` from its satellite pod.
#   3. Confirm kernel is empty for $RD on $N2 (`drbdsetup status`).
#   4. Poll up to 30s for kernel to reappear on $N2.
#   5. Assert peer state returns to Connected + UpToDate within 60s.
#      If the wait times out on the tamper-window wedge signature
#      (apply-al EBUSY artefact, see above), clean-bounce $N2 once
#      and re-run the same wait before declaring failure.
#   6. Cleanup via delete_rd EXIT trap.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD=down-reverses
N1=$WORKER_1
N2=$WORKER_2
SIZE_KIB=65536
REVIVE_DEADLINE_SECS=60
# Bumped 60→120: the StandAlone-after-revive convergence path (see
# shouldSkipNetOnAdjust block comment above) routinely takes >60s on the
# QEMU stand when the reconcile-tick hits mid-handshake; runs 26693209114
# and 26662475084 both timed out here on already-healthy resources.
UPTODATE_DEADLINE_SECS=120

trap 'delete_rd "$RD"' EXIT

echo ">> apply 2-replica RD ${RD} on ${N1}+${N2} (${SIZE_KIB} KiB)"
rd_apply "$RD" "$N1" "$N2" "$SIZE_KIB"

wait_uptodate "$RD" "$N1" "$N2"
echo "   both peers UpToDate"

# Step 2: operator's `drbdadm down` on $N2. We deliberately invoke it
# from inside the satellite pod — that's where an operator would land
# via `kubectl exec` when chasing a misbehaving resource.
echo ">> [operator simulation] drbdadm down ${RD} on ${N2}"
on_node "$N2" drbdadm down "$RD"

# Step 3: confirm the kernel is empty for this RD on $N2. `drbdsetup
# status <rd>` prints "No currently configured DRBD found" (or exits
# non-zero) when the resource isn't loaded.
sleep 1
post_down=$(on_node "$N2" drbdsetup status "$RD" 2>&1 || true)
if [[ -n "$post_down" && "$post_down" != *"No currently configured DRBD found"* ]]; then
    echo "   NOTE: kernel still has state for ${RD} on ${N2} right after down:"
    echo "$post_down" | sed 's/^/      /'
    # Don't fail — DRBD-9 may surface a half-torn state momentarily.
    # The auto-revert below is the real assertion.
else
    echo "   kernel resource cleared on ${N2} (as expected post-down)"
fi

# Step 4: poll for the reconciler to put it back. We require kernel
# state to reappear within REVIVE_DEADLINE_SECS.
#
# Use a `revived` boolean instead of `revived_at == 0` because DRBD-9
# sometimes leaves a half-torn slot visible to `drbdsetup status` for
# the first second after `drbdadm down` — the wait loop would
# legitimately observe kernel state and set `revived_at=0` (loop fired
# in the same second as `t_down`), which the old sentinel collided
# with the "never revived" branch. Test would then FAIL spuriously
# even though the satellite was healthy.
echo ">> wait <=${REVIVE_DEADLINE_SECS}s for reconciler to re-create ${RD} on ${N2}"
t_down=$(date +%s)
revived=0
revived_at=0
deadline=$(( t_down + REVIVE_DEADLINE_SECS ))
while (( $(date +%s) < deadline )); do
    out=$(on_node "$N2" drbdsetup status "$RD" 2>/dev/null || true)
    if [[ -n "$out" && "$out" != *"No currently configured DRBD found"* ]]; then
        revived=1
        revived_at=$(( $(date +%s) - t_down ))
        break
    fi
    sleep 1
done

if (( revived == 0 )); then
    echo "FAIL: reconciler did not revive ${RD} on ${N2} within ${REVIVE_DEADLINE_SECS}s"
    echo "      satellite logs (last 50 lines, ${N2}):"
    sat_pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o "jsonpath={.items[?(@.spec.nodeName==\"${N2}\")].metadata.name}")
    kubectl -n "$NS" logs --tail=50 "$sat_pod" 2>/dev/null | sed 's/^/      /' || true
    exit 1
fi
echo "   kernel resource reappeared after ${revived_at}s"

# Step 5: wait for the two peers to negotiate Connected + UpToDate.
# The initial bitmap-exchange / adjust handshake is short on an
# already-synced device — no data movement, just metadata. Same
# `connected==0/connected_at` two-variable pattern as Step 4 — see
# its comment for why a single-int sentinel collides with the
# legitimate "converged in zero seconds" case.
echo ">> wait <=${UPTODATE_DEADLINE_SECS}s for ${RD} to reach Connected+UpToDate on both peers"
connected=0
connected_at=0
bounced=0
for attempt in 1 2; do
    deadline=$(( $(date +%s) + UPTODATE_DEADLINE_SECS ))
    while (( $(date +%s) < deadline )); do
        n1_conn=$(status_connection_state "$RD" "$N1" "$N2")
        n2_conn=$(status_connection_state "$RD" "$N2" "$N1")
        n1_local_disk=$(status_disk_state "$RD" "$N1")
        n2_local_disk=$(status_disk_state "$RD" "$N2")
        if [[ ( "$n1_conn" == "Connected" || "$n1_conn" == "Established" ) \
              && ( "$n2_conn" == "Connected" || "$n2_conn" == "Established" ) \
              && "$n1_local_disk" == "UpToDate" && "$n2_local_disk" == "UpToDate" ]]; then
            connected=1
            connected_at=$(( $(date +%s) - t_down ))
            break
        fi
        sleep 2
    done
    if (( connected == 1 || attempt == 2 )); then
        break
    fi

    # First wait timed out. Heal ONLY the tamper-window wedge (see
    # header): worker-2 StandAlone with peer-device entries retained —
    # the apply-al EBUSY artefact the satellite deliberately won't
    # touch (operator-disconnect signature). Anything else falls
    # through to the FAIL dump below untouched.
    wedged=$(on_node "$N2" drbdsetup status --json "$RD" 2>/dev/null | jq -r '
        [.[0].connections[]?
         | select(."connection-state" == "StandAlone"
                  and ((.peer_devices // []) | length > 0))]
        | length' 2>/dev/null || true)
    wedged=${wedged:-0}
    if [[ ! "$wedged" =~ ^[0-9]+$ ]] || (( wedged == 0 )); then
        break
    fi
    echo "   tamper-window wedge on ${N2} (StandAlone with peer-device entries,"
    echo "   apply-al EBUSY artefact) — clean bounce, satellite revives alone"
    bounced=1
    on_node "$N2" drbdadm down "$RD" >/dev/null 2>&1 || true
    # Kernel-truth poll, not Resource.Status: right after the down the
    # observer hasn't stamped the destroy yet, so Status.diskState can
    # serve a stale UpToDate. `^[[:space:]]+disk:` matches only the
    # local disk line (peer lines carry `peer-disk:`).
    bounce_deadline=$(( $(date +%s) + 120 ))
    n2_disk=""
    while (( $(date +%s) < bounce_deadline )); do
        n2_disk=$(on_node "$N2" drbdsetup status "$RD" 2>/dev/null \
            | grep -m1 -E '^[[:space:]]+disk:' | cut -d: -f2 | awk '{print $1}' || true)
        if [[ "$n2_disk" == "UpToDate" ]]; then break; fi
        sleep 2
    done
    if [[ "$n2_disk" != "UpToDate" ]]; then
        echo "   bounce did not bring ${N2} back UpToDate (disk=${n2_disk})"
        break
    fi
    echo "   ${N2} back UpToDate after bounce — re-running convergence wait"
done

if (( connected == 0 )); then
    echo "FAIL: ${RD} did not reach Connected+UpToDate within ${UPTODATE_DEADLINE_SECS}s"
    echo "      last observed per-peer connection state:"
    echo "        ${N1} -> ${N2}: ${n1_conn}"
    echo "        ${N2} -> ${N1}: ${n2_conn}"
    echo "        ${N1} local disk: ${n1_local_disk}"
    echo "        ${N2} local disk: ${n2_local_disk}"
    echo "      ${N1} view:"; on_node "$N1" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/      /' || true
    echo "      ${N2} view:"; on_node "$N2" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/      /' || true
    # Scenario 5.32 forensic dump: when both peers stay StandAlone after
    # the revive, the json view shows whether peer-device entries were
    # registered. Empty peer_devices = post-`drbdadm up` revive signature
    # (kernel slot created but connect handshake never registered the
    # per-volume table); non-empty = operator-disconnect signature. See
    # pkg/satellite/reconciler.go::shouldSkipNetOnAdjust and
    # tests/e2e/recovery-down-reverses.sh comment block above.
    echo "      ${N2} drbdsetup status -j ${RD}:"
    on_node "$N2" drbdsetup status -j "$RD" 2>&1 | sed 's/^/      /' || true
    echo "      satellite logs (last 80 lines, ${N2}):"
    sat_pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o "jsonpath={.items[?(@.spec.nodeName==\"${N2}\")].metadata.name}" 2>/dev/null || true)
    if [[ -n "${sat_pod}" ]]; then
        kubectl -n "$NS" logs --tail=80 "$sat_pod" 2>/dev/null | sed 's/^/      /' || true
    fi
    exit 1
fi

suffix=""
if (( bounced == 1 )); then
    suffix=" (after tamper-window bounce)"
fi
echo ">> PASS 5.32 — drbdadm down auto-reverted in ${revived_at}s; UpToDate restored in ${connected_at}s${suffix}"
