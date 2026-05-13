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
# to bring it back. By "30s" we leave a generous margin over the 5s
# observer interval (5s tick + apply latency + adjust + WFC handshake).
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
# pkg/satellite/reconciler.go::applyDRBD runs `drbdadm adjust <rd>`
# unconditionally as long as a `.md-created` marker exists for the
# RD. After `drbdadm down`, the kernel slot is gone but the marker
# survives, so the reconciler retries `adjust` — which then fails
# with "Failure: (158) Unknown resource" because adjust expects
# the resource to already be loaded. The reconciler never falls
# back to `drbdadm up` / `new-resource`, so the resource stays
# down indefinitely. See reconciler_drbd_test.go:1606-1615 for
# the same gap noted under a different scenario (Bug 8 / 5.16).
# Scenario 5.32 therefore FAILs on master today and serves as the
# forward-spec for the missing kernel-state-aware revive path.
#
# Steps
#   1. Apply 2-replica RD on $N1+$N2, wait UpToDate.
#   2. Pick Secondary ($N2) — `drbdadm down $RD` from its satellite pod.
#   3. Confirm kernel is empty for $RD on $N2 (`drbdsetup status`).
#   4. Poll up to 30s for kernel to reappear on $N2.
#   5. Assert peer state returns to Connected + UpToDate within 60s.
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
REVIVE_DEADLINE_SECS=30
UPTODATE_DEADLINE_SECS=60

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
echo ">> wait <=${REVIVE_DEADLINE_SECS}s for reconciler to re-create ${RD} on ${N2}"
t_down=$(date +%s)
revived_at=0
deadline=$(( t_down + REVIVE_DEADLINE_SECS ))
while (( $(date +%s) < deadline )); do
    out=$(on_node "$N2" drbdsetup status "$RD" 2>/dev/null || true)
    if [[ -n "$out" && "$out" != *"No currently configured DRBD found"* ]]; then
        revived_at=$(( $(date +%s) - t_down ))
        break
    fi
    sleep 1
done

if (( revived_at == 0 )); then
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
# already-synced device — no data movement, just metadata.
echo ">> wait <=${UPTODATE_DEADLINE_SECS}s for ${RD} to reach Connected+UpToDate on both peers"
deadline=$(( $(date +%s) + UPTODATE_DEADLINE_SECS ))
connected_at=0
while (( $(date +%s) < deadline )); do
    n1_view=$(on_node "$N1" drbdsetup status "$RD" --verbose 2>/dev/null || true)
    n2_view=$(on_node "$N2" drbdsetup status "$RD" --verbose 2>/dev/null || true)
    n1_conn=$(echo "$n1_view" | grep -oE 'connection:[A-Za-z]+' | head -1 | cut -d: -f2)
    n2_conn=$(echo "$n2_view" | grep -oE 'connection:[A-Za-z]+' | head -1 | cut -d: -f2)
    n2_local_disk=$(echo "$n2_view" | grep -E 'disk:UpToDate' | head -1 || true)
    n1_local_disk=$(echo "$n1_view" | grep -E 'disk:UpToDate' | head -1 || true)
    if [[ "$n1_conn" == "Connected" && "$n2_conn" == "Connected" \
          && -n "$n1_local_disk" && -n "$n2_local_disk" ]]; then
        connected_at=$(( $(date +%s) - t_down ))
        break
    fi
    sleep 2
done

if (( connected_at == 0 )); then
    echo "FAIL: ${RD} did not reach Connected+UpToDate within ${UPTODATE_DEADLINE_SECS}s"
    echo "      ${N1} view:"; on_node "$N1" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/      /' || true
    echo "      ${N2} view:"; on_node "$N2" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/      /' || true
    exit 1
fi

echo ">> PASS 5.32 — drbdadm down auto-reverted in ${revived_at}s; UpToDate restored in ${connected_at}s"
