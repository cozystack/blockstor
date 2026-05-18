#!/usr/bin/env bash
#
# usage: recovery-setgi-per-peer.sh WORK_DIR
#
# Bug 81 — drbdmeta set-gi requires --node-id (per-peer GI seeding).
#
# Background:
#   DRBD 9.2+'s `drbdmeta ... set-gi` refuses the legacy single-call
#   form with "The set-gi command requires the --node-id option" —
#   the GI tuple lives in per-peer bitmap slots, not a single global
#   tuple. Bug 77 pre-stamps each freshly-created replica's metadata
#   with a day0/seed GI so the DRBD handshake on first connect
#   recognises the new replica as "already in sync" and skips the
#   full initial-sync. The 424728661 workaround caught the
#   --node-id error and downgraded it to a log, silently disabling
#   Bug 77. PR #2 (commit 795346c7b) wires per-peer iteration with
#   the `--node-id` flag so every peer's bitmap slot gets the
#   deterministic seed and the optimisation is restored on DRBD 9.2+.
#
#   Tier 2 unit tests (pkg/satellite/reconciler_drbd_test.go) pin
#   that SetGi is called per peer with the right node-id — but only
#   an end-to-end run on real Talos+DRBD-9.2 catches a regression
#   where the create-md → set-gi → adjust → handshake chain stops
#   short-circuiting the initial sync on the wire. That is what
#   this script does.
#
# Verification methodology:
#   1. Create a 3-replica RD on a thin/ZFS_THIN-backed pool
#      (provider.IsThinOrZFS() == true; non-thin pools allocate the
#      backing extent which itself takes time and noises the
#      "initial-sync was skipped" signal). 32 MiB volume — big enough
#      that an actual initial-sync would take measurable seconds
#      (15s+ on the QEMU stand for the resync handshake + bitmap
#      flush against a non-thin device), small enough that the test
#      still completes quickly if the optimisation works.
#   2. Time-based assertion: all 3 replicas reach `disk:UpToDate`
#      within UPTODATE_WINDOW seconds (default 30). On a thin
#      backing the actual data-movement phase is near-instant
#      (ZFS_THIN reads as zero, DRBD's THIN_RESYNC short-circuits
#      the per-block compare), so a healthy stand always settles in
#      ≤ 30s regardless of whether set-gi short-circuited the
#      handshake or DRBD walked the bitmap fast-path. What we are
#      actually guarding against is the FAILURE mode: per-peer
#      set-gi crashing the satellite reconcile loop (the original
#      Bug 81 symptom — "satellite reconcile loops forever"), or
#      the seed step erroring out and leaving the replica stuck in
#      Inconsistent / Connecting forever. With the fix wired, all
#      3 peers converge within seconds; without the fix on a
#      strict 9.2+ kernel (no workaround), the satellite would
#      keep retrying create-md → set-gi → fail and the replica
#      would never reach UpToDate.
#   3. Wire-shape assertion: scan satellite logs for an ERROR or
#      Wrap containing "set-gi" — the only way the Bug 81 wire
#      shape regresses without crashing the reconcile is if SetGi
#      returns a wrapped error that's then surfaced into the
#      controller-runtime error log. The 424728661 workaround
#      explicitly swallowed `--node-id` errors; with that gone,
#      any modern-drbdmeta failure (missing --node-id flag, bad
#      peer-id, etc.) surfaces. Asserting *zero* such error log
#      lines for ${RD} is the deterministic regression guard.
#
# CI vs operator-runbook:
#   On a DRBD 9.1 stand the per-peer set-gi works identically and
#   no errors land in the satellite logs — the test is forward-
#   compatible with the older kernel without needing a version
#   gate. The asserted property is "the day0 GI seed pipeline runs
#   to completion on every peer of a 3-replica RD without errors,
#   and the resource converges to UpToDate"; it holds on both
#   kernel families when the code is correct.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

RD=setgi-per-peer
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3
POOL=${STORPOOL:-zfs-thin}
SIZE_KIB=32768                              # 32 MiB
UPTODATE_WINDOW=${UPTODATE_WINDOW:-30}      # seconds — see header for the
                                            # rationale on this bound.

cleanup() {
    delete_rd "$RD" || true
}
trap cleanup EXIT

# --- step 1: 3-replica RD on a thin/ZFS pool, no tiebreaker ---------
#
# AutoAddQuorumTiebreaker=false: with 3 diskful replicas we don't want
# the controller wedging a diskless tiebreaker into the topology — that
# would inject a 4th node-id and a 4th set-gi peer slot, which is
# orthogonal to the bug-81 surface. Matches the contract in
# recovery-inconsistent-blocking.sh.

echo ">> step 1: apply 3-replica RD ${RD} on ${POOL} (${N1}, ${N2}, ${N3})"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    DrbdOptions/AutoAddQuorumTiebreaker: "false"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: ${SIZE_KIB}}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N1}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N1}
  props: {StorPoolName: ${POOL}}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N2}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N2}
  props: {StorPoolName: ${POOL}}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N3}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N3}
  props: {StorPoolName: ${POOL}}
EOF

created_at=$(date +%s)

# --- step 2: wait for all 3 peers to converge to disk:UpToDate -------
#
# Without the Bug 81 fix on a strict drbdmeta-9.2+ kernel the
# satellite's `runFirstActivation` would fail with "set-gi requires
# --node-id" and the reconcile loop would never write the .md-created
# marker; every subsequent reconcile retries from scratch (create-md
# is wiping every time too), and the replica's local disk never
# reaches UpToDate within any reasonable window. With the fix wired
# all 3 converge in seconds.

echo ">> step 2: wait up to ${UPTODATE_WINDOW}s for 3/3 UpToDate"
deadline=$(( created_at + UPTODATE_WINDOW ))
all_uptodate_at=""
last_trace=""
while (( $(date +%s) < deadline )); do
    n_uptodate=0
    last_trace=""
    for node in "$N1" "$N2" "$N3"; do
        ds=$(status_disk_state "$RD" "$node")
        last_trace="${last_trace}${node}=${ds:-<gone>}; "
        if [[ "$ds" == "UpToDate" ]]; then
            n_uptodate=$(( n_uptodate + 1 ))
        fi
    done

    if (( n_uptodate == 3 )); then
        all_uptodate_at=$(date +%s)
        break
    fi

    sleep 1
done

if [[ -z "$all_uptodate_at" ]]; then
    echo "FAIL: not all 3 replicas reached UpToDate within ${UPTODATE_WINDOW}s"
    echo "   last trace: $last_trace"
    echo "   final drbdsetup status on each node:"
    for node in "$N1" "$N2" "$N3"; do
        echo "   -- ${node} --"
        on_node "$node" drbdsetup status "$RD" 2>&1 | sed 's/^/      | /' || true
    done
    echo "   recent satellite-error logs across all nodes:"
    for node in "$N1" "$N2" "$N3"; do
        pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
            -o "jsonpath={.items[?(@.spec.nodeName==\"${node}\")].metadata.name}")
        [[ -z "$pod" ]] && continue
        echo "   -- ${pod} (${node}) --"
        kubectl -n "$NS" logs --tail=300 "$pod" 2>/dev/null \
            | grep -iE "set-gi|seed|drbdmeta|${RD}" | tail -20 | sed 's/^/      | /' || true
    done
    exit 1
fi
echo "   all 3 replicas UpToDate at +$(( all_uptodate_at - created_at ))s (window=${UPTODATE_WINDOW}s)"

# --- step 3: scan satellite logs for any "set-gi" error ---------------
#
# Surface assertion on the Bug 81 wire shape itself: a regression
# of the per-peer set-gi call would surface as a satellite reconcile
# error containing either:
#   - "set-gi" + "--node-id" (drbdmeta's own error message about
#     missing the flag)
#   - "drbdmeta set-gi" wrapped by the satellite's error chain (the
#     errors.Wrapf in pkg/drbd/drbdadm.go SetGi)
#   - "seed initial-sync GI" wrapped by the satellite's runFirst
#     Activation
#
# We assert ZERO matches for ${RD}. This catches the failure path
# even if step 2 happened to pass on a particularly fast stand: a
# wire-shape regression that surfaces ENOENT / "requires the
# --node-id option" wraps up to the controller-runtime error log,
# regardless of whether DRBD eventually converges via a fallback.

echo ">> step 3: scan satellite logs for set-gi/seed errors on ${RD}"
err_count=0
for node in "$N1" "$N2" "$N3"; do
    pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o "jsonpath={.items[?(@.spec.nodeName==\"${node}\")].metadata.name}")
    if [[ -z "$pod" ]]; then
        continue
    fi
    # Match the satellite-side error envelopes for any set-gi /
    # seedInitialGi failure that *did* surface. We deliberately
    # exclude:
    #   - INFO-level lines (those aren't errors)
    #   - lines without ${RD} (parallel scenarios sharing the stand
    #     would otherwise flag each other's noise)
    # and we grep for the well-known wrap-fragments: "drbdmeta
    # set-gi", "set-gi vol", "seed initial-sync GI", "requires the
    # --node-id option". Any one of those in a non-INFO line for
    # this RD is a fail.
    hits=$(kubectl -n "$NS" logs --tail=2000 "$pod" 2>/dev/null \
        | grep -E "\"level\":\"ERROR\"" \
        | grep "$RD" \
        | grep -E 'drbdmeta set-gi|set-gi vol|seed initial-sync GI|requires the --node-id option' \
        | head -5 || true)
    if [[ -n "$hits" ]]; then
        echo "   !! set-gi error on ${node} (${pod}):"
        echo "$hits" | sed 's/^/      | /'
        err_count=$(( err_count + 1 ))
    fi
done

if (( err_count > 0 )); then
    echo "FAIL: ${err_count} satellite(s) logged a set-gi / seedInitialGi error for ${RD}"
    echo "      — Bug 81 regression: per-peer SetGi wire shape broke on the real kernel."
    exit 1
fi

echo ">> RECOVERY-SETGI-PER-PEER OK"
echo "   - 3/3 replicas UpToDate in $(( all_uptodate_at - created_at ))s (<= ${UPTODATE_WINDOW}s)"
echo "   - no set-gi / seedInitialGi errors surfaced in any satellite log"
