#!/usr/bin/env bash
#
# usage: split-brain-recovery.sh WORK_DIR
#
# Scenario 5.W12 (wave2-05) — split-brain manual recovery: pin the
# operator-runnable recipe contract.
#
# This test is the wave2 P0 counterpart of wave1 5.14
# (recovery-discard-my-data.sh). 5.14 validates the
# `--discard-my-data` recipe end-to-end with a data-marker
# round-trip; 5.W12 pins the *recipe contract itself* — that the
# two-sided documented commands (VICTIM + SURVIVOR) execute
# cleanly against a reconciler-managed RD without blockstor
# fighting back by re-rendering `.res` or re-issuing
# `drbdadm adjust` mid-recipe.
#
# Wave2-05 §5.W12 documents the recipe verbatim:
#
#   VICTIM (loser, side whose data is discarded):
#     drbdadm disconnect <rd>
#     drbdadm secondary <rd>
#     drbdadm -- --discard-my-data connect <rd>
#
#   SURVIVOR (winner, if it also went StandAlone):
#     drbdadm disconnect <rd>
#     drbdadm connect <rd>
#
# Cross-listed with wave1 5.14 (e2e data-marker variant) and
# wave1 5.31 (split-brain *detection*; this scenario covers the
# *recovery* commands' contract).
#
# Setup:
#   - 2-replica RD on workers 1+2, 64 MiB, AutoAddQuorumTiebreaker=false.
#     No third diskless witness — a tiebreaker would arbitrate the
#     split for us and the recipe would never have to run.
#   - Promote $N1 Primary so split-brain has a clear winner side.
#
# Steps:
#   1. Force both halves into StandAlone via `drbdsetup disconnect
#      --force=yes` on each side. `drbdadm disconnect` alone is racy
#      because blockstor's reconciler re-issues `drbdadm adjust` and
#      the connection reopens within ~1 s. The --force=yes variant
#      transitions the FSM into a StandAlone state that `drbdadm
#      adjust` cannot silently undo — see recovery-discard-my-data.sh
#      for the same provocation idiom.
#   2. Snapshot `.res` content + mtime on BOTH satellites — the
#      reconciler-survival guard hinges on the file NOT being
#      rewritten during the recipe window.
#   3. Apply VICTIM recipe on $N2 (literal commands from W12).
#   4. Apply SURVIVOR recipe on $N1 (literal commands from W12).
#   5. Within $RECOVERY_WINDOW seconds, both peers walk back to
#      connection:{Established,Connected} + disk:UpToDate.
#   6. Re-snapshot `.res` content + mtime on both satellites; assert
#      content identical to step 2's snapshot (mtime drift is allowed
#      — DRBD adjust paths can `touch` without rewriting — but a
#      content diff means the reconciler clobbered the operator's
#      side selection mid-recipe).
#   7. $N1 stays Primary throughout (regression guard — losing
#      Primary-ship during the recovery would have interrupted any
#      in-flight consumer in production).
#
# Distinct from recovery-discard-my-data.sh:
#   - 5.14 validates data integrity via md5 round-trip (the "does
#     the discard direction work?" question).
#   - 5.W12 validates the *command contract* and the
#     reconciler-survival guard via `.res` content stability (the
#     "is the documented recipe still runnable as-is?" question).
#
# Bash 4+ required for `mapfile` (sourced from lib.sh). The QEMU stand
# ships bash 5.x; the host runner is whatever brew installed (5.2+).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD=e2e-5-w12-split-brain
N1=$WORKER_1
N2=$WORKER_2
RECOVERY_WINDOW=30   # generous — both sides have to walk
                     # StandAlone → Connecting → Connected →
                     # (Sync*)? → UpToDate; 64 MiB is bounded
                     # but the FSM dance dominates.

trap 'delete_rd "$RD"' EXIT

echo ">> apply 2-replica RD on $N1 + $N2 (no tiebreaker)"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    DrbdOptions/AutoAddQuorumTiebreaker: "false"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N1}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N1}
  props: {StorPoolName: stand}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N2}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N2}
  props: {StorPoolName: stand}
EOF

wait_uptodate "$RD" "$N1" "$N2"

# Promote $N1 so the test has a clear winner side and we can
# regression-guard on Primary-ship through the recovery window.
echo ">> promote $N1 to Primary"
on_node "$N1" drbdadm primary "$RD"

n1_role_before=$(status_role "$RD" "$N1")
echo "   $N1 role pre-test: $n1_role_before"
if [[ "$n1_role_before" != "Primary" ]]; then
    echo "FAIL: $N1 is not Primary before the split-brain provocation"
    exit 1
fi

# Snapshot satellite pod names so log-tail / .res inspection are
# stable against a DaemonSet roll mid-test.
SAT_POD_N1=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o "jsonpath={.items[?(@.spec.nodeName==\"${N1}\")].metadata.name}")
SAT_POD_N2=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o "jsonpath={.items[?(@.spec.nodeName==\"${N2}\")].metadata.name}")

# Resolve each side's view of the peer node-id. `drbdsetup disconnect`
# takes the peer's id (not name); the .res fallback mirrors what
# recovery-discard-my-data.sh does.
resolve_peer_id() {
    local local_node=$1 peer_node=$2 id
    id=$(on_node "$local_node" drbdsetup status "$RD" --verbose 2>/dev/null \
        | grep -oE "node-id:[0-9]+" | head -2 | tail -1 | cut -d: -f2 || true)
    if [[ -z "$id" ]]; then
        id=$(on_node "$local_node" bash -c "
            awk '/^on / { host=\$2 } /node-id/ { print host, \$2 }' /etc/drbd.d/${RD}.res
        " | awk -v h="$peer_node" '$1 == h { print $2 }' | tr -d ';' | head -1)
    fi
    echo "$id"
}

PEER_ID_FROM_N1=$(resolve_peer_id "$N1" "$N2")
PEER_ID_FROM_N2=$(resolve_peer_id "$N2" "$N1")
echo "   $N1's view: peer ($N2) node-id = ${PEER_ID_FROM_N1:-<unknown>}"
echo "   $N2's view: peer ($N1) node-id = ${PEER_ID_FROM_N2:-<unknown>}"
if [[ -z "$PEER_ID_FROM_N1" || -z "$PEER_ID_FROM_N2" ]]; then
    echo "FAIL: could not resolve peer node-ids on both sides"
    exit 1
fi

# Snapshot .res content + mtime BEFORE the provocation. We assert
# content stability across the whole recipe; mtime is informational.
res_n1_before=$(on_node "$N1" sha256sum /etc/drbd.d/${RD}.res | awk '{print $1}')
res_n2_before=$(on_node "$N2" sha256sum /etc/drbd.d/${RD}.res | awk '{print $1}')
res_n1_mtime_before=$(on_node "$N1" stat -c %Y /etc/drbd.d/${RD}.res)
res_n2_mtime_before=$(on_node "$N2" stat -c %Y /etc/drbd.d/${RD}.res)
echo "   .res sha256 on $N1: $res_n1_before (mtime=$res_n1_mtime_before)"
echo "   .res sha256 on $N2: $res_n2_before (mtime=$res_n2_mtime_before)"

# --- Provoke split-brain ----------------------------------------------------
#
# Force both halves into a non-Connected state. `drbdadm disconnect`
# alone races with blockstor's adjust loop; `drbdsetup disconnect
# --force=yes` is the established idiom for moving the FSM into
# StandAlone for testing recipe scripts.
echo ">> force-disconnect both sides → both StandAlone"
on_node "$N2" bash -c "drbdsetup disconnect ${RD} ${PEER_ID_FROM_N2} --force=yes 2>&1 || true"
on_node "$N1" bash -c "drbdsetup disconnect ${RD} ${PEER_ID_FROM_N1} --force=yes 2>&1 || true"

# Settle: wait up to 10 s for both sides to leave Connected. We accept
# any non-Connected sub-state (StandAlone / Unconnected / Connecting /
# Disconnecting / Outdated) — the W12 recipe is the same regardless.
deadline=$(( $(date +%s) + 10 ))
n1_state=""
n2_state=""
while (( $(date +%s) < deadline )); do
    n1_state=$(status_connection_state "$RD" "$N1" "$N2")
    n2_state=$(status_connection_state "$RD" "$N2" "$N1")
    if [[ "$n1_state" != "Connected" && "$n1_state" != "Established" \
       && "$n2_state" != "Connected" && "$n2_state" != "Established" ]]; then
        break
    fi
    sleep 1
done
echo "   $N1 post-provocation: ->$N2=$n1_state"
echo "   $N2 post-provocation: ->$N1=$n2_state"
if [[ "$n1_state" == "Connected" || "$n1_state" == "Established" \
   || "$n2_state" == "Connected" || "$n2_state" == "Established" ]]; then
    echo "FAIL: at least one side reconnected on its own — could not provoke split-brain"
    exit 1
fi

# Mark start of the recovery window for the log scan.
window_start=$(date +%s)

# --- Apply VICTIM recipe verbatim on $N2 -----------------------------------
#
# From scenarios/wave2-05-drbd-state-recovery.md §5.W12:
#   drbdadm disconnect <rd>
#   drbdadm secondary <rd>
#   drbdadm -- --discard-my-data connect <rd>
#
# Each command is run as a separate `on_node` invocation so the test
# fails fast on the *specific* command that broke the recipe — a
# single multi-line `bash -c` would swallow which one regressed.
echo ">> VICTIM recipe on $N2 (loser side)"
on_node "$N2" drbdadm disconnect "$RD" 2>&1 || true
on_node "$N2" drbdadm secondary "$RD" 2>&1 || true
on_node "$N2" drbdadm -- --discard-my-data connect "$RD"

# --- Apply SURVIVOR recipe verbatim on $N1 ---------------------------------
#
# From W12:
#   drbdadm disconnect <rd>
#   drbdadm connect <rd>
echo ">> SURVIVOR recipe on $N1 (winner side)"
on_node "$N1" drbdadm disconnect "$RD" 2>&1 || true
on_node "$N1" drbdadm connect "$RD"

# --- Recovery polling loop -------------------------------------------------
#
# Within $RECOVERY_WINDOW: both peers must end up Connected +
# UpToDate. Sample $N1's role each tick so we fail fast on
# Primary-loss.
echo ">> wait up to ${RECOVERY_WINDOW}s for both peers → Established + UpToDate"
deadline=$(( $(date +%s) + RECOVERY_WINDOW ))
recovery_ok=false
n1_lost_primary=false
while (( $(date +%s) <= deadline )); do
    n1_role=$(status_role "$RD" "$N1")
    if [[ "$n1_role" != "Primary" ]]; then
        n1_lost_primary=true
        echo "   !! $N1 lost Primary role: $n1_role"
        break
    fi

    n1_disk=$(status_disk_state "$RD" "$N1")
    n2_disk=$(status_disk_state "$RD" "$N2")
    n1_conn=$(status_connection_state "$RD" "$N1" "$N2")
    n2_conn=$(status_connection_state "$RD" "$N2" "$N1")

    if [[ "$n1_disk" == "UpToDate" && "$n2_disk" == "UpToDate" \
       && ( "$n1_conn" == "Established" || "$n1_conn" == "Connected" ) \
       && ( "$n2_conn" == "Established" || "$n2_conn" == "Connected" ) ]]; then
        recovery_ok=true
        break
    fi
    sleep 2
done
window_elapsed=$(( $(date +%s) - window_start ))
echo "   recovery window elapsed: ${window_elapsed}s"

if [[ "$n1_lost_primary" == "true" ]]; then
    echo "FAIL: $N1 lost Primary during recovery — data path interrupted"
    exit 1
fi

if [[ "$recovery_ok" != "true" ]]; then
    echo "FAIL: did not converge to Established+UpToDate within ${RECOVERY_WINDOW}s"
    echo "  raw drbdsetup status on $N1:"
    on_node "$N1" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/    /' || true
    echo "  raw drbdsetup status on $N2:"
    on_node "$N2" drbdsetup status "$RD" --verbose 2>&1 | sed 's/^/    /' || true
    exit 1
fi

# --- Reconciler-survival assertion: .res unchanged -------------------------
#
# The critical W12 contract: blockstor's reconciler must NOT have
# re-rendered `.res` during the recipe. A content diff between the
# pre-provocation snapshot and the post-recovery state means the
# satellite tried to "fix" the configuration mid-recipe and could
# have flipped the side selection out from under the operator.
res_n1_after=$(on_node "$N1" sha256sum /etc/drbd.d/${RD}.res | awk '{print $1}')
res_n2_after=$(on_node "$N2" sha256sum /etc/drbd.d/${RD}.res | awk '{print $1}')
res_n1_mtime_after=$(on_node "$N1" stat -c %Y /etc/drbd.d/${RD}.res)
res_n2_mtime_after=$(on_node "$N2" stat -c %Y /etc/drbd.d/${RD}.res)
echo "   .res sha256 on $N1: $res_n1_after (mtime=$res_n1_mtime_after)"
echo "   .res sha256 on $N2: $res_n2_after (mtime=$res_n2_mtime_after)"

if [[ "$res_n1_before" != "$res_n1_after" ]]; then
    echo "FAIL: .res on $N1 was rewritten during the recipe window"
    echo "  before: $res_n1_before"
    echo "  after:  $res_n1_after"
    on_node "$N1" cat /etc/drbd.d/${RD}.res 2>&1 | sed 's/^/    /' || true
    exit 1
fi
if [[ "$res_n2_before" != "$res_n2_after" ]]; then
    echo "FAIL: .res on $N2 was rewritten during the recipe window"
    echo "  before: $res_n2_before"
    echo "  after:  $res_n2_after"
    on_node "$N2" cat /etc/drbd.d/${RD}.res 2>&1 | sed 's/^/    /' || true
    exit 1
fi

# Surface a satellite-log signal of the reconciler firing during the
# window. The convergence + .res hash assertions above are the
# binding contract; an Apply log hit here is informational so the
# reviewer of a CI run can spot a near-miss before it becomes a
# regression in a tighter timing budget.
echo ">> scan satellite logs for adjust-during-recovery on both sides"
adjust_hits_n1=$(kubectl -n "$NS" logs "$SAT_POD_N1" --since="${window_elapsed}s" 2>/dev/null \
    | grep -E "${RD}" | grep -ciE "adjust|Apply per-resource failure" || true)
adjust_hits_n2=$(kubectl -n "$NS" logs "$SAT_POD_N2" --since="${window_elapsed}s" 2>/dev/null \
    | grep -E "${RD}" | grep -ciE "adjust|Apply per-resource failure" || true)
echo "   adjust/Apply-failure log hits: $N1=$adjust_hits_n1, $N2=$adjust_hits_n2"

# Drain any in-flight resync before EXIT trap so delete_rd's
# drbdsetup-down doesn't race a SyncTarget step and hang.
wait_uptodate "$RD" "$N1" "$N2"

echo ">> SPLIT-BRAIN-RECOVERY OK (window=${window_elapsed}s, .res stable on both sides, $N1 stayed Primary)"
