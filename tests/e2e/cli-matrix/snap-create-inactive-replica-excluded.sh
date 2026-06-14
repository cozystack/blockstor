#!/usr/bin/env bash
#
# usage: snap-create-inactive-replica-excluded.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 394.
#
# Snapshot create on an RD that has an INACTIVE diskful replica must
# SUCCEED across the ACTIVE diskful set and must NOT target the
# INACTIVE node.
#
# Root cause (pre-fix): pkg/rest/snapshots.go listDiskfulNodes skipped
# DISKLESS (and thus TIE_BREAKER) replicas but NOT INACTIVE ones, so
# the defaulted snap.Nodes included a node whose DRBD resource is
# `drbdadm down` (INACTIVE). The snapshot suspend-io barrier then waits
# for that node to ack a `drbdsetup suspend-io`, which the satellite
# cannot do on a down resource → that node reports Failed → the whole
# snapshot group aborts (resume-on-abort). Net effect: a `snapshot
# create` on ANY RD that has an INACTIVE replica reliably FAILED.
#
# This is the same INACTIVE-miscount class as Bugs 387/390/393.
#
# Expected (post-fix): the snapshot targets only the ACTIVE diskful
# replicas. The INACTIVE replica holds data but its DRBD device is
# down and cannot participate in the suspend-io barrier; it catches up
# on reactivation. A snapshot of the active set is the correct
# semantic.
#
# Test contract:
#
#   Setup: 3-replica diskful RD on worker-1 + worker-2 + worker-3,
#          all UpToDate. (3 replicas so that deactivating one still
#          leaves 2 ACTIVE diskful peers for the snapshot to land on.)
#
#   1. linstor r deactivate N3 RD
#      - INACTIVE flag visible in Spec.Flags within 30s on N3.
#
#   2. linstor snapshot create RD SNAP   (default fan-out form — no
#      explicit node list, so the controller derives the target set
#      from the diskful replicas; this is the path Bug 394 broke).
#      - Returns zero.
#      - Snapshot CRD spec.nodes == {N1, N2} exactly. N3 (INACTIVE)
#        MUST NOT appear — otherwise the suspend-io barrier aborts.
#      - Every targeted node reaches Ready=true within 60s; the
#        snapshot MUST NOT stamp FAILED.
#
#   3. linstor snapshot list shows SNAP.
#
#   Cleanup: snapshot delete + reactivate N3 + delete_rd +
#            assert_no_orphans.
#
# Catches regressions in:
#   - listDiskfulNodes INACTIVE exclusion (target node-set leak)
#   - the suspend-io barrier aborting the whole group because a
#     down DRBD device was asked to ack suspend-io

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-snap-inactive
SNAP=snap-bug394
SIZE_MIB=64

N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

cleanup() {
    "${LCTL[@]}" snapshot delete "$RD" "$SNAP" 2>/dev/null || true
    # Reactivate N3 so delete_rd drives a clean teardown.
    "${LCTL[@]}" resource activate "$N3" "$RD" 2>/dev/null || true
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

# =====================================================================
# Setup: 3-replica diskful RD on $N1 + $N2 + $N3, all UpToDate
# =====================================================================
echo ">> Setup: 3-replica diskful RD on $N1 + $N2 + $N3 (size=${SIZE_MIB} MiB)"
_out=$("${LCTL[@]}" resource-definition create "$RD" 2>&1) \
    || { echo "FAIL: rd c $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" volume-definition create "$RD" "${SIZE_MIB}M" 2>&1) \
    || { echo "FAIL: vd c $RD ${SIZE_MIB}M: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N1" "$RD" --storage-pool=stand 2>&1) \
    || { echo "FAIL: r c $N1 $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N2" "$RD" --storage-pool=stand 2>&1) \
    || { echo "FAIL: r c $N2 $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N3" "$RD" --storage-pool=stand 2>&1) \
    || { echo "FAIL: r c $N3 $RD: $_out" >&2; exit 1; }

# wait_uptodate is a 2-peer helper: (rd, primary, peer, [vol]). Passing a
# third NODE here landed $N3 in the `vol` slot, which the kernel
# ground-truth path feeds to jq as `--argjson v "$N3"` → jq aborts on a
# non-numeric ("big-worker-3 is not valid JSON"), failing the setup. For a
# 3-replica RD, anchor on $N1 and wait it UpToDate against each peer in
# turn — one query on $N1 covers all three replicas.
wait_uptodate "$RD" "$N1" "$N2"
wait_uptodate "$RD" "$N1" "$N3"

# =====================================================================
# Step 1: deactivate $N3 → INACTIVE
# =====================================================================
echo ">> Step 1: linstor r deactivate $N3 $RD"
_out=$("${LCTL[@]}" resource deactivate "$N3" "$RD" 2>&1) \
    || { echo "FAIL: r deactivate $N3 $RD: $_out" >&2; exit 1; }

echo ">>   wait up to 30s for INACTIVE flag on $N3"
deadline=$(( $(date +%s) + 30 ))
inactive_seen=false
while (( $(date +%s) < deadline )); do
    flags=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${N3}" \
        -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
    if [[ "$flags" == *"INACTIVE"* ]]; then
        inactive_seen=true
        break
    fi
    sleep 2
done
if ! $inactive_seen; then
    echo "FAIL: $N3 never got INACTIVE flag within 30s" >&2
    kubectl get "resources.blockstor.cozystack.io/${RD}.${N3}" -o yaml 2>&1 | tail -30 >&2
    exit 1
fi

# Settle window: let the .res rewrite + drbdadm adjust land before the
# snapshot create races the satellite's reconcile.
sleep 5

# =====================================================================
# Step 2: snapshot create — default fan-out (the Bug 394 path)
# =====================================================================
echo ">> Step 2: linstor snapshot create $RD $SNAP (default fan-out — no node list)"
# Upstream grammar with no positional node before the RD lets the
# controller derive the target set. Pre-fix, listDiskfulNodes leaked
# the INACTIVE $N3 into spec.nodes and the suspend-io barrier aborted
# the whole group. The create itself returning non-zero IS the bug.
_out=$("${LCTL[@]}" snapshot create "$RD" "$SNAP" 2>&1) \
    || { echo "FAIL (Bug 394): snapshot create returned non-zero — group likely aborted on INACTIVE peer: $_out" >&2; exit 1; }

snap_json() {
    kubectl get snapshots.blockstor.cozystack.io -o json 2>/dev/null \
        | jq -c --arg rd "$RD" --arg s "$SNAP" '
            [.items[]?
             | select(.spec.resourceDefinitionName==$rd)
             | select(.spec.snapshotName==$s)] | first // {}'
}

# spec.nodes must be exactly {N1, N2}; N3 (INACTIVE) must NOT appear.
echo ">>   assert spec.nodes == {$N1,$N2} and excludes INACTIVE $N3"
j=$(snap_json)
n3_in_spec=$(jq -r --arg n "$N3" '((.spec.nodes // []) | index($n)) != null' <<<"$j" 2>/dev/null)
if [[ "$n3_in_spec" == "true" ]]; then
    echo "FAIL (Bug 394): $SNAP spec.nodes contains INACTIVE peer $N3 — suspend-io barrier will abort the group" >&2
    echo "$j" >&2
    exit 1
fi
n1_in_spec=$(jq -r --arg n "$N1" '((.spec.nodes // []) | index($n)) != null' <<<"$j" 2>/dev/null)
n2_in_spec=$(jq -r --arg n "$N2" '((.spec.nodes // []) | index($n)) != null' <<<"$j" 2>/dev/null)
if [[ "$n1_in_spec" != "true" || "$n2_in_spec" != "true" ]]; then
    echo "FAIL (Bug 394): $SNAP spec.nodes missing an active diskful peer (want both $N1 and $N2)" >&2
    echo "$j" >&2
    exit 1
fi

# Every targeted node must reach Ready=true; the snapshot must NOT
# stamp FAILED (the abort symptom).
echo ">>   wait up to 60s for Ready=true on every targeted node (must NOT abort/FAIL)"
deadline=$(( $(date +%s) + 60 ))
ready_ok=false
last_json=""
while (( $(date +%s) < deadline )); do
    j=$(snap_json)
    last_json="$j"
    failed=$(jq -r '((.status.flags // []) | index("FAILED")) != null' <<<"$j" 2>/dev/null)
    if [[ "$failed" == "true" ]]; then
        echo "FAIL (Bug 394): Snapshot $SNAP stamped FAILED — group aborted (resume-on-abort)" >&2
        echo "$j" >&2
        exit 1
    fi
    ready_complete=$(jq -r '
        ((.spec.nodes // []) as $want
         | ([.status.nodeStatus[]? | select(.ready==true) | .nodeName]) as $have
         | ($want | length) > 0 and (($want - $have) | length == 0))' <<<"$j" 2>/dev/null)
    if [[ "$ready_complete" == "true" ]]; then
        ready_ok=true
        break
    fi
    sleep 2
done
if ! $ready_ok; then
    echo "FAIL (Bug 394): not every active node reached Ready=true within 60s" >&2
    echo "$last_json" >&2
    exit 1
fi

# =====================================================================
# Step 3: snapshot list shows the snapshot
# =====================================================================
echo ">> Step 3: linstor snapshot list shows $SNAP"
sl_out=$("${LCTL[@]}" snapshot list 2>&1 || true)
echo "$sl_out" | head -20
if ! grep -q "$SNAP" <<<"$sl_out"; then
    echo "FAIL: linstor snapshot list does not show $SNAP" >&2
    echo "$sl_out" >&2
    exit 1
fi

# =====================================================================
# Cleanup handled by EXIT trap.
# =====================================================================
echo ">> PASS: snap-create-inactive-replica-excluded (Bug 394: INACTIVE peer excluded from snapshot target set; group did not abort)"
