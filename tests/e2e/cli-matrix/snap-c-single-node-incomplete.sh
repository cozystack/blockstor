#!/usr/bin/env bash
#
# usage: snap-c-single-node-incomplete.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 352.
#
# User-reported 2026-05-19: `linstor snapshot create <rd> <snap>
# <single-node>` (taking a snapshot of one specific replica
# instead of the cluster-wide default of "all diskful") hangs
# the resulting Snapshot in `Incomplete` state forever — never
# converges to Successful, never errors out.
#
# Upstream LINSTOR's per-resource snapshot path
# (CtrlSnapshotCrtApiCallHandler.takeSnapshot with a non-empty
# node-name list) snapshots only the listed replicas, then
# stamps the Snapshot Successful as soon as those replicas
# (not "all diskful") return SUCCESS. The diskful peers NOT in
# the node-list are left out of the Snapshot.Resources child
# list, so `successful = all(resources match Successful)` is
# vacuous-true for the empty subset.
#
# Bug 352 hypothesis (mirrors the Bug 104/108 quorum-of-children
# pattern): blockstor's RD-level Snapshot reconciler probably
# requires ALL diskful Resources of the parent RD to stamp their
# Snapshot-child as Successful, not just the explicitly-listed
# subset. With only one node in the request, the missing peer's
# child is never created → "successful all over children" stays
# false → CRD parked in Incomplete forever.
#
# Test contract:
#   1. 2-replica diskful RD on worker-1 + worker-2.
#   2. Wait both UpToDate.
#   3. `linstor snapshot create <rd> <snap> <worker-1>` (single-
#      node form — the CLI's positional node arg restricts which
#      replicas snap).
#   4. Within 60s, the Snapshot CRD must reach Successful and
#      the child Snapshot-resource on worker-1 must be in
#      Successful state too. Cell FAILs if the snapshot stays
#      Incomplete past the deadline (Bug 352 fingerprint).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup
trap linstor_cli_teardown EXIT

RD=cli-matrix-snap-single-node
SNAP=snap-only-n1

N1=$WORKER_1
N2=$WORKER_2

cleanup() {
    "${LCTL[@]}" snapshot delete "$RD" "$SNAP" 2>/dev/null || true
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> 2-replica diskful RD on $N1 + $N2"
_out=$("${LCTL[@]}" resource-definition create "$RD" 2>&1) \
    || { echo "FAIL: rd c $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" volume-definition create "$RD" 64M 2>&1) \
    || { echo "FAIL: vd c $RD 64M: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N1" "$RD" --storage-pool=stand 2>&1) \
    || { echo "FAIL: r c $N1: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N2" "$RD" --storage-pool=stand 2>&1) \
    || { echo "FAIL: r c $N2: $_out" >&2; exit 1; }
wait_uptodate "$RD" "$N1" "$N2"

# =====================================================================
# Trigger: snapshot create with explicit single-node restriction
# =====================================================================
echo ">> [Bug 352 trigger] linstor snapshot create $RD $SNAP $N1  (single-node form)"
_out=$("${LCTL[@]}" snapshot create "$RD" "$SNAP" "$N1" 2>&1) \
    || { echo "FAIL: snapshot create returned non-zero: $_out" >&2; exit 1; }

# =====================================================================
# Bug 352 assertion: snapshot must converge to Successful within 60s
# =====================================================================
echo ">> [Bug 352] wait up to 60s for Snapshot $SNAP to reach Successful"
deadline=$(( $(date +%s) + 60 ))
ok=false
last_state=""
while (( $(date +%s) < deadline )); do
    # The CRD shape: each `Snapshots.blockstor.io.blockstor.io`
    # row is per-(rd, snap, node). For a single-node request we
    # expect exactly ONE row keyed by ($RD, $SNAP, $N1) reaching
    # `Successful`. Read both Status.Successful and a state-like
    # field if present.
    rows=$(kubectl get snapshots.blockstor.io.blockstor.io -o json 2>/dev/null \
        | jq -c --arg rd "$RD" --arg s "$SNAP" '
            [.items[]?
             | select(.spec.resourceDefinitionName==$rd)
             | select(.spec.snapshotName==$s)
             | {node: .spec.nodeName, ok: (.status.successful // false), state: (.status.state // .status.phase // "")}]')
    last_state="$rows"

    # "Successful" if every row is ok=true AND at least one row exists
    n_rows=$(jq 'length' <<<"$rows" 2>/dev/null || echo 0)
    if (( n_rows > 0 )); then
        all_ok=$(jq 'all(.ok)' <<<"$rows" 2>/dev/null || echo "false")
        if [[ "$all_ok" == "true" ]]; then
            ok=true
            break
        fi
    fi
    sleep 2
done

if ! $ok; then
    echo "FAIL (Bug 352): Snapshot $SNAP on $RD (node=$N1) stayed Incomplete past 60s" >&2
    echo "----- snapshot row state -----" >&2
    echo "$last_state" >&2
    echo "----- linstor snapshot list -----" >&2
    "${LCTL[@]}" snapshot list 2>&1 | head -20 >&2
    echo "----- kubectl get snapshots -----" >&2
    kubectl get snapshots.blockstor.io.blockstor.io -o yaml 2>&1 | head -80 >&2
    exit 1
fi

# Extra sanity: the snapshot must exist ONLY on $N1, NOT on $N2.
# If blockstor created Snapshot children on both (ignoring the
# node-list filter), that's a different bug — surface it.
n2_rows=$(kubectl get snapshots.blockstor.io.blockstor.io -o json 2>/dev/null \
    | jq -r --arg rd "$RD" --arg s "$SNAP" --arg n "$N2" '
        [.items[]?
         | select(.spec.resourceDefinitionName==$rd)
         | select(.spec.snapshotName==$s)
         | select(.spec.nodeName==$n)] | length')
if [[ "${n2_rows:-0}" != "0" ]]; then
    echo "FAIL (Bug 352 sibling): snapshot CRD created on $N2 even though the request specified only $N1" >&2
    kubectl get snapshots.blockstor.io.blockstor.io -o yaml 2>&1 | head -80 >&2
    exit 1
fi

echo ">> snap-c-single-node-incomplete OK (Bug 352 pinned: single-node snapshot reaches Successful within 60s)"
