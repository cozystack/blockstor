#!/usr/bin/env bash
#
# usage: snap-single-replica-zfs-restore-u464.sh WORK_DIR
#
# L6 cli-matrix cell — campaign-2 U464 (BS-side restore variant).
#
# Upstream U464 is single-node ZFS in-place rollback. blockstor
# DELIBERATELY rejects in-place rollback (501, known-delta row #73 —
# `zfs rollback` destroys every newer snapshot). The supported
# single-node path is RESTORE: snapshot a single-replica resource and
# materialise it into a NEW resource-definition cloned (`zfs clone`)
# from the snapshot, on the same node, on a zfs-thin pool.
#
# This cell pins that path end-to-end on the zfs-thin pool:
#
#   Setup: single diskful RD on worker-1, zfs-thin pool, UpToDate.
#   1. snapshot create RD SNAP — succeeds (single-replica suspend/take/
#      resume barrier completes; IO is not left frozen).
#   2. snapshot resource restore --to-resource TGT worker-1 — succeeds.
#      The target RD is created and its single replica converges UpToDate
#      (the `zfs clone` of the snapshot dataset materialises real data).
#   3. The in-place `snapshot rollback` of the SAME snapshot still 501s
#      (the deliberate-delta guard is intact alongside the working
#      restore path).
#
#   Cleanup: delete both RDs + snapshot + assert_no_orphans.
#
# Catches regressions in:
#   - the single-replica suspend-io barrier (a stuck Phase-1/2 would
#     hang the snapshot create and leave the lone replica frozen)
#   - the ZFS clone/restore materialisation on zfs-thin (U290-adjacent)
#   - the in-place-rollback 501 deliberate delta not silently regressing
#     into a destructive `zfs rollback`

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

linstor_cli_setup

SP=zfs-thin
SRC=ccu3-u464-src
TGT=ccu3-u464-tgt
SNAP=snap-u464
SIZE_MIB=64

N1=$WORKER_1

cleanup() {
    "${LCTL[@]}" snapshot delete "$SRC" "$SNAP" 2>/dev/null || true
    delete_rd "$TGT"
    delete_rd "$SRC"
    assert_no_orphans "$SRC"
    assert_no_orphans "$TGT"
    linstor_cli_teardown
}
trap cleanup EXIT

# =====================================================================
# Setup: single diskful RD on $N1, zfs-thin pool
# =====================================================================
echo ">> Setup: single-replica diskful RD $SRC on $N1 (pool=$SP, size=${SIZE_MIB} MiB)"
_out=$("${LCTL[@]}" resource-definition create "$SRC" 2>&1) \
    || { echo "FAIL: rd c $SRC: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" volume-definition create "$SRC" "${SIZE_MIB}M" 2>&1) \
    || { echo "FAIL: vd c $SRC ${SIZE_MIB}M: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N1" "$SRC" --storage-pool="$SP" 2>&1) \
    || { echo "FAIL: r c $N1 $SRC: $_out" >&2; exit 1; }

# Single replica: wait_uptodate needs a peer ($3), which is unbound here
# (one-node RD) → `$3: unbound variable` under `set -u`. Poll the lone
# node's observer-stamped disk state directly; it reaches UpToDate from
# its own day0 GI seed with no peer to sync from.
wait_disk_state "$SRC" "$N1" "UpToDate" 180 0

# =====================================================================
# Step 1: snapshot create (single-replica suspend/take/resume barrier)
# =====================================================================
echo ">> Step 1: snapshot create $SRC $SNAP"
_out=$("${LCTL[@]}" snapshot create "$SRC" "$SNAP" 2>&1) \
    || { echo "FAIL (U464): snapshot create returned non-zero — single-replica barrier may have hung: $_out" >&2; exit 1; }

echo ">>   wait up to 60s for the snapshot to reach Ready on $N1 (resume completed)"
deadline=$(( $(date +%s) + 60 ))
ok=false
while (( $(date +%s) < deadline )); do
    j=$(kubectl get snapshots.blockstor.cozystack.io -o json 2>/dev/null \
        | jq -c --arg rd "$SRC" --arg s "$SNAP" '
            [.items[]?
             | select(.spec.resourceDefinitionName==$rd)
             | select(.spec.snapshotName==$s)] | first // {}')
    failed=$(jq -r '((.status.flags // []) | index("FAILED")) != null' <<<"$j" 2>/dev/null)
    if [[ "$failed" == "true" ]]; then
        echo "FAIL (U464): single-replica snapshot stamped FAILED: $j" >&2
        exit 1
    fi
    ready=$(jq -r '
        ((.spec.nodes // []) as $want
         | ([.status.nodeStatus[]? | select(.ready==true) | .nodeName]) as $have
         | ($want | length) > 0 and (($want - $have) | length == 0))' <<<"$j" 2>/dev/null)
    if [[ "$ready" == "true" ]]; then ok=true; break; fi
    sleep 2
done
if ! $ok; then
    echo "FAIL (U464): single-replica snapshot never reached Ready within 60s" >&2
    exit 1
fi

# =====================================================================
# Step 2: restore into a NEW RD on the SAME single node (zfs clone)
# =====================================================================
echo ">> Step 2: snapshot resource restore --to-resource $TGT $N1"
if ! _out=$("${LCTL[@]}" snapshot resource restore \
        --from-resource "$SRC" \
        --from-snapshot "$SNAP" \
        --to-resource "$TGT" \
        "$N1" 2>&1); then
    echo "FAIL (U464): single-replica restore returned non-zero: $_out" >&2
    exit 1
fi

echo ">>   assert target RD $TGT exists"
if ! "${LCTL[@]}" resource-definition list --resource-definitions "$TGT" 2>/dev/null | grep -q "$TGT"; then
    echo "FAIL (U464): restored RD $TGT not visible after restore" >&2
    exit 1
fi

echo ">>   wait up to 180s for the single restored replica to reach UpToDate on $N1"
# Single restored replica — same 2-peer-helper trap as the source RD
# above: poll the lone node's observer disk state directly.
wait_disk_state "$TGT" "$N1" "UpToDate" 180 0

# =====================================================================
# Step 3: in-place rollback of the SAME snapshot still 501s (delta #73)
# =====================================================================
echo ">> Step 3: snapshot rollback $SRC $SNAP MUST still be refused (deliberate delta #73)"
rb_out=$("${LCTL[@]}" snapshot rollback "$SRC" "$SNAP" 2>&1 || true)
echo "$rb_out" | head -5
if ! grep -qiE 'not implemented|restore-resource|501' <<<"$rb_out"; then
    echo "FAIL (U464): in-place rollback did NOT surface the 501 deliberate-delta refusal" >&2
    echo "  Expected an actionable 'not implemented; use snapshot-restore-resource' message." >&2
    echo "$rb_out" >&2
    exit 1
fi

echo ">> PASS: snap-single-replica-zfs-restore-u464 (single-replica zfs-thin RESTORE works; in-place rollback stays 501)"
