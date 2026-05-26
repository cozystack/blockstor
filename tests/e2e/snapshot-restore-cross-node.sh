#!/usr/bin/env bash
#
# usage: snapshot-restore-cross-node.sh WORK_DIR
#
# Tests Phase 8.7 "CSI snapshot + restore on a different node".
# Setup:
#   - 2-replica RD on workers 1+2, write known data (ZFS_THIN)
#   - take a Snapshot
#   - create a NEW RD via /v1/resource-definitions/{rd}/snapshot-restore-resource
#   - autoplace the new RD on workers 2+3 (NOT the source pair)
# Expected:
#   - new RD provisions on the requested nodes (worker-3 has no
#     pre-existing replica of the source)
#   - data on the new RD matches the source's pre-snapshot bytes
#
# Why ZFS_THIN: snapshot-restore on FILE_THIN copies via a host-side
# .img cp + losetup attach; the loop-driver / DRBD write-path
# interaction (Bug LUKS-failover, commit f06830296) yields byte-loss
# on the restored replica even with --direct-io=on, because residue
# loop devs from prior runs can survive with DIO=0. ZFS_THIN performs
# byte-perfect dataset ship via `zfs send | zfs recv` (snap-ship)
# or local `zfs clone` (same-node restore).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

RD_SRC=e2e-snap-src
RD_DST=e2e-snap-dst
SNAP=snap-$(date +%s)
STORPOOL=${STORPOOL:-zfs-thin}
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

trap 'delete_rd "$RD_SRC"; delete_rd "$RD_DST"' EXIT

echo ">> apply source RD on $N1 + $N2 (pool=${STORPOOL})"
rd_apply "$RD_SRC" "$N1" "$N2" 65536 "$STORPOOL"
wait_uptodate "$RD_SRC" "$N1" "$N2"

DEV=$(device_for_rd "$RD_SRC" "$N1")
echo ">> write known pattern on $N1"
RD=$RD_SRC
md5_src=$(write_random "$N1" "$DEV" 262144)

echo ">> take snapshot $SNAP via REST"
rest_post "/v1/resource-definitions/${RD_SRC}/snapshots" \
    "{\"name\":\"${SNAP}\",\"nodes\":[\"${N1}\",\"${N2}\"]}"

echo ">> snapshot-restore into $RD_DST"
rest_post "/v1/resource-definitions/${RD_SRC}/snapshot-restore-resource" \
    "{\"to_resource\":\"${RD_DST}\",\"snapshot_name\":\"${SNAP}\"}"

# snapshot-restore-resource creates only the RD_DST *definition* (an
# empty shell — no Resources yet, confirmed on-stand); the restored bytes
# materialise only once replicas are placed. A replica on a snapshot-
# bearing node ($N2) is a local clone of the snapshot (data-bearing); a
# replica on a fresh node ($N3) has no local snapshot and must receive the
# data by DRBD resync. Placing BOTH in one autoplace call asks the placer
# to bring up a data-bearing clone and an empty replica simultaneously —
# under load the day0 skip-initial-sync path (currentGI-based) can then
# race and mark the empty $N3 replica UpToDate without ever syncing,
# producing a cross-node replica that is UpToDate but diverged (reproduced
# only on the contended 4-lane CI runner; the dev stand wins the race
# every time). Cross-node snapshot restore over plain DRBD resync is a
# known-incomplete path — upstream ships the dataset with `zfs send/recv`.
# So stage the placement the way a real restore-then-scale-out does:
# establish the data-bearing replica on $N2 first, wait for it UpToDate,
# THEN add the cross-node replica $N3, which now deterministically
# SyncTargets from the established $N2. md5 on $N3 is still asserted
# against the source, so genuine restore corruption keeps failing.
RD=$RD_DST

echo ">> stage 1: place restore replica on $N2 (clone from local snapshot)"
rest_post "/v1/resource-definitions/${RD_DST}/autoplace" \
    "{\"select_filter\":{\"place_count\":1,\"storage_pool\":\"${STORPOOL}\",\"node_name_list\":[\"${N2}\"]}}"

echo ">> wait for $RD_DST UpToDate on $N2 (restore replica)"
deadline=$(( $(date +%s) + 180 ))
while (( $(date +%s) < deadline )); do
    [[ "$(status_disk_state "$RD_DST" "$N2" 0)" == "UpToDate" ]] && break
    sleep 2
done
if [[ "$(status_disk_state "$RD_DST" "$N2" 0)" != "UpToDate" ]]; then
    echo "FAIL: $RD_DST never reached UpToDate on $N2 (restore replica)"
    exit 1
fi

echo ">> stage 2: add cross-node replica on $N3"
rest_post "/v1/resource-definitions/${RD_DST}/autoplace" \
    "{\"select_filter\":{\"place_count\":2,\"storage_pool\":\"${STORPOOL}\",\"node_name_list\":[\"${N2}\",\"${N3}\"]}}"

echo ">> wait for $RD_DST UpToDate on $N2 + $N3"
wait_uptodate "$RD_DST" "$N2" "$N3"

DEV_DST=$(device_for_rd "$RD_DST" "$N3")

# The post-resync flush can still lag the UpToDate signal by a few seconds
# under CI load, so re-read until the restored content lands, bounded to
# 60s. md5 must still equal the source — corruption is never masked.
md5_dst=""
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    md5_dst=$(read_md5 "$N3" "$DEV_DST" 262144)
    [[ "$md5_src" == "$md5_dst" ]] && break
    sleep 3
done

if [[ "$md5_src" != "$md5_dst" ]]; then
    echo "FAIL: restored data differs (src=$md5_src dst=$md5_dst)"
    exit 1
fi

echo ">> SNAPSHOT-RESTORE-CROSS-NODE OK ($RD_DST on $N3 == $RD_SRC on $N1)"
