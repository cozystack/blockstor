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

echo ">> autoplace $RD_DST on $N2 + $N3 (cross-node)"
rest_post "/v1/resource-definitions/${RD_DST}/autoplace" \
    "{\"select_filter\":{\"place_count\":2,\"storage_pool\":\"${STORPOOL}\",\"node_name_list\":[\"${N2}\",\"${N3}\"]}}"

echo ">> wait for $RD_DST UpToDate on $N2 + $N3"
RD=$RD_DST
wait_uptodate "$RD_DST" "$N2" "$N3"

DEV_DST=$(device_for_rd "$RD_DST" "$N3")
md5_dst=$(read_md5 "$N3" "$DEV_DST" 262144)

if [[ "$md5_src" != "$md5_dst" ]]; then
    echo "FAIL: restored data differs (src=$md5_src dst=$md5_dst)"
    exit 1
fi

echo ">> SNAPSHOT-RESTORE-CROSS-NODE OK ($RD_DST on $N3 == $RD_SRC on $N1)"
