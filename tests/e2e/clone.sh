#!/usr/bin/env bash
#
# usage: clone.sh WORK_DIR
#
# Tests CSI clone (volume-from-volume, no intermediate VolumeSnapshot).
# Same plumbing as snapshot-restore but driven through the CSI shape:
# the CSI controller-publish-volume path takes a VolumeContentSource of
# kind Volume → blockstor's snapshot-restore-resource is called by the
# CSI shim with an auto-generated transient snapshot name.
#
# Setup:
#   - source RD with known data on $N1 + $N2
#   - call /v1/resource-definitions/{rd}/clone with a target name
# Expected:
#   - target RD exists
#   - target's data == source's data (md5 match)

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD_SRC=e2e-clone-src
RD_DST=e2e-clone-dst
N1=test-worker-1
N2=test-worker-2

trap 'delete_rd "$RD_SRC"; delete_rd "$RD_DST"' EXIT

echo ">> apply source RD"
rd_apply "$RD_SRC" "$N1" "$N2"
wait_uptodate "$RD_SRC" "$N1" "$N2"

DEV=$(device_for_rd "$RD_SRC" "$N1")
RD=$RD_SRC
md5_src=$(write_random "$N1" "$DEV" 262144)

# blockstor's clone is implemented as snapshot-restore under the hood.
# A direct CSI clone path (CreateVolume with VolumeContentSource of kind
# Volume) compiles to: take a transient snapshot of the source, restore
# into the target, drop the snapshot. We reproduce that here.
SNAP=clone-$(date +%s)

echo ">> internal: take transient snapshot $SNAP for clone"
kubectl -n "$NS" exec deploy/blockstor-controller -- \
    curl -fsS -XPOST -H'Content-Type: application/json' \
    "http://localhost:3370/v1/resource-definitions/${RD_SRC}/snapshots" \
    -d "{\"name\":\"${SNAP}\",\"nodes\":[\"${N1}\",\"${N2}\"]}"

echo ">> clone $RD_SRC → $RD_DST"
kubectl -n "$NS" exec deploy/blockstor-controller -- \
    curl -fsS -XPOST -H'Content-Type: application/json' \
    "http://localhost:3370/v1/resource-definitions/${RD_SRC}/snapshot-restore-resource" \
    -d "{\"to_resource\":\"${RD_DST}\",\"snapshot_name\":\"${SNAP}\"}"

kubectl -n "$NS" exec deploy/blockstor-controller -- \
    curl -fsS -XPOST -H'Content-Type: application/json' \
    "http://localhost:3370/v1/resource-definitions/${RD_DST}/autoplace" \
    -d "{\"select_filter\":{\"place_count\":2,\"storage_pool\":\"stand\"}}"

RD=$RD_DST
wait_uptodate "$RD_DST" "$N1" "$N2"

DEV_DST=$(device_for_rd "$RD_DST" "$N1")
md5_dst=$(read_md5 "$N1" "$DEV_DST" 262144)

if [[ "$md5_src" != "$md5_dst" ]]; then
    echo "FAIL: clone data differs (src=$md5_src dst=$md5_dst)"
    exit 1
fi

echo ">> CLONE OK"
