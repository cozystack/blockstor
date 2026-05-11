#!/usr/bin/env bash
#
# usage: resize-no-drbd.sh WORK_DIR
#
# Resize an RD whose LayerStack is just ["STORAGE"] — no DRBD, no
# LUKS. Validates the storage-only resize path: PUT a larger
# size_kib through REST, satellite picks the spec delta up and
# runs the provider's ResizeVolume (truncate + losetup -c for
# FILE/FILE_THIN, lvextend for LVM-thin, zfs set volsize for ZFS),
# and the consumer-facing /dev/loopN device reflects the new size
# without losing the data that was already on it.
#
# Single-replica because the no-DRBD stack doesn't replicate; we
# only care that the device grows + the data survives.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

RD=e2e-resize-no-drbd
N1=$WORKER_1
SIZE_INITIAL_KIB=65536
SIZE_GROWN_KIB=131072
TOLERANCE_KIB=128
STORPOOL=${STORPOOL:-stand}

cleanup() {
    kubectl delete resource --all --force --grace-period=0 --ignore-not-found 2>/dev/null || true
    kubectl delete resourcedefinition --all --ignore-not-found 2>/dev/null || true
}
trap cleanup EXIT

echo ">> apply ${RD} with LayerStack=[STORAGE], initial ${SIZE_INITIAL_KIB} KiB"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  layerStack: ["STORAGE"]
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: ${SIZE_INITIAL_KIB}}
EOF

cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N1}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N1}
  props: {StorPoolName: ${STORPOOL}}
EOF

echo ">> wait up to 60s for satellite to provision the device"
deadline=$(( $(date +%s) + 60 ))
DEV=""
while (( $(date +%s) < deadline )); do
    DEV=$(kubectl get resource "${RD}.${N1}" \
        -o jsonpath='{.status.volumes[?(@.volumeNumber==0)].devicePath}' \
        2>/dev/null || true)
    if [[ -n "$DEV" ]]; then
        break
    fi
    sleep 2
done

if [[ -z "$DEV" ]]; then
    echo "FAIL: no devicePath observed after 60s"
    exit 1
fi

if [[ "$DEV" == /dev/drbd* ]]; then
    echo "FAIL: LayerStack=[STORAGE] but device is DRBD: $DEV"
    exit 1
fi

echo ">> initial device path: $DEV"
echo ">> initial device size:"
on_node "$N1" bash -c "blockdev --getsize64 $DEV"

echo ">> write 4 MiB pattern to the device, capture md5 of the first 1 MiB"
on_node "$N1" bash -c "dd if=/dev/urandom of=$DEV bs=1M count=4 conv=fsync status=none"
md5_pre=$(on_node "$N1" bash -c "dd if=$DEV bs=1M count=1 status=none | md5sum | awk '{print \$1}'")
echo "   md5_pre=$md5_pre"

echo ">> PUT size_kib=${SIZE_GROWN_KIB} via REST"
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n blockstor-system port-forward svc/blockstor-controller "$PF_PORT":3370 \
    >/tmp/resize-no-drbd-pf.log 2>&1 &
PF_PID=$!
trap 'kill $PF_PID 2>/dev/null || true; cleanup' EXIT

for _ in $(seq 1 10); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

curl -sf -X PUT \
    -H 'Content-Type: application/json' \
    -d "{\"size_kib\":${SIZE_GROWN_KIB}}" \
    "http://localhost:$PF_PORT/v1/resource-definitions/${RD}/volume-definitions/0" \
    >/dev/null

echo ">> wait up to 60s for the device to grow"
deadline=$(( $(date +%s) + 60 ))
cur=0
while (( $(date +%s) < deadline )); do
    cur=$(on_node "$N1" bash -c "blockdev --getsize64 $DEV" 2>/dev/null || echo 0)
    cur=$(( cur / 1024 ))
    if (( cur + TOLERANCE_KIB >= SIZE_GROWN_KIB )); then
        break
    fi
    sleep 2
done

if (( cur + TOLERANCE_KIB < SIZE_GROWN_KIB )); then
    echo "FAIL: device size $cur KiB < $SIZE_GROWN_KIB (tolerance ${TOLERANCE_KIB})"
    exit 1
fi

echo ">> grew to $cur KiB; verify the first 1 MiB data survived"
md5_post=$(on_node "$N1" bash -c "dd if=$DEV bs=1M count=1 status=none | md5sum | awk '{print \$1}'")
echo "   md5_post=$md5_post"

if [[ "$md5_pre" != "$md5_post" ]]; then
    echo "FAIL: md5 changed after resize (pre=$md5_pre, post=$md5_post)"
    exit 1
fi

echo ">> RESIZE-NO-DRBD OK ($SIZE_INITIAL_KIB → $cur KiB, data preserved)"
