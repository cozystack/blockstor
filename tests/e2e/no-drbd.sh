#!/usr/bin/env bash
#
# usage: no-drbd.sh WORK_DIR
#
# Phase 9: layer stack ["STORAGE"] — single-replica local-storage mode,
# no DRBD layer. The satellite must NOT render a .res file or invoke
# drbdadm; the consumer Pod sees the raw provider device directly
# (e.g. /dev/<vg>/<rd>_00000 on LVM-thin, /dev/zd<N> on ZFS_THIN).
#
# Setup:
#   - 1-replica RD on worker-1 with LayerStack=["STORAGE"]
#   - write urandom + capture md5
# Steps:
#   1. apply RD with explicit layerStack
#   2. apply Resource on N1
#   3. wait for satellite to provision the LV (no DRBD)
#   4. write 4 MiB pattern, read it back — md5 must match
#   5. assert no .res file landed on the satellite
# Expected:
#   - storage device is the raw provider path, not /dev/drbd<N>
#   - data round-trips byte-perfect
#   - /etc/drbd.d/${RD}.res absent on the satellite

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

RD=e2e-no-drbd
N1=$WORKER_1
SIZE_KIB=131072    # 128 MiB
STORPOOL=${STORPOOL:-stand}

trap 'delete_rd "$RD"' EXIT

echo ">> apply ${RD} with LayerStack=[STORAGE]"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  layerStack: ["STORAGE"]
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: ${SIZE_KIB}}
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

echo ">> wait up to 60s for satellite to provision raw LV"
deadline=$(( $(date +%s) + 60 ))
DEV=""
while (( $(date +%s) < deadline )); do
    DEV=$(kubectl get resource "${RD}.${N1}" -o jsonpath='{.status.devicePath}' 2>/dev/null || true)
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

echo ">> write+read 4 MiB pattern"
md5_pre=$(write_random "$N1" "$DEV" 4194304)
md5_post=$(read_md5 "$N1" "$DEV" 4194304)

if [[ "$md5_pre" != "$md5_post" ]]; then
    echo "FAIL: data corrupted (pre=$md5_pre post=$md5_post)"
    exit 1
fi

echo ">> assert no .res file rendered"
if on_node "$N1" bash -c "test -f /var/lib/blockstor-satellite/${RD}.res"; then
    echo "FAIL: .res file rendered despite LayerStack=[STORAGE]"
    exit 1
fi

echo ">> NO-DRBD OK (LayerStack=[STORAGE], dev=$DEV, md5=$md5_pre)"
