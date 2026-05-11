#!/usr/bin/env bash
#
# usage: luks-layer.sh WORK_DIR
#
# Phase 9: layer stack ["LUKS","STORAGE"] — single-replica encrypted
# PVC. cryptsetup luksFormat runs on first activation, luksOpen on
# every reconcile. Consumer Pod sees /dev/mapper/<rd>-<vol>-luks. No
# DRBD layer.
#
# Setup:
#   - 1-replica RD on worker-1 with LayerStack=["LUKS","STORAGE"]
#   - DrbdOptions/Encryption/passphrase set on the RD
#   - write urandom + capture md5
# Steps:
#   1. apply RD with explicit layerStack + passphrase prop
#   2. apply Resource on N1
#   3. wait for satellite to: provision LV → luksFormat → luksOpen
#   4. write 4 MiB pattern, read back — md5 must match
#   5. simulate satellite restart: kubectl rollout restart daemonset
#      blockstor-satellite. After the pod comes back, the mapper must
#      be re-opened (luksOpen on existing LUKS device) and the same
#      md5 must read back — proves header survives, key is re-derived.
# Expected:
#   - data round-trips through the cryptsetup mapper
#   - /etc/drbd.d/${RD}.res absent (no DRBD)
#   - /dev/mapper/${RD}-0-luks present after reconcile

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

RD=e2e-luks-layer
N1=$WORKER_1
SIZE_KIB=131072    # 128 MiB
PASSPHRASE='this-is-a-32-byte-test-passphrase!!'
STORPOOL=${STORPOOL:-stand}

trap 'delete_rd "$RD"' EXIT

echo ">> apply ${RD} with LayerStack=[LUKS,STORAGE]"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  layerStack: ["LUKS", "STORAGE"]
  props:
    DrbdOptions/Encryption/passphrase: "${PASSPHRASE}"
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

echo ">> wait up to 60s for satellite to luksFormat + luksOpen"
deadline=$(( $(date +%s) + 60 ))
DEV=""
while (( $(date +%s) < deadline )); do
    if on_node "$N1" bash -c "test -e /dev/mapper/${RD}-0-luks"; then
        DEV=/dev/mapper/${RD}-0-luks
        break
    fi
    sleep 2
done

if [[ -z "$DEV" ]]; then
    echo "FAIL: /dev/mapper/${RD}-0-luks not present after 60s"
    exit 1
fi

echo ">> assert no .res rendered (no DRBD)"
if on_node "$N1" bash -c "test -f /var/lib/blockstor-satellite/${RD}.res"; then
    echo "FAIL: .res file rendered despite LayerStack=[LUKS,STORAGE]"
    exit 1
fi

echo ">> write+read 4 MiB pattern"
md5_pre=$(write_random "$N1" "$DEV" 4194304)
md5_post=$(read_md5 "$N1" "$DEV" 4194304)

if [[ "$md5_pre" != "$md5_post" ]]; then
    echo "FAIL: data corrupted (pre=$md5_pre post=$md5_post)"
    exit 1
fi

echo ">> rollout-restart satellite, wait for re-open"
kubectl -n "${NS}" rollout restart daemonset blockstor-satellite
kubectl -n "${NS}" rollout status daemonset blockstor-satellite --timeout=120s

deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    if on_node "$N1" bash -c "test -e /dev/mapper/${RD}-0-luks"; then
        break
    fi
    sleep 2
done

if ! on_node "$N1" bash -c "test -e /dev/mapper/${RD}-0-luks"; then
    echo "FAIL: mapper not reopened after satellite restart"
    exit 1
fi

md5_after_restart=$(read_md5 "$N1" "$DEV" 4194304)
if [[ "$md5_pre" != "$md5_after_restart" ]]; then
    echo "FAIL: data lost across satellite restart (pre=$md5_pre post=$md5_after_restart)"
    exit 1
fi

echo ">> LUKS-LAYER OK (encrypt → write → restart → re-open → md5 stable)"
