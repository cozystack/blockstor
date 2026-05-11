#!/usr/bin/env bash
#
# usage: resize-luks.sh WORK_DIR
#
# Tests Phase 8.2 "volume resize" combined with Phase 6 LUKS layer.
# Setup:
#   - LUKS-encrypted RD on workers 1+2, initial size 64 MiB
#   - write a known pattern, capture md5
# Steps:
#   1. PUT /v1/resource-definitions/{rd}/volume-definitions/0 to bump size to 128 MiB
#   2. wait for satellite reconciler to:
#        a. provider.ResizeVolume (lvextend / zfs set volsize / truncate)
#        b. cryptsetup resize on the LUKS layer
#        c. drbdadm resize --assume-clean
# Expected:
#   - DRBD reports the new size
#   - the original 64-MiB region still reads back the original md5

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD=e2e-resize-luks
N1=$WORKER_1
N2=$WORKER_2
SIZE_INITIAL_KIB=65536    # 64 MiB
SIZE_GROWN_KIB=131072     # 128 MiB

trap 'delete_rd "$RD"' EXIT

echo ">> apply LUKS-encrypted RD ($SIZE_INITIAL_KIB KiB)"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  layerStack: ["DRBD", "LUKS", "STORAGE"]
  props:
    DrbdOptions/Encryption/passphrase: "this-is-a-32-byte-test-passphrase!!"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: ${SIZE_INITIAL_KIB}}
EOF
for n in "$N1" "$N2"; do
    cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${n}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${n}
  props: {StorPoolName: stand}
EOF
done

wait_uptodate "$RD" "$N1" "$N2"
DEV=$(device_for_rd "$RD" "$N1")
md5_pre=$(write_random "$N1" "$DEV" 4194304)

echo ">> resize via REST → $SIZE_GROWN_KIB KiB"
rest_put "/v1/resource-definitions/${RD}/volume-definitions/0" \
    "{\"size_kib\":${SIZE_GROWN_KIB}}"

echo ">> wait 60s for satellite resize chain"
TOLERANCE_KIB=128
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    cur_kib=$(on_node "$N1" bash -c "blockdev --getsize64 ${DEV}" 2>/dev/null || true)
    cur_kib=$(( ${cur_kib:-0} / 1024 ))
    if (( cur_kib + TOLERANCE_KIB >= SIZE_GROWN_KIB )); then
        break
    fi
    sleep 2
done

if (( cur_kib + TOLERANCE_KIB < SIZE_GROWN_KIB )); then
    echo "FAIL: device size $cur_kib < $SIZE_GROWN_KIB after 60s (tolerance ${TOLERANCE_KIB})"
    exit 1
fi

echo ">> read first 4 MiB — md5 must still match"
md5_post=$(read_md5 "$N1" "$DEV" 4194304)
if [[ "$md5_pre" != "$md5_post" ]]; then
    echo "FAIL: data corrupted by resize (pre=$md5_pre post=$md5_post)"
    exit 1
fi

echo ">> RESIZE-LUKS OK ($SIZE_INITIAL_KIB → $SIZE_GROWN_KIB KiB, data preserved)"
