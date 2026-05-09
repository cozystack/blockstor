#!/usr/bin/env bash
#
# usage: drbd-luks-stack.sh WORK_DIR
#
# Phase 9: layer stack ["DRBD","LUKS","STORAGE"] — encrypted
# at-rest + replicated over DRBD. Each peer holds an independent
# LUKS-encrypted copy; DRBD ships ciphertext between peers.
#
# Setup:
#   - 2-replica RD on workers 1+2 with LayerStack=["DRBD","LUKS","STORAGE"]
#   - DrbdOptions/Encryption/passphrase set on the RD
#   - write urandom on primary + capture md5
# Steps:
#   1. apply RD with explicit layerStack + passphrase
#   2. apply Resources on N1, N2
#   3. wait_uptodate — both peers reach disk:UpToDate over DRBD
#   4. write 4 MiB on primary's /dev/drbdN, read back — md5 matches
#   5. failover: drop primary, secondary promotes, read → same md5
#   6. assert .res `disk` line points at /dev/mapper/${RD}-0-luks
#      (DRBD replicates the cryptsetup mapper, not the raw LV)
# Expected:
#   - both replicas see the same plaintext through their respective
#     LUKS mappers; the underlying LVs hold ciphertext
#   - failover preserves data byte-perfect

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD=e2e-drbd-luks
N1=test-worker-1
N2=test-worker-2
SIZE_KIB=131072    # 128 MiB
PASSPHRASE='this-is-a-32-byte-test-passphrase!!'
STORPOOL=${STORPOOL:-stand}

trap 'delete_rd "$RD"' EXIT

echo ">> apply ${RD} with LayerStack=[DRBD,LUKS,STORAGE]"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  layerStack: ["DRBD", "LUKS", "STORAGE"]
  props:
    DrbdOptions/Encryption/passphrase: "${PASSPHRASE}"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: ${SIZE_KIB}}
EOF

for n in "$N1" "$N2"; do
    cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${n}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${n}
  props: {StorPoolName: ${STORPOOL}}
EOF
done

wait_uptodate "$RD" "$N1" "$N2"

echo ">> assert .res disk line points at LUKS mapper, not raw LV"
res_body=$(on_node "$N1" cat "/var/lib/blockstor-satellite/${RD}.res")
if ! grep -q "/dev/mapper/${RD}-0-luks" <<< "$res_body"; then
    echo "FAIL: .res does not point at LUKS mapper; body=$res_body"
    exit 1
fi

DEV=$(device_for_rd "$RD" "$N1")

echo ">> write+read 4 MiB on primary"
md5_pre=$(write_random "$N1" "$DEV" 4194304)
md5_post=$(read_md5 "$N1" "$DEV" 4194304)
if [[ "$md5_pre" != "$md5_post" ]]; then
    echo "FAIL: round-trip on primary corrupted (pre=$md5_pre post=$md5_post)"
    exit 1
fi

echo ">> failover: delete primary's Resource, force promote on N2"
kubectl delete resource "${RD}.${N1}" --ignore-not-found --wait=true
sleep 5
DEV2=$(device_for_rd "$RD" "$N2")
on_node "$N2" drbdadm primary --force "$RD" || true

md5_failover=$(read_md5 "$N2" "$DEV2" 4194304)
if [[ "$md5_pre" != "$md5_failover" ]]; then
    echo "FAIL: failover corrupted data (pre=$md5_pre after=$md5_failover)"
    exit 1
fi

echo ">> DRBD-LUKS-STACK OK (replicated ciphertext, plaintext stable across failover)"
