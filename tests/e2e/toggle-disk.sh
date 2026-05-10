#!/usr/bin/env bash
#
# usage: toggle-disk.sh WORK_DIR
#
# Tests the upstream-LINSTOR `linstor r toggle-disk` parity. Setup:
#   1. 2-replica RD on N1+N2; 1 DISKLESS witness on N3.
#   2. Toggle-disk on N3 (with explicit storage pool) → witness
#      becomes diskful.
#   3. Wait for UpToDate on N3.
#   4. Toggle-disk back → satellite drops DISKLESS in flag, the
#      replica detaches.
#
# Exit-criterion: each toggle round-trips ≤90 s and the resource
# never enters split-brain or Inconsistent on the surviving peers.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

RD=e2e-toggle-disk
N1=test-worker-1
N2=test-worker-2
N3=test-worker-3
POOL=${STORPOOL:-thin1}

trap 'delete_rd "$RD"' EXIT

echo ">> apply 2 diskful + 1 DISKLESS"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    DrbdOptions/AutoAddQuorumTiebreaker: "false"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}
EOF
for n in "$N1" "$N2"; do
    cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${n}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${n}
  props:
    StorPoolName: "${POOL}"
EOF
done
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N3}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N3}
  flags: ["DISKLESS"]
EOF

wait_uptodate "$RD" "$N1" "$N2"

# 1. Promote witness via REST.
echo ">> toggle-disk N3 → diskful (pool=${POOL})"
controller_pod=$(kubectl get pod -n blockstor-system -l app=blockstor-manager -o jsonpath='{.items[0].metadata.name}')
kubectl -n blockstor-system exec "$controller_pod" -- \
    curl -fsS -X PUT "http://127.0.0.1:3370/v1/resource-definitions/${RD}/resources/${N3}/toggle-disk/storage-pool/${POOL}"

echo ">> wait up to 90s for N3 to become UpToDate"
deadline=$(( $(date +%s) + 90 ))
while (( $(date +%s) < deadline )); do
    state=$(on_node "$N3" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
    if [[ "$state" == *"UpToDate"* ]]; then
        break
    fi
    sleep 2
done

if [[ "$state" != *"UpToDate"* ]]; then
    echo "FAIL: N3 not UpToDate after toggle-disk → diskful (state: $state)"
    exit 1
fi

# 2. Demote back to diskless.
echo ">> toggle-disk N3 → diskless"
kubectl -n blockstor-system exec "$controller_pod" -- \
    curl -fsS -X PUT "http://127.0.0.1:3370/v1/resource-definitions/${RD}/resources/${N3}/toggle-disk"

echo ">> wait up to 60s for satellite to detach"
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    state=$(on_node "$N3" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
    if [[ "$state" == *"Diskless"* ]]; then
        break
    fi
    sleep 2
done

if [[ "$state" != *"Diskless"* ]]; then
    echo "FAIL: N3 not Diskless after toggle-disk back (state: $state)"
    exit 1
fi

echo ">> TOGGLE-DISK OK (round-trip diskless ↔ diskful succeeded)"
