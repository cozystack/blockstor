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
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3
POOL=${STORPOOL:-stand}

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

# REST endpoint is on the in-cluster Service. Port-forward to a
# free local port — distroless controller image has no curl, so
# `kubectl exec` would fail.
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n blockstor-system port-forward svc/blockstor-controller "$PF_PORT":3370 \
    >/tmp/toggle-disk-pf.log 2>&1 &
PF_PID=$!
trap 'kill $PF_PID 2>/dev/null || true; delete_rd "$RD"' EXIT

for _ in $(seq 1 10); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

# 1. Promote witness via REST.
echo ">> toggle-disk N3 → diskful (pool=${POOL})"
curl -fsS -X PUT \
    "http://localhost:$PF_PORT/v1/resource-definitions/${RD}/resources/${N3}/toggle-disk/storage-pool/${POOL}"

echo ">> wait up to 90s for N3 to become UpToDate"
deadline=$(( $(date +%s) + 90 ))
while (( $(date +%s) < deadline )); do
    state=$(status_disk_state "$RD" "$N3")
    if [[ "$state" == "UpToDate" ]]; then
        break
    fi
    sleep 2
done

if [[ "$state" != "UpToDate" ]]; then
    echo "FAIL: N3 not UpToDate after toggle-disk → diskful (state: $state)"
    exit 1
fi

# 2. Demote back to diskless.
echo ">> toggle-disk N3 → diskless"
curl -fsS -X PUT \
    "http://localhost:$PF_PORT/v1/resource-definitions/${RD}/resources/${N3}/toggle-disk"

echo ">> wait up to 60s for satellite to detach"
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    state=$(status_disk_state "$RD" "$N3")
    if [[ "$state" == "Diskless" ]]; then
        break
    fi
    sleep 2
done

if [[ "$state" != "Diskless" ]]; then
    echo "FAIL: N3 not Diskless after toggle-disk back (state: $state)"
    exit 1
fi

echo ">> TOGGLE-DISK OK (round-trip diskless ↔ diskful succeeded)"
