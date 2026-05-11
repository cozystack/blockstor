#!/usr/bin/env bash
#
# usage: linstor-cli-replica-move.sh WORK_DIR
#
# Exercises the full replica-move flow through the upstream linstor
# CLI:
#   1. linstor resource-definition / volume-definition create
#   2. linstor resource create on $WORKER_1 + $WORKER_2  (2 replicas)
#   3. wait both UpToDate
#   4. linstor resource create on $WORKER_3              (3rd replica)
#   5. wait all three UpToDate
#   6. linstor resource delete on $WORKER_1              (drop old replica)
#   7. assert WORKER_1's Resource CRD vanishes and the other two
#      stay UpToDate — the operator-driven equivalent of an
#      "evacuate" without using kubectl apply.
#
# Skipped on hosts without the `linstor` CLI binary.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

if ! command -v linstor >/dev/null 2>&1; then
    echo "SKIP: linstor CLI not in PATH (apt install linstor-client)"
    exit 0
fi

RD=e2e-cli-replica-move

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n blockstor-system port-forward svc/blockstor-controller "$PF_PORT":3370 \
    >/tmp/linstor-cli-replica-move-pf.log 2>&1 &
PF_PID=$!

cleanup() {
    kubectl delete resource --all --force --grace-period=0 --ignore-not-found 2>/dev/null || true
    kubectl delete resourcedefinition --all --ignore-not-found 2>/dev/null || true
    kill "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 10); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:$PF_PORT" --machine-readable)

echo ">> linstor resource-definition + volume-definition create $RD"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 32M >/dev/null

echo ">> linstor resource create $WORKER_1 / $WORKER_2 $RD (initial 2 replicas)"
"${LCTL[@]}" resource create "$WORKER_1" "$RD" --storage-pool stand >/dev/null
"${LCTL[@]}" resource create "$WORKER_2" "$RD" --storage-pool stand >/dev/null

wait_uptodate "$RD" "$WORKER_1" "$WORKER_2"

echo ">> linstor resource create $WORKER_3 $RD (add 3rd replica)"
"${LCTL[@]}" resource create "$WORKER_3" "$RD" --storage-pool stand >/dev/null

echo ">> wait 180s for $WORKER_3 to reach UpToDate"
deadline=$(( $(date +%s) + 180 ))
while (( $(date +%s) < deadline )); do
    state3=$(on_node "$WORKER_3" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
    if [[ "$state3" == *"disk:UpToDate"* ]]; then
        break
    fi
    sleep 2
done

if [[ "$state3" != *"disk:UpToDate"* ]]; then
    echo "FAIL: $WORKER_3 never reached UpToDate after add (got: $state3)"
    exit 1
fi

echo ">> linstor resource delete $WORKER_1 $RD (drop old replica)"
"${LCTL[@]}" resource delete "$WORKER_1" "$RD" >/dev/null

echo ">> wait up to 120s for $WORKER_1's Resource CRD to disappear"
deadline=$(( $(date +%s) + 120 ))
while (( $(date +%s) < deadline )); do
    if ! kubectl get resource "$RD.$WORKER_1" >/dev/null 2>&1; then
        break
    fi
    sleep 2
done

if kubectl get resource "$RD.$WORKER_1" >/dev/null 2>&1; then
    echo "FAIL: $RD.$WORKER_1 still present after CLI delete"
    exit 1
fi

echo ">> verify $WORKER_2 + $WORKER_3 stay UpToDate after the move"
for w in "$WORKER_2" "$WORKER_3"; do
    state=$(on_node "$w" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
    if [[ "$state" != *"disk:UpToDate"* ]]; then
        echo "FAIL: $w drifted from UpToDate after move (got: $state)"
        exit 1
    fi
done

echo ">> LINSTOR-CLI-REPLICA-MOVE OK ($WORKER_1 → $WORKER_3 round-trip via CLI)"
