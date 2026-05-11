#!/usr/bin/env bash
#
# usage: evacuate.sh WORK_DIR
#
# Tests Phase 8.3 "linstor node evacuate actually migrates replicas".
# Setup:
#   - 3-node cluster, 2-replica RD on workers 1+2, worker-3 idle
#   - flag worker-1 EVICTED via REST
# Expected:
#   - NodeReconciler creates a 3rd Resource on worker-3 (the migration)
#   - parent RD now has 3 Resources
#   - worker-1's Resource still present (operator decides when to drop it)

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

RD=e2e-evacuate
SOURCE=$WORKER_1
PEER=$WORKER_2
TARGET=$WORKER_3

trap 'delete_rd "$RD"' EXIT

echo ">> apply 2-replica RD ($SOURCE + $PEER)"
rd_apply "$RD" "$SOURCE" "$PEER"
wait_uptodate "$RD" "$SOURCE" "$PEER"

echo ">> mark $SOURCE EVICTED"
kubectl patch nodes.blockstor.io.blockstor.io "$SOURCE" --type=merge \
    -p '{"spec":{"flags":["EVICTED"]}}'

echo ">> wait up to 60s for migration to $TARGET"
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    if kubectl get "resources.blockstor.io.blockstor.io/${RD}.${TARGET}" >/dev/null 2>&1; then
        break
    fi
    sleep 2
done

if ! kubectl get "resources.blockstor.io.blockstor.io/${RD}.${TARGET}" >/dev/null 2>&1; then
    echo "FAIL: NodeReconciler did not create replacement on $TARGET"
    kubectl get resources.blockstor.io.blockstor.io --no-headers | awk -v rd="$RD" '$1 ~ rd'
    exit 1
fi

# Source replica must STILL exist — EVICTED is a "drain" semantic, not
# a "delete". LOST flag is the destructive variant (covered separately
# by the eviction unit tests).
if ! kubectl get "resources.blockstor.io.blockstor.io/${RD}.${SOURCE}" >/dev/null 2>&1; then
    echo "FAIL: source replica on $SOURCE removed prematurely (EVICTED ≠ LOST)"
    exit 1
fi

echo ">> EVACUATE OK ($SOURCE+$PEER → migrated to $TARGET, source still present)"
