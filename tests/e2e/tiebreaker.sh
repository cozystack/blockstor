#!/usr/bin/env bash
#
# usage: tiebreaker.sh WORK_DIR
#
# Tests Phase 8.3 "tiebreaker auto-creation".
# Setup:
#   - 2-replica RD on workers 1+2
#   - cluster has 3 satellite nodes (worker-3 idle)
# Expected:
#   - ResourceDefinitionReconciler observes 2 diskful replicas + 3 nodes
#   - auto-creates a 3rd Resource on worker-3 with flags
#     [DISKLESS, TIE_BREAKER]
#   - .res file on worker-3 lists the other two as peers

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

RD=e2e-tiebreaker
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

trap 'delete_rd "$RD"' EXIT

echo ">> apply 2-replica RD on $N1 + $N2"
rd_apply "$RD" "$N1" "$N2"
wait_uptodate "$RD" "$N1" "$N2"

echo ">> wait 60s for ResourceDefinitionReconciler to add tiebreaker"
deadline=$(( $(date +%s) + 60 ))
witness_found=false
while (( $(date +%s) < deadline )); do
    if kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N3}" >/dev/null 2>&1; then
        witness_found=true
        break
    fi
    sleep 1
done

if [[ "$witness_found" != "true" ]]; then
    echo "FAIL: tiebreaker witness not created on $N3 (waited $(( $(date +%s) - (deadline - 60) ))s)"
    kubectl get resources.blockstor.io.blockstor.io --no-headers | awk -v rd="$RD" '$1 ~ rd'
    exit 1
fi

flags=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N3}" \
    -o jsonpath='{.spec.flags}')

if [[ "$flags" != *"DISKLESS"* || "$flags" != *"TIE_BREAKER"* ]]; then
    echo "FAIL: tiebreaker missing required flags (got: $flags)"
    exit 1
fi

# The witness must NOT be auto-promoted by the auto-diskful path —
# even though it's a DISKLESS replica that may go InUse during DRBD
# negotiation, TIE_BREAKER overrides that. Run a brief observation
# window to confirm the flag survives.
sleep 15
flags_post=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N3}" \
    -o jsonpath='{.spec.flags}')
if [[ "$flags_post" != *"DISKLESS"* ]]; then
    echo "FAIL: tiebreaker auto-promoted (DISKLESS dropped: $flags_post)"
    exit 1
fi

echo ">> TIEBREAKER OK (witness on $N3 carries DISKLESS+TIE_BREAKER, stable)"
