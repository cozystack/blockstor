#!/usr/bin/env bash
#
# usage: auto-diskful.sh WORK_DIR
#
# Tests Phase 8.3 "auto-diskful" promotion.
# Setup:
#   - 2 diskful replicas of an RD on workers 1+2
#   - 1 DISKLESS replica on worker-3 (cluster-wide attachable pattern)
# Steps:
#   1. promote $N3 to Primary so Status.InUse=true on the DISKLESS replica
#   2. ResourceReconciler sees: InUse + DISKLESS + non-tiebreaker + pool available
#   3. removes DISKLESS flag, stamps StorPoolName onto Spec.Props
#   4. satellite reconciler picks up the change, creates the LV / drbdadm attach

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

RD=e2e-auto-diskful
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

trap 'delete_rd "$RD"' EXIT

echo ">> apply 2 diskful + 1 DISKLESS"
# Disable the auto-tiebreaker — this test explicitly creates its
# own DISKLESS replica on N3, and we don't want the RD reconciler
# racing to create a TIE_BREAKER witness on N3 first (which would
# then conflict with our explicit apply, ALSO landing on N3).
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
  props: {StorPoolName: stand}
EOF
done

# Make sure $N3 is NOT a tiebreaker — explicitly diskless without the
# TIE_BREAKER flag. (ResourceDefinitionReconciler would otherwise add
# its own witness on $N3 for the 2-diskful → +tiebreaker rule, and it
# would be exempt from auto-diskful.)
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

# Drive InUse on $N3 by promoting it.
on_node "$N3" drbdadm primary "$RD"

echo ">> wait 90s for auto-diskful promotion"
deadline=$(( $(date +%s) + 90 ))
while (( $(date +%s) < deadline )); do
    flags=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N3}" \
        -o jsonpath='{.spec.flags}' 2>/dev/null || true)
    if [[ "$flags" != *"DISKLESS"* ]]; then
        break
    fi
    sleep 2
done

if [[ "$flags" == *"DISKLESS"* ]]; then
    echo "FAIL: auto-diskful did not run (flags still: $flags)"
    exit 1
fi

stor=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N3}" \
    -o jsonpath='{.spec.props.StorPoolName}')
if [[ -z "$stor" ]]; then
    echo "FAIL: StorPoolName not stamped on promoted replica"
    exit 1
fi

echo ">> AUTO-DISKFUL OK ($N3 promoted: pool=$stor, DISKLESS removed)"
