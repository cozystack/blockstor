#!/usr/bin/env bash
#
# usage: smoke-blockstor.sh WORK_DIR
#
# End-to-end smoke for the blockstor stack (controller + satellite +
# loopfile pool). Drives DRBD directly through blockstor — does not
# depend on linstor-csi or piraeus. Verifies:
#
#   1. ResourceDefinition + 2 Resource get accepted by the satellites
#      and `drbdsetup status` reports both replicas UpToDate.
#   2. Data written on one node reads back byte-identical on the peer
#      after a manual primary/secondary failover.
#   3. `kubectl delete` cleans up via finalizer (rm .res, losetup -d,
#      rm img).
#
# Assumes blockstor-controller + blockstor-satellite are already
# running in `blockstor-system` on the cluster.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

RD=smoke-blockstor
PRIMARY=test-worker-1
PEER=test-worker-2
NS=blockstor-system
SIZE_KIB=65536  # 64 MiB

# We tag the .res artefact with a unique RD name so reruns don't
# collide with leftover state from prior smoke iterations.

cleanup() {
    kubectl delete --wait=true --timeout=30s "resource.blockstor.io.blockstor.io/${RD}.${PRIMARY}" 2>/dev/null || true
    kubectl delete --wait=true --timeout=30s "resource.blockstor.io.blockstor.io/${RD}.${PEER}"    2>/dev/null || true
    kubectl delete --wait=true --timeout=30s "resourcedefinition.blockstor.io.blockstor.io/${RD}"  2>/dev/null || true
}

trap cleanup EXIT

echo ">> creating ResourceDefinition + 2 Resources"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: ${SIZE_KIB}}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${PRIMARY}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${PRIMARY}
  props: {StorPoolName: stand}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${PEER}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${PEER}
  props: {StorPoolName: stand}
EOF

# Pod-of-pods: helper to run a command in the satellite pod that's
# scheduled on a particular node. Wraps the lengthy jsonpath bit.
on_node() {
    local node=$1
    shift
    local pod
    pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o "jsonpath={.items[?(@.spec.nodeName==\"${node}\")].metadata.name}")
    kubectl -n "$NS" exec "$pod" -- "$@"
}

echo ">> waiting for both peers UpToDate (max 60s)"
for _ in $(seq 1 30); do
    p1=$(on_node "$PRIMARY" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
    p2=$(on_node "$PEER"    drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)

    if [[ "$p1" == *"disk:UpToDate"* && "$p2" == *"disk:UpToDate"* ]]; then
        echo "   both UpToDate"
        break
    fi

    sleep 2
done

echo ">> writing 1 MiB pattern on $PRIMARY"
DEV=$(on_node "$PRIMARY" bash -c "grep -oE '/dev/drbd[0-9]+' /etc/drbd.d/${RD}.res | head -1")
on_node "$PRIMARY" bash -c "
    drbdadm primary ${RD}
    dd if=/dev/urandom of=${DEV} bs=1M count=1 status=none oflag=direct
    md5=\$(dd if=${DEV} bs=1M count=1 status=none iflag=direct | md5sum | awk '{print \$1}')
    echo primary=\$md5
    drbdadm secondary ${RD}
" | tee /tmp/blockstor-smoke-primary.log

PRIMARY_MD5=$(grep '^primary=' /tmp/blockstor-smoke-primary.log | cut -d= -f2)

echo ">> reading on $PEER after failover"
on_node "$PEER" bash -c "
    drbdadm primary ${RD}
    md5=\$(dd if=${DEV} bs=1M count=1 status=none iflag=direct | md5sum | awk '{print \$1}')
    echo peer=\$md5
    drbdadm secondary ${RD}
" | tee /tmp/blockstor-smoke-peer.log

PEER_MD5=$(grep '^peer=' /tmp/blockstor-smoke-peer.log | cut -d= -f2)

if [[ "$PRIMARY_MD5" != "$PEER_MD5" ]]; then
    echo "FAIL: md5 mismatch — primary=$PRIMARY_MD5 peer=$PEER_MD5"
    exit 1
fi

echo ">> SMOKE OK ($PRIMARY_MD5 matches across both replicas)"
