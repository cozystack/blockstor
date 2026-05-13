#!/usr/bin/env bash
#
# usage: burnin-blockstor.sh WORK_DIR [DURATION_SEC]
#
# Continuous PVC churn against the deployed blockstor stack. Each
# iteration:
#   - kubectl apply RD + 2 Resources (one auto-primary, one peer)
#   - wait for both UpToDate (≤60 s)
#   - 1 MiB urandom write on Primary, capture md5
#   - failover: Primary → Secondary, peer → Primary
#   - read on peer, compare md5 (must match — exits non-zero otherwise)
#   - delete Resource + RD via finalizer
#
# Default DURATION_SEC = 86400 (24h). The PLAN's "stand running for
# 24h continuous PVC churn" item points at this script.
#
# Reports a summary every 60 iterations: pass/fail counts + recent
# convergence timings.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
DURATION=${2:-86400}

export KUBECONFIG="$WORK_DIR/kubeconfig"

# PRIMARY / PEER auto-discover when not pinned: pick two worker nodes
# (those WITHOUT the control-plane role label). Falls back to first two
# ready nodes if no role label is present. Overridable via env.
discover_workers() {
    local out
    out=$(kubectl get nodes \
        -l '!node-role.kubernetes.io/control-plane' \
        -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' \
        2>/dev/null | head -2)
    if [[ -z "$out" ]]; then
        out=$(kubectl get nodes \
            -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' \
            2>/dev/null | head -2)
    fi
    echo "$out"
}

if [[ -z "${PRIMARY:-}" || -z "${PEER:-}" ]]; then
    mapfile -t _WORKERS < <(discover_workers)
    PRIMARY=${PRIMARY:-${_WORKERS[0]:-}}
    PEER=${PEER:-${_WORKERS[1]:-}}
fi

if [[ -z "$PRIMARY" || -z "$PEER" || "$PRIMARY" == "$PEER" ]]; then
    echo "burnin: failed to discover two distinct worker nodes (PRIMARY=$PRIMARY PEER=$PEER); set them via env" >&2
    exit 2
fi

echo "burnin: PRIMARY=$PRIMARY PEER=$PEER"
NS=blockstor-system
SIZE_KIB=65536  # 64 MiB
DEADLINE=$(( $(date +%s) + DURATION ))

PASS=0
FAIL=0
ITER=0

# on_node runs a command in the satellite pod scheduled on a given node.
on_node() {
    local node=$1
    shift
    local pod
    pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o "jsonpath={.items[?(@.spec.nodeName==\"${node}\")].metadata.name}")
    kubectl -n "$NS" exec "$pod" -- "$@"
}

cleanup_iter() {
    local rd=$1
    kubectl delete --wait=true --timeout=30s "resource.blockstor.io.blockstor.io/${rd}.${PRIMARY}" 2>/dev/null || true
    kubectl delete --wait=true --timeout=30s "resource.blockstor.io.blockstor.io/${rd}.${PEER}"    2>/dev/null || true
    kubectl delete --wait=true --timeout=30s "resourcedefinition.blockstor.io.blockstor.io/${rd}"  2>/dev/null || true
}

while [[ $(date +%s) -lt $DEADLINE ]]; do
    ITER=$((ITER + 1))
    RD="burnin-${ITER}"

    cat <<EOF | kubectl apply -f - >/dev/null
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
spec: {resourceDefinitionName: ${RD}, nodeName: ${PRIMARY}, props: {StorPoolName: stand}}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${PEER}}
spec: {resourceDefinitionName: ${RD}, nodeName: ${PEER}, props: {StorPoolName: stand}}
EOF

    # Wait for UpToDate convergence — bail out fast if it doesn't
    # land in 60 s, that's a regression we want to capture not paper over.
    BOTH=0
    for _ in $(seq 1 30); do
        p1=$(on_node "$PRIMARY" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
        p2=$(on_node "$PEER"    drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
        if [[ "$p1" == *"disk:UpToDate"* && "$p2" == *"disk:UpToDate"* ]]; then
            BOTH=1
            break
        fi
        sleep 2
    done

    if [[ $BOTH -ne 1 ]]; then
        FAIL=$((FAIL + 1))
        echo "[$(date -u +%FT%TZ)] iter=$ITER FAIL: convergence timeout"
        cleanup_iter "$RD"
        continue
    fi

    DEV=$(on_node "$PRIMARY" bash -c "grep -oE '/dev/drbd[0-9]+' /etc/drbd.d/${RD}.res | head -1")
    PRIMARY_MD5=$(on_node "$PRIMARY" bash -c "
        drbdadm primary ${RD}
        dd if=/dev/urandom of=${DEV} bs=1M count=1 status=none oflag=direct
        dd if=${DEV} bs=1M count=1 status=none iflag=direct | md5sum | awk '{print \$1}'
        drbdadm secondary ${RD}
    " | tail -1)

    PEER_MD5=$(on_node "$PEER" bash -c "
        drbdadm primary ${RD}
        dd if=${DEV} bs=1M count=1 status=none iflag=direct | md5sum | awk '{print \$1}'
        drbdadm secondary ${RD}
    " | tail -1)

    if [[ "$PRIMARY_MD5" == "$PEER_MD5" ]]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "[$(date -u +%FT%TZ)] iter=$ITER FAIL: md5 mismatch primary=$PRIMARY_MD5 peer=$PEER_MD5"
    fi

    cleanup_iter "$RD"

    if (( ITER % 60 == 0 )); then
        echo "[$(date -u +%FT%TZ)] iter=$ITER pass=$PASS fail=$FAIL elapsed=$(( $(date +%s) - DEADLINE + DURATION ))s"
    fi
done

echo "[$(date -u +%FT%TZ)] DONE iter=$ITER pass=$PASS fail=$FAIL"
[[ $FAIL -eq 0 ]] || exit 1
