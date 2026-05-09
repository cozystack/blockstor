#!/usr/bin/env bash
# Shared helpers for tests/e2e/*.sh — keeps each scenario script
# focused on the scenario itself, not on boilerplate. Sourced from the
# scenario, never executed directly.
#
# Conventions:
#   - All scripts take WORK_DIR as $1 (matches stand/Makefile).
#   - $KUBECONFIG is set from WORK_DIR/kubeconfig.
#   - Per-test timeout knobs live at the top of each script.
#   - Use on_node() to reach a satellite pod; never hard-code pod names.

set -euo pipefail

NS=${NS:-blockstor-system}

# on_node runs CMD inside the satellite pod scheduled on NODE.
# Wraps the jsonpath dance; quote args carefully.
on_node() {
    local node=$1
    shift
    local pod
    pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o "jsonpath={.items[?(@.spec.nodeName==\"${node}\")].metadata.name}")

    if [[ -z "$pod" ]]; then
        echo "no satellite pod on node $node" >&2
        return 1
    fi

    kubectl -n "$NS" exec "$pod" -- "$@"
}

# wait_uptodate POD waits up to 120s for both replicas of $RD to reach
# disk:UpToDate. Caller defines $RD and the two node names $PRIMARY,
# $PEER before calling. Exits non-zero on timeout. Initial sync on
# a fresh DRBD resource takes 30-60s on a busy stand; 120s is the
# margin.
wait_uptodate() {
    local rd=$1 primary=$2 peer=$3 deadline=$(( $(date +%s) + 120 ))

    while (( $(date +%s) < deadline )); do
        local p1 p2
        p1=$(on_node "$primary" drbdsetup status "$rd" 2>/dev/null | grep "disk:" | head -1 || true)
        p2=$(on_node "$peer"    drbdsetup status "$rd" 2>/dev/null | grep "disk:" | head -1 || true)

        if [[ "$p1" == *"disk:UpToDate"* && "$p2" == *"disk:UpToDate"* ]]; then
            return 0
        fi

        sleep 2
    done

    echo "FAIL: $rd never reached UpToDate on both peers" >&2
    return 1
}

# device_for_rd resolves the local /dev/drbdN minor for an RD.
device_for_rd() {
    local rd=$1 node=$2
    on_node "$node" bash -c "grep -oE '/dev/drbd[0-9]+' /etc/drbd.d/${rd}.res | head -1"
}

# write_random NODE DEV BYTES — write urandom to the device, return md5.
write_random() {
    local node=$1 dev=$2 bytes=$3
    on_node "$node" bash -c "
        drbdadm primary ${RD} 2>/dev/null || true
        dd if=/dev/urandom of=${dev} bs=1 count=${bytes} status=none oflag=direct
        md5sum < <(dd if=${dev} bs=1 count=${bytes} status=none iflag=direct) | awk '{print \$1}'
    "
}

# read_md5 NODE DEV BYTES — read first BYTES of DEV, return md5.
read_md5() {
    local node=$1 dev=$2 bytes=$3
    on_node "$node" bash -c "
        drbdadm primary ${RD} 2>/dev/null || true
        md5sum < <(dd if=${dev} bs=1 count=${bytes} status=none iflag=direct) | awk '{print \$1}'
    "
}

# delete_rd cleans up an RD + every Resource named after it.
# Trapped from each scenario so partial runs don't leave orphans.
delete_rd() {
    local rd=$1
    kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
        | awk -v rd="$rd." '$1 ~ "^"rd {print $1}' \
        | xargs -r kubectl delete --wait=true --timeout=30s resources.blockstor.io.blockstor.io 2>/dev/null || true
    kubectl delete --wait=true --timeout=30s "resourcedefinitions.blockstor.io.blockstor.io/${rd}" 2>/dev/null || true
}

# require_workers enforces that the cluster has at least N satellite
# nodes Ready. Useful for scenarios that cannot run on a 2-node setup.
require_workers() {
    local want=$1
    local got
    got=$(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' --no-headers 2>/dev/null \
        | awk '$2 == "Ready"' | wc -l)

    if (( got < want )); then
        echo "SKIP: scenario needs $want satellite workers, found $got" >&2
        exit 0
    fi
}

# rd_apply applies a 2-replica RD with given size onto the named pair
# of workers. Used by scenarios that don't need the full apply boilerplate.
rd_apply() {
    local rd=$1 primary=$2 peer=$3 size=${4:-65536}
    cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${rd}}
spec:
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: ${size}}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${rd}.${primary}}
spec:
  resourceDefinitionName: ${rd}
  nodeName: ${primary}
  props: {StorPoolName: stand}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${rd}.${peer}}
spec:
  resourceDefinitionName: ${rd}
  nodeName: ${peer}
  props: {StorPoolName: stand}
EOF
}
