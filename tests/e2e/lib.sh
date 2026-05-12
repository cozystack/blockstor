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

# Discover the cluster's worker node names so scripts can reference
# them as $WORKER_1, $WORKER_2, $WORKER_3 instead of hardcoding a
# specific cluster prefix (parallel stands name workers `<NAME>-worker-N`).
# Sorted alphabetically so $WORKER_1 == worker-1, etc.
mapfile -t _BS_WORKERS < <(
    kubectl get nodes -l '!node-role.kubernetes.io/control-plane' \
        -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | sort
)
WORKER_1="${_BS_WORKERS[0]:-}"
WORKER_2="${_BS_WORKERS[1]:-}"
WORKER_3="${_BS_WORKERS[2]:-}"
export WORKER_1 WORKER_2 WORKER_3

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

# wait_uptodate POD waits up to 180s for both replicas of $RD to reach
# disk:UpToDate. Caller defines $RD and the two node names $PRIMARY,
# $PEER before calling. Exits non-zero on timeout. Initial sync on a
# fresh DRBD resource on a busy QEMU stand can take 60-120s; 180s is
# the safety margin.
wait_uptodate() {
    local rd=$1 primary=$2 peer=$3 deadline=$(( $(date +%s) + 180 ))

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
# BYTES is rounded up to a 4096-byte block (direct I/O alignment).
write_random() {
    local node=$1 dev=$2 bytes=$3
    local blocks=$(( (bytes + 4095) / 4096 ))
    on_node "$node" bash -c "
        drbdadm primary ${RD} 2>/dev/null || true
        dd if=/dev/urandom of=${dev} bs=4096 count=${blocks} status=none oflag=direct
        dd if=${dev} bs=4096 count=${blocks} status=none iflag=direct | md5sum | awk '{print \$1}'
    "
}

# read_md5 NODE DEV BYTES — read first BYTES of DEV, return md5.
# Same alignment rules as write_random.
read_md5() {
    local node=$1 dev=$2 bytes=$3
    local blocks=$(( (bytes + 4095) / 4096 ))
    on_node "$node" bash -c "
        drbdadm primary ${RD} 2>/dev/null || true
        dd if=${dev} bs=4096 count=${blocks} status=none iflag=direct | md5sum | awk '{print \$1}'
    "
}

# delete_rd cleans up an RD + every Resource named after it + every
# Snapshot of the RD. Trapped from each scenario so partial runs
# don't leave orphans that trip the next test's wait_uptodate with
# stale kernel / .res / marker / snapshot state. Belt-and-suspenders
# at every layer:
#
#   - delete Snapshot CRDs (otherwise the satellite-side reconciler
#     re-asserts kernel state for "still-needed-for-snapshot" devices)
#   - delete Resource CRDs (waits on finalizers; the satellite-side
#     teardown chain runs drbdadm down + provider.DeleteVolume)
#   - delete the RD CRD
#   - on every satellite: drbdsetup down + remove .res + remove the
#     .md-created marker (otherwise re-create with the same name
#     skips drbdadm create-md and trips 'No valid meta data found')
delete_rd() {
    local rd=$1

    kubectl get snapshots.blockstor.io.blockstor.io --no-headers 2>/dev/null \
        | awk -v rd="$rd." '$1 ~ "^"rd {print $1}' \
        | xargs -r kubectl delete --wait=true --timeout=30s snapshots.blockstor.io.blockstor.io 2>/dev/null || true
    kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
        | awk -v rd="$rd." '$1 ~ "^"rd {print $1}' \
        | xargs -r kubectl delete --wait=true --timeout=30s resources.blockstor.io.blockstor.io 2>/dev/null || true
    kubectl delete --wait=true --timeout=30s "resourcedefinitions.blockstor.io.blockstor.io/${rd}" 2>/dev/null || true

    # Force-kill any lingering kernel-level state for this RD. The
    # marker-file cleanup is essential — leaving .md-created behind
    # makes the next re-create with the same RD name silently skip
    # drbdadm create-md, so drbdadm adjust then fails with 'No valid
    # meta data found' on the freshly-allocated lower disk.
    #
    # Outer + inner timeouts: `drbdsetup down` can hang forever if
    # the kernel module has a half-open connection to a force-deleted
    # peer (DRBD-9 keeps trying to gracefully tear). Without these
    # the test's EXIT trap blocks the next scenario indefinitely.
    for pod in $(kubectl -n "$NS" get pods -l app=blockstor-satellite -o name 2>/dev/null); do
        timeout 15 kubectl -n "$NS" exec "$pod" -- bash -c "
            timeout 5 drbdsetup down ${rd} 2>/dev/null || true
            rm -f /etc/drbd.d/${rd}.res /etc/drbd.d/${rd}.md-created
            rm -f /var/lib/blockstor-pool/${rd}_*.partial 2>/dev/null || true
        " 2>/dev/null || true
    done
}

# wait_cluster_idle waits until the stand is back to a clean slate
# between back-to-back e2e scenarios on the same cluster — no
# blockstor CRDs for resources / RDs / snapshots, and no kernel-side
# DRBD configuration. Returns success once both layers are empty or
# after the deadline expires (best-effort; logs to stderr but doesn't
# fail). The batch driver should call this before launching the next
# scenario so resize-luks / linstor-cli / cross-node don't observe
# the previous test's residue.
wait_cluster_idle() {
    local deadline=$(( $(date +%s) + 30 ))

    while (( $(date +%s) < deadline )); do
        local crd_count drbd_busy=0
        crd_count=$( {
            kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null
            kubectl get resourcedefinitions.blockstor.io.blockstor.io --no-headers 2>/dev/null
            kubectl get snapshots.blockstor.io.blockstor.io --no-headers 2>/dev/null
        } | grep -cv '^$' || true )

        for pod in $(kubectl -n "$NS" get pods -l app=blockstor-satellite -o name 2>/dev/null); do
            local out
            out=$(kubectl -n "$NS" exec "$pod" -- drbdsetup status 2>/dev/null || true)
            if [[ "$out" != "" && "$out" != *"No currently configured DRBD found"* ]]; then
                drbd_busy=1

                break
            fi
        done

        if [[ "$crd_count" == "0" && "$drbd_busy" == "0" ]]; then
            return 0
        fi

        sleep 2
    done

    echo "wait_cluster_idle: timed out, stand may still have residue" >&2

    return 0
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

# rest_post POSTs JSON BODY to PATH on the in-cluster controller.
# Uses kubectl-port-forward + a host-side curl/wget so we don't need
# curl in the distroless controller image. Path starts with /v1.
rest_post() {
    local path=$1 body=$2

    # Random ephemeral port so back-to-back rest_post / rest_put
    # calls don't collide on TIME_WAIT remnants from the previous
    # port-forward — observed on clone.sh where the second
    # rest_post would bind a stale socket and curl would error 22.
    local lport
    lport=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
    kubectl -n "$NS" port-forward deploy/blockstor-controller "${lport}:3370" >/dev/null 2>&1 &
    local pf=$!

    _wait_port_forward "$lport" "$pf"

    local out
    out=$(curl -fsS -XPOST -H'Content-Type: application/json' \
        "http://127.0.0.1:${lport}${path}" -d "$body")

    kill "$pf" 2>/dev/null || true
    wait "$pf" 2>/dev/null || true

    echo "$out"
}

# rest_put is the PUT variant of rest_post — same port-forward dance.
rest_put() {
    local path=$1 body=$2

    local lport
    lport=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
    kubectl -n "$NS" port-forward deploy/blockstor-controller "${lport}:3370" >/dev/null 2>&1 &
    local pf=$!

    _wait_port_forward "$lport" "$pf"

    local out
    out=$(curl -fsS -XPUT -H'Content-Type: application/json' \
        "http://127.0.0.1:${lport}${path}" -d "$body")

    kill "$pf" 2>/dev/null || true
    wait "$pf" 2>/dev/null || true

    echo "$out"
}

# _wait_port_forward blocks until the forwarded socket actually
# answers (probed via /v1/healthz which is a no-store, no-cache
# 204 from the controller). The flat `sleep 1` it replaces lost
# races under 17-stand parallel-iter load — kubectl port-forward
# can take >1 s to bind to a free local port when the apiserver
# is busy, and curl then fails with `(7) Failed to connect`.
_wait_port_forward() {
    local lport=$1 pf=$2 attempt

    for attempt in $(seq 1 30); do
        if curl -fsS -m 1 "http://127.0.0.1:${lport}/v1/healthz" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
    done

    echo "rest_post/put: port-forward to :${lport} never bound" >&2
    kill "$pf" 2>/dev/null || true
    return 1
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
