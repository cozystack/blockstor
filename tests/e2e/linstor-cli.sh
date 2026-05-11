#!/usr/bin/env bash
#
# usage: linstor-cli.sh WORK_DIR
#
# Validates that blockstor's REST API answers golinstor's CLI
# (`linstor`) correctly — both list-style views (node / storage-pool /
# resource-definition / resource) and side-effecting commands
# (resource-definition create / resource create / resource delete).
# The test does not run a real DRBD bring-up on the CLI side;
# blockstor's own reconciler picks up the CRDs the CLI created and
# drives the satellites, which validates the REST→CRD plumbing in
# both directions.
#
# Requires: `linstor` CLI binary in PATH on the stand host (Talos
# nodes have it via siderolabs/drbd extension; the stand host has
# it from `apt install linstor-client`).
#
# Phase 11 entry: pinned upstream-LINSTOR REST compatibility surface.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

if ! command -v linstor >/dev/null 2>&1; then
    echo "SKIP: linstor CLI not in PATH (apt install linstor-client)"
    exit 0
fi

RD=e2e-linstor-cli

# port-forward blockstor-controller:3370 so the host-side `linstor`
# CLI can reach the REST endpoint without poking a NodePort. Bind to
# a free random port — parallel `iter` runs on the same host would
# otherwise race on a fixed local port.
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n blockstor-system port-forward svc/blockstor-controller "$PF_PORT":3370 \
    >/tmp/linstor-cli-pf.log 2>&1 &
PF_PID=$!

cleanup() {
    kubectl delete resource --all --force --grace-period=0 --ignore-not-found 2>/dev/null || true
    kubectl delete resourcedefinition --all --ignore-not-found 2>/dev/null || true
    kill "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Give the port-forward a beat to bind.
for _ in $(seq 1 10); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:$PF_PORT" --machine-readable)

echo ">> linstor node list — every worker the controller knows about must appear"
nodes_json=$("${LCTL[@]}" node list)
for w in "$WORKER_1" "$WORKER_2"; do
    if ! echo "$nodes_json" | grep -q "\"$w\""; then
        echo "FAIL: node $w missing from linstor node list"
        echo "$nodes_json"
        exit 1
    fi
done

echo ">> linstor resource-definition create $RD"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null

echo ">> linstor volume-definition create $RD 32M"
"${LCTL[@]}" volume-definition create "$RD" 32M >/dev/null

echo ">> linstor resource create $WORKER_1 $RD --storage-pool stand"
"${LCTL[@]}" resource create "$WORKER_1" "$RD" --storage-pool stand >/dev/null

echo ">> linstor resource create $WORKER_2 $RD --storage-pool stand"
"${LCTL[@]}" resource create "$WORKER_2" "$RD" --storage-pool stand >/dev/null

echo ">> linstor resource-definition list — $RD must show up"
rd_json=$("${LCTL[@]}" resource-definition list)
if ! echo "$rd_json" | grep -q "\"$RD\""; then
    echo "FAIL: $RD missing from resource-definition list"
    echo "$rd_json"
    exit 1
fi

echo ">> linstor resource list — both replicas must show up"
res_json=$("${LCTL[@]}" resource list)
for w in "$WORKER_1" "$WORKER_2"; do
    if ! echo "$res_json" | grep -q "\"$w\""; then
        echo "FAIL: replica on $w missing from linstor resource list"
        echo "$res_json"
        exit 1
    fi
done

wait_uptodate "$RD" "$WORKER_1" "$WORKER_2"

echo ">> linstor resource delete $WORKER_2 $RD"
"${LCTL[@]}" resource delete "$WORKER_2" "$RD" >/dev/null

echo ">> wait up to 120s for blockstor to delete the Resource CRD"
# 120s rather than 60s because under heavy parallel-iter load the
# satellite's finalizer-strip path queues alongside every other
# reconcile and may not fire within 60s.
deadline=$(( $(date +%s) + 120 ))
while (( $(date +%s) < deadline )); do
    if ! kubectl get resource "$RD.$WORKER_2" >/dev/null 2>&1; then
        break
    fi
    sleep 2
done

if kubectl get resource "$RD.$WORKER_2" >/dev/null 2>&1; then
    echo "FAIL: Resource $RD.$WORKER_2 still present after CLI delete"
    exit 1
fi

echo ">> LINSTOR-CLI OK (node list, RD create, resource create, list, delete all round-trip)"
