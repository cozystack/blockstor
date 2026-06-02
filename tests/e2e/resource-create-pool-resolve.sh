#!/usr/bin/env bash
#
# usage: resource-create-pool-resolve.sh WORK_DIR
#
# Bug 364 (P1, hunt-caught 2026-06-02) — `linstor r c <node> <rd>`
# without `--storage-pool` against an RG that pins its default via
# `select_filter.storage_pool_list` (not `select_filter.storage_pool`)
# created a Resource with empty `Props["StorPoolName"]`. The satellite
# reconciler then had no pool to bind to and the replica wedged at
# "Provisioning" — visible to the operator only as a phantom replica
# that never reached UpToDate.
#
# linstor-csi is the canonical caller for this path: it posts no body
# to the per-node resource-create endpoint and relies on RG-side
# propagation for the pool name. When the StorageClass sets
# `linstor.csi.linbit.com/storagePool: <p>`, linstor-csi's RGCreate
# path lands the value under SelectFilter.StoragePoolList[0] (not
# .StoragePool), so every Cozystack volume hits this path.
#
# This e2e pins the fix on a live cluster: an RG with only
# `storage_pool_list` must drive the resource-create's fresh-create
# pool resolution to stamp Props["StorPoolName"] from the list's
# first entry, and the satellite must actually reach UpToDate.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/bug364-pf.log 2>&1 &
PF_PID=$!

cleanup() {
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
    curl -s -X DELETE "http://localhost:$PF_PORT/v1/resource-definitions/bug364-rd" >/dev/null 2>&1 || true
    curl -s -X DELETE "http://localhost:$PF_PORT/v1/resource-groups/bug364-rg" >/dev/null 2>&1 || true
}
trap cleanup EXIT

for _ in $(seq 1 30); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

B="http://localhost:$PF_PORT"

# --- Setup: RG with only storage_pool_list ---
echo ">> seed bug364-rg with storage_pool_list (no storage_pool)"
curl -sf -X POST "$B/v1/resource-groups" -H "Content-Type: application/json" \
    -d '{"name":"bug364-rg","select_filter":{"place_count":1,"storage_pool_list":["lvm-thin"]}}' >/dev/null

echo ">> seed bug364-rd in bug364-rg"
curl -sf -X POST "$B/v1/resource-definitions" -H "Content-Type: application/json" \
    -d '{"resource_definition":{"name":"bug364-rd","resource_group_name":"bug364-rg"}}' >/dev/null

curl -sf -X POST "$B/v1/resource-definitions/bug364-rd/volume-definitions" \
    -H "Content-Type: application/json" \
    -d '{"volume_definition":{"size_kib":65536}}' >/dev/null

# --- Bug 364: r c <node> bug364-rd without --storage-pool ---
echo ">> POST resource-create on $WORKER_1 with empty body (linstor-csi shape)"
CODE=$(curl -s -o /tmp/bug364-resp.txt -w "%{http_code}" -X POST \
    "$B/v1/resource-definitions/bug364-rd/resources/$WORKER_1" \
    -H "Content-Type: application/json" \
    -d '')
if [[ "$CODE" != "201" ]]; then
    echo "FAIL: r c returned $CODE, expected 201"
    cat /tmp/bug364-resp.txt
    exit 1
fi
echo "   201 OK"

# --- Persistence: Props["StorPoolName"] must be stamped ---
POOL=$(curl -sf "$B/v1/resource-definitions/bug364-rd/resources" | python3 -c '
import sys, json
for r in json.load(sys.stdin):
    props = r.get("props") or {}
    print(props.get("StorPoolName", ""))
    break
')
if [[ "$POOL" != "lvm-thin" ]]; then
    echo "FAIL: Props[StorPoolName] = $POOL, expected lvm-thin (Bug 364)"
    curl -sf "$B/v1/resource-definitions/bug364-rd/resources" | python3 -m json.tool
    exit 1
fi
echo ">> Props[StorPoolName] = lvm-thin (Bug 364 fix OK)"

# --- Live convergence: the replica must reach UpToDate ---
echo ">> wait for replica UpToDate on $WORKER_1"
for _ in $(seq 1 30); do
    STATE=$(curl -sf "$B/v1/resource-definitions/bug364-rd/resources" | python3 -c '
import sys, json
for r in json.load(sys.stdin):
    state = r.get("state") or {}
    print(state.get("drbd_state", ""))
    break
')
    if [[ "$STATE" == "UpToDate" ]]; then
        echo "   replica UpToDate after wait"
        break
    fi
    sleep 2
done

if [[ "$STATE" != "UpToDate" ]]; then
    echo "FAIL: replica never reached UpToDate (last state: $STATE)"
    exit 1
fi

echo ">> PASS: Bug 364 — r c without --storage-pool resolves through RG StoragePoolList[0]"
