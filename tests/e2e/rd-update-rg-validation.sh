#!/usr/bin/env bash
#
# usage: rd-update-rg-validation.sh WORK_DIR
#
# Bug 372 (P1, hunt-caught 2026-06-02) — PUT /v1/resource-definitions/
# {rd} with a body `{"resource_group":"nonexistent-rg-xyz"}` returned
# HTTP 200 + "resource definition modified", silently stamping the
# bogus name onto `RD.Spec.ResourceGroupName`. The RD then lived on
# with a dangling reference: `linstor rd l` rendered fine, but the
# placer's Controller→RG→RD prop-inheritance walk silently dropped
# the RG tier, breaking auto-place, auto-diskful, place_count
# observability, and rebalance scheduling.
#
# Sibling Bug 134 closed the same hole on RD create; this is the
# symmetric gap on `rd modify --resource-group`. This e2e pins the
# rejection on a live cluster and verifies the RG reference stays
# untouched after the refusal — i.e. a failing PUT never persists.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/bug372-pf.log 2>&1 &
PF_PID=$!

cleanup() {
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
    curl -s -X DELETE "http://localhost:$PF_PORT/v1/resource-definitions/bug372-rd" >/dev/null 2>&1 || true
    curl -s -X DELETE "http://localhost:$PF_PORT/v1/resource-groups/bug372-okrg" >/dev/null 2>&1 || true
    curl -s -X DELETE "http://localhost:$PF_PORT/v1/resource-groups/bug372-other" >/dev/null 2>&1 || true
}
trap cleanup EXIT

for _ in $(seq 1 30); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

B="http://localhost:$PF_PORT"

# --- Setup: two real RGs + one RD pointing at the first ---
echo ">> seed bug372-okrg + bug372-other RGs"
curl -sf -X POST "$B/v1/resource-groups" -H "Content-Type: application/json" \
    -d '{"name":"bug372-okrg","select_filter":{"place_count":1}}' >/dev/null
curl -sf -X POST "$B/v1/resource-groups" -H "Content-Type: application/json" \
    -d '{"name":"bug372-other","select_filter":{"place_count":1}}' >/dev/null

echo ">> seed bug372-rd in bug372-okrg"
curl -sf -X POST "$B/v1/resource-definitions" -H "Content-Type: application/json" \
    -d '{"resource_definition":{"name":"bug372-rd","resource_group_name":"bug372-okrg"}}' >/dev/null

# --- Bug 372: PUT RD with nonexistent RG name ---
echo ">> PUT /v1/resource-definitions/bug372-rd resource_group=ghost (Bug 372: must 404 + persist nothing)"
CODE=$(curl -s -o /tmp/bug372-resp.txt -w "%{http_code}" -X PUT \
    "$B/v1/resource-definitions/bug372-rd" \
    -H "Content-Type: application/json" \
    -d '{"resource_group":"ghost-rg-bug372"}')
if [[ "$CODE" != "404" ]]; then
    echo "FAIL: ghost RG PUT returned $CODE, expected 404"
    cat /tmp/bug372-resp.txt
    exit 1
fi
if ! grep -q "ghost-rg-bug372" /tmp/bug372-resp.txt; then
    echo "FAIL: rejection envelope missing offending name:"
    cat /tmp/bug372-resp.txt
    exit 1
fi
echo "   404 + envelope OK"

# --- Persistence: RG reference unchanged ---
RG_NAME=$(curl -sf "$B/v1/resource-definitions/bug372-rd" | python3 -c 'import sys,json; print(json.load(sys.stdin)["resource_group_name"])')
if [[ "$RG_NAME" != "bug372-okrg" ]]; then
    echo "FAIL: RD.resource_group_name changed to $RG_NAME after rejected PUT (expected bug372-okrg)"
    exit 1
fi
echo ">> persistence check: RD.resource_group_name unchanged after rejected PUT OK"

# --- Bug 232 alias: dst_rsc_grp must trigger the same gate ---
echo ">> PUT dst_rsc_grp=ghost (Bug 232 alias must also gate)"
CODE=$(curl -s -o /tmp/bug372-resp.txt -w "%{http_code}" -X PUT \
    "$B/v1/resource-definitions/bug372-rd" \
    -H "Content-Type: application/json" \
    -d '{"dst_rsc_grp":"ghost-via-dst-alias"}')
if [[ "$CODE" != "404" ]]; then
    echo "FAIL: dst_rsc_grp alias PUT returned $CODE, expected 404"
    cat /tmp/bug372-resp.txt
    exit 1
fi
echo "   404 + envelope OK"

# --- Positive: valid RG move must work ---
echo ">> PUT /v1/resource-definitions/bug372-rd resource_group=bug372-other (must 200)"
CODE=$(curl -s -o /tmp/bug372-resp.txt -w "%{http_code}" -X PUT \
    "$B/v1/resource-definitions/bug372-rd" \
    -H "Content-Type: application/json" \
    -d '{"resource_group":"bug372-other"}')
if [[ "$CODE" != "200" ]]; then
    echo "FAIL: valid RG move PUT returned $CODE, expected 200"
    cat /tmp/bug372-resp.txt
    exit 1
fi
RG_NAME=$(curl -sf "$B/v1/resource-definitions/bug372-rd" | python3 -c 'import sys,json; print(json.load(sys.stdin)["resource_group_name"])')
if [[ "$RG_NAME" != "bug372-other" ]]; then
    echo "FAIL: RD.resource_group_name did not move to bug372-other (got $RG_NAME)"
    exit 1
fi
echo "   200 + RG moved OK"

# --- Negative: props-only PUT must not be gated by the RG check ---
echo ">> PUT override_props (props-only, no RG key): must 200, RG untouched"
CODE=$(curl -s -o /tmp/bug372-resp.txt -w "%{http_code}" -X PUT \
    "$B/v1/resource-definitions/bug372-rd" \
    -H "Content-Type: application/json" \
    -d '{"override_props":{"Aux/bug372":"smoke"}}')
if [[ "$CODE" != "200" ]]; then
    echo "FAIL: props-only PUT returned $CODE, expected 200"
    cat /tmp/bug372-resp.txt
    exit 1
fi
RG_NAME=$(curl -sf "$B/v1/resource-definitions/bug372-rd" | python3 -c 'import sys,json; print(json.load(sys.stdin)["resource_group_name"])')
if [[ "$RG_NAME" != "bug372-other" ]]; then
    echo "FAIL: props-only PUT side-effected RG (got $RG_NAME, expected bug372-other)"
    exit 1
fi
echo "   200 + RG unchanged OK"

echo ">> PASS: Bug 372 — PUT /v1/resource-definitions/{rd} rejects unknown RG at REST wire"
