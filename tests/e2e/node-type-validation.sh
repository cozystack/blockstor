#!/usr/bin/env bash
#
# usage: node-type-validation.sh WORK_DIR
#
# Bug 370 (P1, hunt-caught 2026-06-02) — POST /v1/nodes with an
# unknown `type` value (e.g. `"type":"INVALID"`) leaked HTTP 500 +
# the raw k8s CEL rejection ("spec.type: Unsupported value: ..."),
# which python-linstor and golinstor cannot classify (the envelope
# carried bare apiCallRcError with no FAIL_INVLD_NODE_TYPE sub-code).
#
# Operators saw an opaque "internal server error" envelope on a
# trivial typo where upstream LINSTOR returns a structured 400 +
# FAIL_INVLD_NODE_TYPE (430).
#
# This e2e pins the rejection on a live cluster (not just the unit
# test) and verifies the apiserver never persists a Node CRD with
# the bad enum.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/bug370-pf.log 2>&1 &
PF_PID=$!

cleanup() {
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
    # Defensive: drop any phantom node that slipped through.
    curl -s -X DELETE "http://localhost:$PF_PORT/v1/nodes/bug370-bad-type" >/dev/null 2>&1 || true
}
trap cleanup EXIT

for _ in $(seq 1 30); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

B="http://localhost:$PF_PORT"

# --- Bug 370: POST node with type=INVALID ---
echo ">> POST /v1/nodes with type=INVALID (Bug 370: must 400 + structured envelope, not 500)"
CODE=$(curl -s -o /tmp/bug370-resp.txt -w "%{http_code}" -X POST \
    "$B/v1/nodes" -H "Content-Type: application/json" \
    -d '{"name":"bug370-bad-type","type":"INVALID","net_interfaces":[{"name":"default","address":"10.99.99.250"}]}')

if [[ "$CODE" != "400" ]]; then
    echo "FAIL: Bug 370 POST returned $CODE, expected 400"
    cat /tmp/bug370-resp.txt
    exit 1
fi

if ! grep -q "INVALID" /tmp/bug370-resp.txt; then
    echo "FAIL: rejection envelope missing offending value:"
    cat /tmp/bug370-resp.txt
    exit 1
fi

if ! grep -qE "SATELLITE|CONTROLLER|supported" /tmp/bug370-resp.txt; then
    echo "FAIL: rejection envelope missing accepted-value enumeration:"
    cat /tmp/bug370-resp.txt
    exit 1
fi

echo "   400 + structured envelope OK"

# --- Persistence check: no phantom node CRD ---
LANDED=$(curl -sf "$B/v1/nodes" | python3 -c '
import sys, json
for n in json.load(sys.stdin):
    if n["name"] == "bug370-bad-type":
        print("yes")
        sys.exit(0)
print("no")
')
if [[ "$LANDED" == "yes" ]]; then
    echo "FAIL: phantom Node bug370-bad-type persisted after rejected POST"
    exit 1
fi
echo ">> no phantom Node CRD persisted after rejected POST OK"

# --- Negative: empty type still flows through (canonical CLI shape) ---
echo ">> POST /v1/nodes with no type (canonical CLI default): must 201"
CODE=$(curl -s -o /tmp/bug370-resp.txt -w "%{http_code}" -X POST \
    "$B/v1/nodes" -H "Content-Type: application/json" \
    -d '{"name":"bug370-default-type","net_interfaces":[{"name":"default","address":"10.99.99.251"}]}')
if [[ "$CODE" != "201" ]]; then
    echo "FAIL: missing type should default through, got $CODE"
    cat /tmp/bug370-resp.txt
    exit 1
fi
curl -s -X DELETE "$B/v1/nodes/bug370-default-type" >/dev/null || true
echo "   201 OK"

# --- Negative: canonical SATELLITE works ---
echo ">> POST /v1/nodes with type=SATELLITE (canonical enum): must 201"
CODE=$(curl -s -o /tmp/bug370-resp.txt -w "%{http_code}" -X POST \
    "$B/v1/nodes" -H "Content-Type: application/json" \
    -d '{"name":"bug370-satellite","type":"SATELLITE","net_interfaces":[{"name":"default","address":"10.99.99.252"}]}')
if [[ "$CODE" != "201" ]]; then
    echo "FAIL: SATELLITE type rejected, got $CODE"
    cat /tmp/bug370-resp.txt
    exit 1
fi
curl -s -X DELETE "$B/v1/nodes/bug370-satellite" >/dev/null || true
echo "   201 OK"

echo ">> PASS: Bug 370 — POST /v1/nodes rejects unknown type at REST wire"
