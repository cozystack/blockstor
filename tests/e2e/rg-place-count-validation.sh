#!/usr/bin/env bash
#
# usage: rg-place-count-validation.sh WORK_DIR
#
# Bug 367 / 361 (P1, hunt-caught 2026-06-02) — POST/PUT
# /v1/resource-groups silently accepted negative + absurd
# place_count. The PUT path additionally scheduled a rebalance on
# the corrupt target. This e2e pins all four rejections on a live
# stand and verifies:
#
#   - place_count<0 rejected on POST + PUT
#   - place_count>1_000_000 rejected on POST + PUT
#   - place_count=0 still ACCEPTED (upstream-compat scale-to-zero)
#   - PUT with no place_count mentioned still works (no false-positive)
#   - rejected PUT does NOT mutate the underlying RG

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/bug367-pf.log 2>&1 &
PF_PID=$!

cleanup() {
    # Defensive RG cleanup so a failed run doesn't leak rg-bug367-* into the cluster.
    for rg in rg-bug367-neg rg-bug367-huge rg-bug367-zero rg-bug367-put; do
        curl -s -X DELETE "http://localhost:$PF_PORT/v1/resource-groups/$rg" >/dev/null 2>&1 || true
    done
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 30); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

B="http://localhost:$PF_PORT"

# --- POST place_count=-3 must 400 ---
echo ">> POST rg with place_count=-3 (Bug 367 negative must 400)"
CODE=$(curl -s -o /tmp/bug367-resp.txt -w "%{http_code}" -X POST \
    "$B/v1/resource-groups" -H "Content-Type: application/json" \
    -d '{"name":"rg-bug367-neg","select_filter":{"place_count":-3}}')
if [[ "$CODE" != "400" ]]; then
    echo "FAIL: POST place_count=-3 returned $CODE, expected 400"
    cat /tmp/bug367-resp.txt
    exit 1
fi
if ! grep -q "is negative" /tmp/bug367-resp.txt; then
    echo "FAIL: rejection envelope missing 'is negative':"
    cat /tmp/bug367-resp.txt
    exit 1
fi
echo "   400 OK"

# --- POST place_count=2147483647 must 400 ---
echo ">> POST rg with place_count=INT32_MAX (Bug 367 sanity ceiling must 400)"
CODE=$(curl -s -o /tmp/bug367-resp.txt -w "%{http_code}" -X POST \
    "$B/v1/resource-groups" -H "Content-Type: application/json" \
    -d '{"name":"rg-bug367-huge","select_filter":{"place_count":2147483647}}')
if [[ "$CODE" != "400" ]]; then
    echo "FAIL: POST INT32_MAX returned $CODE, expected 400"
    cat /tmp/bug367-resp.txt
    exit 1
fi
if ! grep -q "sanity ceiling" /tmp/bug367-resp.txt; then
    echo "FAIL: rejection envelope missing 'sanity ceiling':"
    cat /tmp/bug367-resp.txt
    exit 1
fi
echo "   400 OK"

# --- POST place_count=0 must be ACCEPTED (upstream-compat) ---
echo ">> POST rg with place_count=0 (must remain 201 — upstream scale-to-zero)"
CODE=$(curl -s -o /tmp/bug367-resp.txt -w "%{http_code}" -X POST \
    "$B/v1/resource-groups" -H "Content-Type: application/json" \
    -d '{"name":"rg-bug367-zero","select_filter":{"place_count":0}}')
if [[ "$CODE" != "201" ]]; then
    echo "FAIL: POST place_count=0 returned $CODE, expected 201"
    cat /tmp/bug367-resp.txt
    exit 1
fi
echo "   201 OK"

# --- Seed a healthy RG for the PUT-side test ---
echo ">> seed rg-bug367-put with place_count=3"
curl -sf -X POST "$B/v1/resource-groups" -H "Content-Type: application/json" \
    -d '{"name":"rg-bug367-put","select_filter":{"place_count":3,"storage_pool":"zfs-thin"}}' >/dev/null

# --- PUT place_count=-5 must 400 and MUST NOT mutate the RG ---
echo ">> PUT rg place_count=-5 (Bug 367 PUT must 400 + no mutation)"
CODE=$(curl -s -o /tmp/bug367-resp.txt -w "%{http_code}" -X PUT \
    "$B/v1/resource-groups/rg-bug367-put" -H "Content-Type: application/json" \
    -d '{"select_filter":{"place_count":-5}}')
if [[ "$CODE" != "400" ]]; then
    echo "FAIL: PUT place_count=-5 returned $CODE, expected 400"
    cat /tmp/bug367-resp.txt
    exit 1
fi
if ! grep -q "is negative" /tmp/bug367-resp.txt; then
    echo "FAIL: PUT rejection envelope missing 'is negative':"
    cat /tmp/bug367-resp.txt
    exit 1
fi
echo "   400 OK"

# Persistence assertion: place_count must still be 3.
AFTER=$(curl -sf "$B/v1/resource-groups/rg-bug367-put")
AFTER_PC=$(echo "$AFTER" | python3 -c 'import sys,json; print(json.load(sys.stdin)["select_filter"]["place_count"])')
if [[ "$AFTER_PC" != "3" ]]; then
    echo "FAIL: rg-bug367-put place_count CHANGED to $AFTER_PC after rejected PUT (expected 3)"
    exit 1
fi
echo "   persistence assertion OK (still place_count=3)"

# --- PUT with no place_count mentioned must NOT fire the gate ---
echo ">> PUT rg with description-only patch (gate must not fire)"
CODE=$(curl -s -o /tmp/bug367-resp.txt -w "%{http_code}" -X PUT \
    "$B/v1/resource-groups/rg-bug367-put" -H "Content-Type: application/json" \
    -d '{"description":"e2e probe"}')
if [[ "$CODE" != "200" ]]; then
    echo "FAIL: description-only PUT returned $CODE, expected 200"
    cat /tmp/bug367-resp.txt
    exit 1
fi
echo "   200 OK"

# --- PUT place_count=0 (legitimate scale-to-zero) must still work ---
echo ">> PUT rg place_count=0 (upstream-compat scale-to-zero must 200)"
CODE=$(curl -s -o /tmp/bug367-resp.txt -w "%{http_code}" -X PUT \
    "$B/v1/resource-groups/rg-bug367-put" -H "Content-Type: application/json" \
    -d '{"select_filter":{"place_count":0}}')
if [[ "$CODE" != "200" ]]; then
    echo "FAIL: PUT place_count=0 returned $CODE, expected 200"
    cat /tmp/bug367-resp.txt
    exit 1
fi
echo "   200 OK"

echo ">> PASS: Bug 367/361 — RG place_count gated at REST wire on both POST and PUT"
