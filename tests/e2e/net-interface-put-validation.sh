#!/usr/bin/env bash
#
# usage: net-interface-put-validation.sh WORK_DIR
#
# Bug 371 (P0, hunt-caught 2026-06-02) — `PUT /v1/nodes/<n>/net-interfaces/default`
# with bad address / port was un-validated and PERSISTED into the
# live Node spec of an Online satellite, deforming the actual
# controller→satellite handshake of a production node via an
# unauthenticated REST call.
#
# Bug 368/369 — POST /v1/nodes with satellite_port=-1 / 99999 also
# silently produced an offline Node CRD with a permanently-broken
# port. Symmetric gap on the create path.
#
# This e2e pins all four rejections on a live cluster (not just
# the unit-test paths) AND verifies the pre-existing Online
# satellites stay healthy — i.e. a failing PUT never persists,
# never disturbs the live link.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/bug371-pf.log 2>&1 &
PF_PID=$!

cleanup() {
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

# Pick a live satellite — every worker registers as a SATELLITE so
# the first one in the list is fine.
TARGET=$(curl -sf "$B/v1/nodes" | python3 -c '
import sys, json
for n in json.load(sys.stdin):
    if n.get("type") == "SATELLITE":
        print(n["name"])
        sys.exit(0)
sys.exit(1)
')
echo ">> probing live satellite: $TARGET"

# Capture pre-state so we can verify nothing persisted on rejection.
BEFORE=$(curl -sf "$B/v1/nodes/$TARGET/net-interfaces/default")
BEFORE_ADDR=$(echo "$BEFORE" | python3 -c 'import sys,json; print(json.load(sys.stdin)["address"])')
BEFORE_PORT=$(echo "$BEFORE" | python3 -c 'import sys,json; print(json.load(sys.stdin)["satellite_port"])')
echo "   before: address=$BEFORE_ADDR port=$BEFORE_PORT"

# --- Bug 371: PUT with garbage address ---
echo ">> PUT net-interface address=garbage (Bug 371: must 400 + persist nothing)"
CODE=$(curl -s -o /tmp/bug371-resp.txt -w "%{http_code}" -X PUT \
    "$B/v1/nodes/$TARGET/net-interfaces/default" \
    -H "Content-Type: application/json" \
    -d "{\"address\":\"garbage\",\"satellite_port\":3366}")
if [[ "$CODE" != "400" ]]; then
    echo "FAIL: garbage address PUT returned $CODE, expected 400"
    cat /tmp/bug371-resp.txt
    exit 1
fi
if ! grep -qE "not a valid IPv4|garbage" /tmp/bug371-resp.txt; then
    echo "FAIL: garbage rejection envelope missing offending address:"
    cat /tmp/bug371-resp.txt
    exit 1
fi
echo "   400 + envelope OK"

# --- Bug 371: PUT with negative port ---
echo ">> PUT net-interface satellite_port=-7 (Bug 371: must 400 + persist nothing)"
CODE=$(curl -s -o /tmp/bug371-resp.txt -w "%{http_code}" -X PUT \
    "$B/v1/nodes/$TARGET/net-interfaces/default" \
    -H "Content-Type: application/json" \
    -d "{\"address\":\"$BEFORE_ADDR\",\"satellite_port\":-7}")
if [[ "$CODE" != "400" ]]; then
    echo "FAIL: negative port PUT returned $CODE, expected 400"
    cat /tmp/bug371-resp.txt
    exit 1
fi
if ! grep -qE "out of range|-7" /tmp/bug371-resp.txt; then
    echo "FAIL: negative-port rejection envelope missing offending value:"
    cat /tmp/bug371-resp.txt
    exit 1
fi
echo "   400 + envelope OK"

# --- Bug 371: PUT with huge port ---
echo ">> PUT net-interface satellite_port=99999 (Bug 371: must 400 + persist nothing)"
CODE=$(curl -s -o /tmp/bug371-resp.txt -w "%{http_code}" -X PUT \
    "$B/v1/nodes/$TARGET/net-interfaces/default" \
    -H "Content-Type: application/json" \
    -d "{\"address\":\"$BEFORE_ADDR\",\"satellite_port\":99999}")
if [[ "$CODE" != "400" ]]; then
    echo "FAIL: huge port PUT returned $CODE, expected 400"
    cat /tmp/bug371-resp.txt
    exit 1
fi
if ! grep -qE "out of range|99999" /tmp/bug371-resp.txt; then
    echo "FAIL: huge-port rejection envelope missing offending value:"
    cat /tmp/bug371-resp.txt
    exit 1
fi
echo "   400 + envelope OK"

# --- Persistence check: live satellite address+port must be untouched. ---
AFTER=$(curl -sf "$B/v1/nodes/$TARGET/net-interfaces/default")
AFTER_ADDR=$(echo "$AFTER" | python3 -c 'import sys,json; print(json.load(sys.stdin)["address"])')
AFTER_PORT=$(echo "$AFTER" | python3 -c 'import sys,json; print(json.load(sys.stdin)["satellite_port"])')
if [[ "$AFTER_ADDR" != "$BEFORE_ADDR" || "$AFTER_PORT" != "$BEFORE_PORT" ]]; then
    echo "FAIL: net-interface CHANGED after rejected PUTs"
    echo "   before: address=$BEFORE_ADDR port=$BEFORE_PORT"
    echo "   after:  address=$AFTER_ADDR  port=$AFTER_PORT"
    exit 1
fi
echo ">> persistence check: net-interface unchanged after rejected PUTs OK"

# --- Bug 368: POST node with negative satellite_port ---
echo ">> POST /v1/nodes with satellite_port=-1 (Bug 368: must 400)"
CODE=$(curl -s -o /tmp/bug371-resp.txt -w "%{http_code}" -X POST \
    "$B/v1/nodes" -H "Content-Type: application/json" \
    -d "{\"name\":\"bug368-bad-port\",\"type\":\"SATELLITE\",\"net_interfaces\":[{\"name\":\"default\",\"address\":\"10.99.99.99\",\"satellite_port\":-1}]}")
if [[ "$CODE" != "400" ]]; then
    echo "FAIL: Bug 368 POST returned $CODE, expected 400"
    cat /tmp/bug371-resp.txt
    # Defensive cleanup if a phantom landed.
    curl -s -X DELETE "$B/v1/nodes/bug368-bad-port" >/dev/null || true
    exit 1
fi
echo "   400 + envelope OK"

# --- Bug 369: POST node with port > 65535 ---
echo ">> POST /v1/nodes with satellite_port=99999 (Bug 369: must 400)"
CODE=$(curl -s -o /tmp/bug371-resp.txt -w "%{http_code}" -X POST \
    "$B/v1/nodes" -H "Content-Type: application/json" \
    -d "{\"name\":\"bug369-huge-port\",\"type\":\"SATELLITE\",\"net_interfaces\":[{\"name\":\"default\",\"address\":\"10.99.99.99\",\"satellite_port\":99999}]}")
if [[ "$CODE" != "400" ]]; then
    echo "FAIL: Bug 369 POST returned $CODE, expected 400"
    cat /tmp/bug371-resp.txt
    curl -s -X DELETE "$B/v1/nodes/bug369-huge-port" >/dev/null || true
    exit 1
fi
echo "   400 + envelope OK"

# Confirm no phantom node landed.
LANDED=$(curl -sf "$B/v1/nodes" | python3 -c '
import sys, json
names = {n["name"] for n in json.load(sys.stdin)}
for bad in ("bug368-bad-port", "bug369-huge-port"):
    if bad in names:
        print(bad)
        sys.exit(0)
sys.exit(0)
')
if [[ -n "$LANDED" ]]; then
    echo "FAIL: phantom node $LANDED persisted after rejected POST"
    curl -s -X DELETE "$B/v1/nodes/$LANDED" >/dev/null || true
    exit 1
fi
echo ">> no phantom Node CRD persisted after rejected POSTs OK"

# --- Valid PUT with a sane address+port: must succeed AND must
#     return the polished envelope (no trailing space, "modified:
#     <name>" verb). Restore the original address+port so the
#     satellite stays Online for downstream e2e scenarios.
echo ">> valid PUT (restore original): must 200 + 'net-interface modified: default'"
CODE=$(curl -s -o /tmp/bug371-resp.txt -w "%{http_code}" -X PUT \
    "$B/v1/nodes/$TARGET/net-interfaces/default" \
    -H "Content-Type: application/json" \
    -d "{\"address\":\"$BEFORE_ADDR\",\"satellite_port\":$BEFORE_PORT}")
if [[ "$CODE" != "200" ]]; then
    echo "FAIL: valid restore PUT returned $CODE, expected 200"
    cat /tmp/bug371-resp.txt
    exit 1
fi
if ! grep -q "net-interface modified: default" /tmp/bug371-resp.txt; then
    echo "FAIL: valid PUT envelope shape regressed (Bug 371 trailing-space):"
    cat /tmp/bug371-resp.txt
    exit 1
fi
echo "   200 + envelope OK"

echo ">> PASS: Bug 371/368/369 — net-interface PUT/POST rejects bad address+port at REST wire"
