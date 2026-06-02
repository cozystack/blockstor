#!/usr/bin/env bash
#
# usage: vd-path-non-numeric-envelope.sh WORK_DIR
#
# Bug 380 (P3, hunt-caught 2026-06-02) — per-volume-definition URL
# {vn} pathvar leaked a raw `strconv.ParseInt: parsing "abc": invalid
# syntax` envelope when the caller passed a non-numeric segment. The
# stdlib func name in the wire body is internal Go plumbing — gives
# the operator zero hint about the allowed [0, 65535] range, and the
# python-linstor CLI's XML decoder fallback can crash on the
# unexpected punctuation.
#
# This e2e pins the rejection on a live cluster and verifies:
#   1. GET on {vn}=abc returns 400 + envelope that names
#      `volume_number`, the `[0, 65535]` range, AND the offending
#      segment.
#   2. DELETE on {vn}=abc returns the same shape.
#   3. The envelope does NOT leak the raw `strconv.ParseInt:` prefix.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/bug380-pf.log 2>&1 &
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

assert_volnum_envelope() {
    local body_file="$1"
    local label="$2"
    python3 -c "
import sys, json
with open('$body_file') as f:
    body = f.read()
try:
    arr = json.loads(body)
except Exception as e:
    print('FAIL[$label]: body not JSON:', e, '— body=', body)
    sys.exit(1)
if not isinstance(arr, list) or len(arr) != 1:
    print('FAIL[$label]: envelope shape:', arr)
    sys.exit(1)
msg = arr[0].get('message', '')
if 'strconv.ParseInt' in msg:
    print('FAIL[$label]: leaks raw stdlib prefix:', msg)
    sys.exit(1)
if 'volume_number' not in msg:
    print('FAIL[$label]: does not name volume_number field:', msg)
    sys.exit(1)
if '[0, 65535]' not in msg:
    print('FAIL[$label]: does not surface canonical range:', msg)
    sys.exit(1)
print('OK[$label]')
"
}

echo ">> case 1: GET vd with non-numeric vn"
BODY1=/tmp/bug380-get.txt
CODE=$(curl -s -o "$BODY1" -w "%{http_code}" \
    "$B/v1/resource-definitions/some-rd/volume-definitions/abc")
if [[ "$CODE" != "400" ]]; then
    echo "FAIL: GET returned $CODE, expected 400"
    cat "$BODY1"
    exit 1
fi
assert_volnum_envelope "$BODY1" "get"

echo ">> case 2: DELETE vd with non-numeric vn"
BODY2=/tmp/bug380-del.txt
CODE=$(curl -s -o "$BODY2" -w "%{http_code}" -X DELETE \
    "$B/v1/resource-definitions/some-rd/volume-definitions/abc")
if [[ "$CODE" != "400" ]]; then
    echo "FAIL: DELETE returned $CODE, expected 400"
    cat "$BODY2"
    exit 1
fi
assert_volnum_envelope "$BODY2" "delete"

echo "PASS: bug 380 — VD {vn} pathvar emits operator-grade envelope on non-numeric input"
