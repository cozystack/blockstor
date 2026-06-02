#!/usr/bin/env bash
#
# usage: vd-put-non-positive-size.sh WORK_DIR
#
# Bug 383 (P3, hunt-caught 2026-06-02) — VD PUT update path
# `PUT /v1/resource-definitions/{rd}/volume-definitions/{vn}` silently
# accepted non-positive `size_kib` when `force=true` was set. The
# force escape hatch was scoped only at the scenario 4.W13 "no auto-
# shrink" refusal (callers that already ran `resize2fs -s` on the
# consumer know the new size is below the live one); it was never
# intended to let a caller persist `size_kib=0` or a negative value
# into the spec.
#
# Pre-fix the satellite reconciler then looped on `drbdadm create-md`
# indefinitely (DRBD's per-device minimum is ~4 MiB once metadata is
# reserved), identical to the Bug 381 spawn-fast-path footgun but
# reached through the PUT update path.
#
# This e2e pins the rejection on a live cluster and verifies:
#   1. PUT VD with `size_kib=-100, force=true` returns 400.
#   2. PUT VD with `size_kib=0, force=true` returns 400.
#   3. The stored VD row stays at the pre-PUT size after rejection.
#   4. Legitimate force-shrink (positive new size below previous)
#      still lands — the gate is not over-broad.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/bug383-pf.log 2>&1 &
PF_PID=$!

cleanup() {
    curl -sf -X DELETE "http://localhost:$PF_PORT/v1/resource-definitions/bug383-rd" >/dev/null 2>&1 || true
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

# Seed RD + VD at a sane size so each PUT has a real row to mutate.
echo ">> seed RD bug383-rd with VD 0 at 1 GiB"
CODE=$(curl -s -o /tmp/bug383-seed.txt -w "%{http_code}" -X POST \
    "$B/v1/resource-definitions" -H "Content-Type: application/json" \
    -d '{"resource_definition":{"name":"bug383-rd"}}')
if [[ "$CODE" != "201" ]]; then
    echo "FAIL: seed RD returned $CODE"
    cat /tmp/bug383-seed.txt
    exit 1
fi

CODE=$(curl -s -o /tmp/bug383-vd.txt -w "%{http_code}" -X POST \
    "$B/v1/resource-definitions/bug383-rd/volume-definitions" \
    -H "Content-Type: application/json" \
    -d '{"volume_definition":{"size_kib":1048576}}')
if [[ "$CODE" != "200" && "$CODE" != "201" ]]; then
    echo "FAIL: seed VD returned $CODE"
    cat /tmp/bug383-vd.txt
    exit 1
fi

# helper: read VD's persisted size_kib via GET.
get_vd_size() {
    curl -s "$B/v1/resource-definitions/bug383-rd/volume-definitions/0" \
        | python3 -c "import sys, json; d=json.load(sys.stdin); print(d['size_kib'] if isinstance(d, dict) else d[0]['size_kib'])"
}

INITIAL=$(get_vd_size)
if [[ "$INITIAL" != "1048576" ]]; then
    echo "FAIL: initial seed size_kib=$INITIAL, want 1048576"
    exit 1
fi

# Case 1: negative + force=true must be rejected, row untouched.
echo ">> case 1: PUT size_kib=-100 force=true returns 400"
BODY1=/tmp/bug383-neg.txt
CODE=$(curl -s -o "$BODY1" -w "%{http_code}" -X PUT \
    "$B/v1/resource-definitions/bug383-rd/volume-definitions/0" \
    -H "Content-Type: application/json" \
    -d '{"size_kib":-100,"force":true}')
if [[ "$CODE" != "400" ]]; then
    echo "FAIL: negative + force returned $CODE, expected 400"
    cat "$BODY1"
    exit 1
fi

python3 -c "
import sys, json
with open('$BODY1') as f:
    arr = json.load(f)
if not isinstance(arr, list) or len(arr) != 1:
    print('FAIL: envelope shape:', arr)
    sys.exit(1)
msg = arr[0].get('message', '')
if 'must be > 0' not in msg:
    print('FAIL: message does not mention the > 0 floor:', msg)
    sys.exit(1)
if 'filesystem shrink-then-resize' in msg:
    print('FAIL: message confuses Bug 383 with the shrink-force path:', msg)
    sys.exit(1)
print('OK')
"

AFTER1=$(get_vd_size)
if [[ "$AFTER1" != "1048576" ]]; then
    echo "FAIL: row mutated after negative-force rejection: size_kib=$AFTER1"
    exit 1
fi

# Case 2: zero + force=true must be rejected, row untouched.
echo ">> case 2: PUT size_kib=0 force=true returns 400"
BODY2=/tmp/bug383-zero.txt
CODE=$(curl -s -o "$BODY2" -w "%{http_code}" -X PUT \
    "$B/v1/resource-definitions/bug383-rd/volume-definitions/0" \
    -H "Content-Type: application/json" \
    -d '{"size_kib":0,"force":true}')
if [[ "$CODE" != "400" ]]; then
    echo "FAIL: zero + force returned $CODE, expected 400"
    cat "$BODY2"
    exit 1
fi

AFTER2=$(get_vd_size)
if [[ "$AFTER2" != "1048576" ]]; then
    echo "FAIL: row mutated after zero-force rejection: size_kib=$AFTER2"
    exit 1
fi

# Case 3: legitimate force-shrink (positive, below previous) still works.
echo ">> case 3: legitimate force-shrink to 524288 KiB still lands"
BODY3=/tmp/bug383-shrink.txt
CODE=$(curl -s -o "$BODY3" -w "%{http_code}" -X PUT \
    "$B/v1/resource-definitions/bug383-rd/volume-definitions/0" \
    -H "Content-Type: application/json" \
    -d '{"size_kib":524288,"force":true}')
if [[ "$CODE" != "200" ]]; then
    echo "FAIL: legitimate force-shrink returned $CODE, expected 200"
    cat "$BODY3"
    exit 1
fi

AFTER3=$(get_vd_size)
if [[ "$AFTER3" != "524288" ]]; then
    echo "FAIL: force-shrink did not land: size_kib=$AFTER3"
    exit 1
fi

echo "PASS: bug 383 — VD PUT rejects non-positive size_kib regardless of force, legitimate shrink still works"
