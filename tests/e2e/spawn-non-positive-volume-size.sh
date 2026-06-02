#!/usr/bin/env bash
#
# usage: spawn-non-positive-volume-size.sh WORK_DIR
#
# Bug 381 (P3, hunt-caught 2026-06-02) — spawn fast path
# `POST /v1/resource-groups/{rg}/spawn` silently accepted
# non-positive `volume_sizes` entries. `volume_sizes` is bytes
# (Bug-92 wire shape); each entry is divided by 1024 to land as
# size_kib on the VD, so a `-100` truncated to `size_kib=0` and a
# `0` stayed `0`. Both spawned the RD with a zero-sized VD — the
# satellite reconciler then looped on `drbdadm create-md`
# indefinitely (DRBD's per-device minimum is ~4 MiB once metadata
# is reserved).
#
# This e2e pins the rejection on a live cluster and verifies:
#   1. spawn with `volume_sizes: [-100]` returns 400 (not 201).
#   2. spawn with `volume_sizes: [0]` returns 400.
#   3. RD is NOT created (no orphan-RD leak).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/bug381-pf.log 2>&1 &
PF_PID=$!

cleanup() {
    # best-effort clean
    curl -sf -X DELETE "http://localhost:$PF_PORT/v1/resource-definitions/bug381-neg-rd" >/dev/null 2>&1 || true
    curl -sf -X DELETE "http://localhost:$PF_PORT/v1/resource-definitions/bug381-zero-rd" >/dev/null 2>&1 || true
    curl -sf -X DELETE "http://localhost:$PF_PORT/v1/resource-groups/bug381-rg" >/dev/null 2>&1 || true
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

# Seed an RG with a real pool so the spawn would have succeeded
# pre-fix (we want to prove the rejection comes from the size gate,
# not from RG-resolution failure or autoplace shortfall).
echo ">> seed RG bug381-rg"
CODE=$(curl -s -o /tmp/bug381-seed.txt -w "%{http_code}" -X POST \
    "$B/v1/resource-groups" -H "Content-Type: application/json" \
    -d '{"name":"bug381-rg","select_filter":{"place_count":1,"storage_pool":"lvm-thin"}}')
if [[ "$CODE" != "201" ]]; then
    echo "FAIL: seed RG returned $CODE"
    cat /tmp/bug381-seed.txt
    exit 1
fi

# Case 1: negative size must be rejected.
echo ">> case 1: spawn with volume_sizes=[-100] returns 400"
BODY1=/tmp/bug381-neg.txt
CODE=$(curl -s -o "$BODY1" -w "%{http_code}" -X POST \
    "$B/v1/resource-groups/bug381-rg/spawn" -H "Content-Type: application/json" \
    -d '{"resource_definition_name":"bug381-neg-rd","volume_sizes":[-100]}')
if [[ "$CODE" != "400" ]]; then
    echo "FAIL: negative size returned $CODE, expected 400"
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
if 'below minimum' not in msg:
    print('FAIL: message does not mention below-minimum:', msg)
    sys.exit(1)
if 'bug381-neg-rd' not in msg:
    print('FAIL: message does not name RD:', msg)
    sys.exit(1)
print('OK')
"

# Case 1b: no orphan RD must be left behind.
echo ">> case 1b: no orphan RD created"
CODE2=$(curl -s -o /dev/null -w "%{http_code}" \
    "$B/v1/resource-definitions/bug381-neg-rd")
if [[ "$CODE2" != "404" ]]; then
    echo "FAIL: orphan RD bug381-neg-rd exists (status $CODE2)"
    exit 1
fi

# Case 2: zero size must be rejected.
echo ">> case 2: spawn with volume_sizes=[0] returns 400"
BODY2=/tmp/bug381-zero.txt
CODE=$(curl -s -o "$BODY2" -w "%{http_code}" -X POST \
    "$B/v1/resource-groups/bug381-rg/spawn" -H "Content-Type: application/json" \
    -d '{"resource_definition_name":"bug381-zero-rd","volume_sizes":[0]}')
if [[ "$CODE" != "400" ]]; then
    echo "FAIL: zero size returned $CODE, expected 400"
    cat "$BODY2"
    exit 1
fi

CODE3=$(curl -s -o /dev/null -w "%{http_code}" \
    "$B/v1/resource-definitions/bug381-zero-rd")
if [[ "$CODE3" != "404" ]]; then
    echo "FAIL: orphan RD bug381-zero-rd exists (status $CODE3)"
    exit 1
fi

echo "PASS: bug 381 — spawn rejects non-positive volume_sizes without orphan-RD leak"
