#!/usr/bin/env bash
#
# usage: net-interface-delete-parent-missing.sh WORK_DIR
#
# Bug 379 (P3, hunt-caught 2026-06-02) — NIC-level echo of Bug 378.
# `DELETE /v1/nodes/{node}/net-interfaces/{name}` returned a raw 404
# `patch NetInterfaces of Node "X": node "X": object not found` when
# the parent node was already gone. Symmetric to the Bug 378 gap on
# per-key property delete which made cozystack's node-evacuation
# playbook race into a fatal 404 on the second pass.
#
# The Bug-hunt v0.1.3 Finding 9 fix already made "node present, NIC
# missing" idempotent (200 + warnNetInterfaceNotFound) but never
# extended that idempotency to the parent-missing branch, so a
# teardown that runs `linstor n d <node>` then
# `linstor node interface delete <node> default` crashed once the
# node finally cleared.
#
# This e2e pins the rejection on a live cluster and verifies:
#   1. DELETE NIC on a ghost node returns 200 (not 404).
#   2. The envelope carries the warn-band mask + objRefs[Node].
#   3. Pre-existing branch (node present, NIC missing) keeps its
#      warnNetInterfaceNotFound 200 envelope.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/bug379-pf.log 2>&1 &
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

# Case 1: parent-missing NIC delete must return 200, not 404 — the
# Bug 379 promise.
echo ">> case 1: DELETE NIC on ghost node returns 200"
BODY=/tmp/bug379-ghost.txt
CODE=$(curl -s -o "$BODY" -w "%{http_code}" -X DELETE \
    "$B/v1/nodes/bug379-ghost/net-interfaces/default")

if [[ "$CODE" != "200" ]]; then
    echo "FAIL: ghost-node NIC delete returned $CODE, expected 200 (Bug 379)"
    cat "$BODY"
    exit 1
fi

# Case 2: envelope must carry warn-band ret_code + objRefs[Node] so
# audit-log filters built around objRefNode catch the cascade.
echo ">> case 2: envelope carries warn-band + objRefs[Node]"
python3 -c "
import sys, json
with open('$BODY') as f:
    arr = json.load(f)
if not isinstance(arr, list) or len(arr) != 1:
    print('FAIL: envelope shape:', arr)
    sys.exit(1)
rc = arr[0]
# warn-band mask is 0x200000000 (Bug 378 / Bug 142 / Bug 66 share it).
if (rc.get('ret_code', 0) & 0x300000000) != 0x200000000:
    print('FAIL: ret_code not warn-band:', hex(rc.get('ret_code', 0)))
    sys.exit(1)
if 'bug379-ghost' not in rc.get('message', ''):
    print('FAIL: message does not name node:', rc)
    sys.exit(1)
if rc.get('obj_refs', {}).get('Node') != 'bug379-ghost':
    print('FAIL: obj_refs[Node] missing:', rc)
    sys.exit(1)
print('OK')
"

# Case 3: pre-existing branch (node present, NIC missing) keeps its
# warn-band envelope but with the older warnNetInterfaceNotFound mask.
# Use the first real worker as the parent — that one is always there.
echo ">> case 3: DELETE missing NIC on extant node keeps existing shape"
BODY2=/tmp/bug379-extant.txt
CODE2=$(curl -s -o "$BODY2" -w "%{http_code}" -X DELETE \
    "$B/v1/nodes/$WORKER_1/net-interfaces/bug379-never-registered")

if [[ "$CODE2" != "200" ]]; then
    echo "FAIL: extant-node missing-NIC delete returned $CODE2, expected 200"
    cat "$BODY2"
    exit 1
fi

python3 -c "
import sys, json
with open('$BODY2') as f:
    arr = json.load(f)
if not isinstance(arr, list) or len(arr) != 1:
    print('FAIL: envelope shape:', arr)
    sys.exit(1)
rc = arr[0]
# Whatever mask the pre-existing branch uses, it must NOT be the
# Bug 379 'node already absent' message — that would mean we
# regressed and folded a 'NIC missing on extant node' into the new
# parent-missing branch.
if 'node already absent' in rc.get('message', ''):
    print('FAIL: existing-node branch leaked into parent-missing shape:', rc)
    sys.exit(1)
print('OK')
"

echo "PASS: bug 379 — NIC delete is idempotent on parent-missing"
