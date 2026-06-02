#!/usr/bin/env bash
#
# usage: sp-immutable-driver-props.sh WORK_DIR
#
# Bug 373 (P1, hunt-caught 2026-06-02) — `PUT /v1/nodes/{node}/
# storage-pools/{pool}` accepted `override_props` (and `delete_props`
# / `delete_namespaces`) that touched the backing-driver identity
# keys (StorDriver/ZPool[Thin], LvmVg, ThinPool, FileDir,
# StorPoolName). Mutating any of them silently flipped the live
# StoragePool to a different / non-existent backend; every active
# replica still reported UpToDate (the kernel-DRBD device was open
# against the original backend), so the cluster gave NO operator-
# visible signal until the next placement / autoplace / resize call
# blew up with "pool backing storage missing on node …".
#
# This e2e pins the rejection on a live cluster (not just the unit
# test) and verifies the pool row stays byte-identical post-call.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/bug373-pf.log 2>&1 &
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
NODE="${E2E_NODE_DISKFUL_1:-dev-worker-1}"
POOL="zfs-thin"

# Capture the live pool's StorDriver/ZPoolThin so we can verify
# byte-identical state post-call.
PRE_BACKING=$(curl -sf "$B/v1/nodes/$NODE/storage-pools/$POOL" \
    | python3 -c 'import sys, json; sp=json.load(sys.stdin); print(sp.get("props",{}).get("StorDriver/ZPoolThin",""))')

if [[ -z "$PRE_BACKING" ]]; then
    echo "FAIL: pool $POOL on $NODE has no StorDriver/ZPoolThin to protect; check stand fixture"
    exit 1
fi

echo ">> pre-call StorDriver/ZPoolThin = $PRE_BACKING"

# --- override_props on a backing-driver key must 400 ---
echo ">> PUT override_props StorDriver/ZPoolThin=bogus: must 400 + structured envelope"
CODE=$(curl -s -o /tmp/bug373-resp.txt -w "%{http_code}" -X PUT \
    "$B/v1/nodes/$NODE/storage-pools/$POOL" -H "Content-Type: application/json" \
    -d '{"override_props":{"StorDriver/ZPoolThin":"bug373-bogus"}}')

if [[ "$CODE" != "400" ]]; then
    echo "FAIL: Bug 373 PUT override_props returned $CODE, expected 400"
    cat /tmp/bug373-resp.txt
    exit 1
fi

if ! grep -q "StorDriver/ZPoolThin" /tmp/bug373-resp.txt; then
    echo "FAIL: rejection envelope missing offending key:"
    cat /tmp/bug373-resp.txt
    exit 1
fi

if ! grep -q "refusing to mutate" /tmp/bug373-resp.txt; then
    echo "FAIL: rejection envelope missing operator-facing phrase:"
    cat /tmp/bug373-resp.txt
    exit 1
fi

echo "   400 + structured envelope OK"

# --- live row must stay byte-identical ---
POST_BACKING=$(curl -sf "$B/v1/nodes/$NODE/storage-pools/$POOL" \
    | python3 -c 'import sys, json; sp=json.load(sys.stdin); print(sp.get("props",{}).get("StorDriver/ZPoolThin",""))')

if [[ "$POST_BACKING" != "$PRE_BACKING" ]]; then
    echo "FAIL: backing key mutated despite rejected PUT: $PRE_BACKING -> $POST_BACKING"
    exit 1
fi
echo ">> backing key unchanged post-rejection OK"

# --- delete_props on backing-driver key must 400 ---
echo ">> PUT delete_props=[StorDriver/ZPoolThin]: must 400"
CODE=$(curl -s -o /tmp/bug373-resp.txt -w "%{http_code}" -X PUT \
    "$B/v1/nodes/$NODE/storage-pools/$POOL" -H "Content-Type: application/json" \
    -d '{"delete_props":["StorDriver/ZPoolThin"]}')

if [[ "$CODE" != "400" ]]; then
    echo "FAIL: Bug 373 PUT delete_props returned $CODE, expected 400"
    cat /tmp/bug373-resp.txt
    exit 1
fi
echo "   400 OK"

# --- delete_namespaces=[StorDriver] must 400 ---
echo ">> PUT delete_namespaces=[StorDriver]: must 400"
CODE=$(curl -s -o /tmp/bug373-resp.txt -w "%{http_code}" -X PUT \
    "$B/v1/nodes/$NODE/storage-pools/$POOL" -H "Content-Type: application/json" \
    -d '{"delete_namespaces":["StorDriver"]}')

if [[ "$CODE" != "400" ]]; then
    echo "FAIL: Bug 373 PUT delete_namespaces returned $CODE, expected 400"
    cat /tmp/bug373-resp.txt
    exit 1
fi
echo "   400 OK"

# --- benign override_props (PrefNic) must STILL land (regression guard) ---
echo ">> PUT override_props PrefNic=default: must 200 (benign)"
CODE=$(curl -s -o /tmp/bug373-resp.txt -w "%{http_code}" -X PUT \
    "$B/v1/nodes/$NODE/storage-pools/$POOL" -H "Content-Type: application/json" \
    -d '{"override_props":{"PrefNic":"default"}}')

if [[ "$CODE" != "200" ]]; then
    echo "FAIL: benign PUT PrefNic returned $CODE, expected 200 (regression)"
    cat /tmp/bug373-resp.txt
    exit 1
fi
echo "   200 OK"

# Drop the benign PrefNic we just stamped so the next e2e run sees a
# clean fixture; the backing-key delete is allowed because PrefNic
# isn't in the immutable list.
curl -sf -X PUT "$B/v1/nodes/$NODE/storage-pools/$POOL" \
    -H "Content-Type: application/json" \
    -d '{"delete_props":["PrefNic"]}' >/dev/null || true

# --- final: backing key still byte-identical to pre-test ---
FINAL_BACKING=$(curl -sf "$B/v1/nodes/$NODE/storage-pools/$POOL" \
    | python3 -c 'import sys, json; sp=json.load(sys.stdin); print(sp.get("props",{}).get("StorDriver/ZPoolThin",""))')

if [[ "$FINAL_BACKING" != "$PRE_BACKING" ]]; then
    echo "FAIL: backing key mutated across the test sequence: $PRE_BACKING -> $FINAL_BACKING"
    exit 1
fi

echo ">> PASS: Bug 373 — PUT storage-pools refuses StorDriver/* mutation"
