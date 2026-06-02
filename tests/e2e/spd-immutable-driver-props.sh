#!/usr/bin/env bash
#
# usage: spd-immutable-driver-props.sh WORK_DIR
#
# Bug 375 (P2, hunt-caught 2026-06-02) — SPD-level echo of Bug 373.
# `PUT /v1/storage-pool-definitions/{name}` accepted `override_props`
# (and `delete_props` / `delete_namespaces`) that touched the backing-
# driver identity keys (StorDriver/ZPool[Thin], LvmVg, ThinPool,
# FileDir, StorPoolName) on an EXISTING definition. The StoragePool
# Definition is the catalog row every future per-node StoragePool
# inherits its default backing-driver identity from; silently flipping
# the SPD default desyncs the catalog from any per-node SP that hasn't
# materialised yet, breaking placement / autoplace consistency the
# moment the next `linstor sp c <node> <name>` lands.
#
# This e2e pins the rejection on a live cluster and verifies:
#   1. PUT override_props with StorDriver/* on an EXISTING SPD => 400
#   2. PUT delete_props on StorDriver/* => 400
#   3. PUT delete_namespaces=[StorDriver] => 400
#   4. Catalog row stays byte-identical post-call
#   5. PUT-create with StorDriver/* on a brand-new SPD name => 200
#      (seed path operators rely on for one-round-trip provisioning)
#   6. Benign PUT (Aux/*) on existing SPD still lands (regression guard)

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/bug375-pf.log 2>&1 &
PF_PID=$!

cleanup() {
    # best-effort drop of any SPD this test stamped — keeps the stand
    # clean for the next scenario regardless of where we exited.
    curl -sf -X DELETE "http://localhost:$PF_PORT/v1/storage-pool-definitions/bug375-existing" >/dev/null 2>&1 || true
    curl -sf -X DELETE "http://localhost:$PF_PORT/v1/storage-pool-definitions/bug375-create" >/dev/null 2>&1 || true
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

# Seed an SPD we know is ours so we can mutate / refuse / verify
# without depending on stand-fixture pools. The POST path is the
# upstream-canonical CREATE for SPDs.
echo ">> seed SPD bug375-existing"
CODE=$(curl -s -o /tmp/bug375-seed.txt -w "%{http_code}" -X POST \
    "$B/v1/storage-pool-definitions" -H "Content-Type: application/json" \
    -d '{"storage_pool_name":"bug375-existing","props":{"StorDriver/LvmVg":"vg-original"}}')

if [[ "$CODE" != "201" ]]; then
    echo "FAIL: seed SPD POST returned $CODE, expected 201"
    cat /tmp/bug375-seed.txt
    exit 1
fi

# Snapshot the seeded backing key so every "must not mutate" assertion
# has a definite pre-state to compare against.
PRE_VG=$(curl -s "$B/v1/storage-pool-definitions/bug375-existing" | python3 -c '
import sys, json
try:
    arr = json.load(sys.stdin)
    if isinstance(arr, list):
        spd = arr[0] if arr else {}
    else:
        spd = arr
except Exception:
    print("")
    sys.exit(0)
print((spd.get("props") or {}).get("StorDriver/LvmVg", ""))
')

if [[ "$PRE_VG" != "vg-original" ]]; then
    echo "FAIL: seeded SPD backing key wrong: got '$PRE_VG', want 'vg-original'"
    exit 1
fi

# --- override_props on a backing-driver key must 400 ---
echo ">> PUT override_props StorDriver/LvmVg=bogus on EXISTING SPD: must 400"
CODE=$(curl -s -o /tmp/bug375-resp.txt -w "%{http_code}" -X PUT \
    "$B/v1/storage-pool-definitions/bug375-existing" -H "Content-Type: application/json" \
    -d '{"override_props":{"StorDriver/LvmVg":"bug375-bogus"}}')

if [[ "$CODE" != "400" ]]; then
    echo "FAIL: Bug 375 PUT override_props returned $CODE, expected 400"
    cat /tmp/bug375-resp.txt
    exit 1
fi

if ! grep -q "StorDriver/LvmVg" /tmp/bug375-resp.txt; then
    echo "FAIL: rejection envelope missing offending key:"
    cat /tmp/bug375-resp.txt
    exit 1
fi

if ! grep -q "refusing to mutate" /tmp/bug375-resp.txt; then
    echo "FAIL: rejection envelope missing operator-facing phrase:"
    cat /tmp/bug375-resp.txt
    exit 1
fi

if ! grep -q "StorPoolDfn" /tmp/bug375-resp.txt; then
    echo "FAIL: rejection envelope missing StorPoolDfn obj_ref (operator can't grep audit log by SPD name):"
    cat /tmp/bug375-resp.txt
    exit 1
fi

echo "   400 + structured envelope OK"

# --- live row must stay byte-identical ---
POST_VG=$(curl -s "$B/v1/storage-pool-definitions/bug375-existing" | python3 -c '
import sys, json
try:
    arr = json.load(sys.stdin)
    spd = arr[0] if isinstance(arr, list) and arr else (arr if not isinstance(arr, list) else {})
except Exception:
    print("")
    sys.exit(0)
print((spd.get("props") or {}).get("StorDriver/LvmVg", ""))
')

if [[ "$POST_VG" != "$PRE_VG" ]]; then
    echo "FAIL: backing key mutated despite rejected PUT: $PRE_VG -> $POST_VG"
    exit 1
fi
echo ">> backing key unchanged post-rejection OK"

# --- delete_props on backing-driver key must 400 ---
echo ">> PUT delete_props=[StorDriver/LvmVg] on EXISTING SPD: must 400"
CODE=$(curl -s -o /tmp/bug375-resp.txt -w "%{http_code}" -X PUT \
    "$B/v1/storage-pool-definitions/bug375-existing" -H "Content-Type: application/json" \
    -d '{"delete_props":["StorDriver/LvmVg"]}')

if [[ "$CODE" != "400" ]]; then
    echo "FAIL: Bug 375 PUT delete_props returned $CODE, expected 400"
    cat /tmp/bug375-resp.txt
    exit 1
fi
echo "   400 OK"

# --- delete_namespaces=[StorDriver] must 400 ---
echo ">> PUT delete_namespaces=[StorDriver] on EXISTING SPD: must 400"
CODE=$(curl -s -o /tmp/bug375-resp.txt -w "%{http_code}" -X PUT \
    "$B/v1/storage-pool-definitions/bug375-existing" -H "Content-Type: application/json" \
    -d '{"delete_namespaces":["StorDriver"]}')

if [[ "$CODE" != "400" ]]; then
    echo "FAIL: Bug 375 PUT delete_namespaces returned $CODE, expected 400"
    cat /tmp/bug375-resp.txt
    exit 1
fi
echo "   400 OK"

# --- benign override_props on EXISTING SPD must STILL land (regression guard) ---
echo ">> PUT override_props Aux/rack=r1 on EXISTING SPD: must 200 (benign)"
CODE=$(curl -s -o /tmp/bug375-resp.txt -w "%{http_code}" -X PUT \
    "$B/v1/storage-pool-definitions/bug375-existing" -H "Content-Type: application/json" \
    -d '{"override_props":{"Aux/rack":"r1"}}')

if [[ "$CODE" != "200" ]]; then
    echo "FAIL: benign PUT Aux/rack returned $CODE, expected 200 (regression)"
    cat /tmp/bug375-resp.txt
    exit 1
fi
echo "   200 OK"

# --- final: backing key still byte-identical to pre-test ---
FINAL_VG=$(curl -s "$B/v1/storage-pool-definitions/bug375-existing" | python3 -c '
import sys, json
try:
    arr = json.load(sys.stdin)
    spd = arr[0] if isinstance(arr, list) and arr else (arr if not isinstance(arr, list) else {})
except Exception:
    print("")
    sys.exit(0)
print((spd.get("props") or {}).get("StorDriver/LvmVg", ""))
')

if [[ "$FINAL_VG" != "$PRE_VG" ]]; then
    echo "FAIL: backing key mutated across the test sequence: $PRE_VG -> $FINAL_VG"
    exit 1
fi

# --- PUT-create on brand-new SPD with StorDriver/* seed props must 200 ---
# This is the operator's one-round-trip provisioning flow
# (`linstor sp-d set-property new-def StorDriver/LvmVg vg-x`). The
# refusal is gated on "the SPD row exists already"; brand-new names
# are a seed path, not a mutation path.
echo ">> PUT-create brand-new SPD bug375-create with StorDriver/LvmVg seed: must 200"
CODE=$(curl -s -o /tmp/bug375-resp.txt -w "%{http_code}" -X PUT \
    "$B/v1/storage-pool-definitions/bug375-create" -H "Content-Type: application/json" \
    -d '{"override_props":{"StorDriver/LvmVg":"vg-fresh"}}')

if [[ "$CODE" != "200" ]]; then
    echo "FAIL: PUT-create with seed StorDriver/* returned $CODE, expected 200"
    cat /tmp/bug375-resp.txt
    exit 1
fi

SEED_VG=$(curl -s "$B/v1/storage-pool-definitions/bug375-create" | python3 -c '
import sys, json
try:
    arr = json.load(sys.stdin)
    spd = arr[0] if isinstance(arr, list) and arr else (arr if not isinstance(arr, list) else {})
except Exception:
    print("")
    sys.exit(0)
print((spd.get("props") or {}).get("StorDriver/LvmVg", ""))
')

if [[ "$SEED_VG" != "vg-fresh" ]]; then
    echo "FAIL: PUT-create seed value not persisted: got '$SEED_VG', want 'vg-fresh'"
    exit 1
fi

echo ">> PASS: Bug 375 — PUT storage-pool-definitions refuses StorDriver/* mutation on existing rows; PUT-create still seeds"
