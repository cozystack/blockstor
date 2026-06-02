#!/usr/bin/env bash
#
# usage: vd-volume-number-validation.sh WORK_DIR
#
# Bug 363 (hunt-v4) regression catcher: an explicit `volume_number`
# outside DRBD-9's addressable [0, 65535] range slipped past the
# wire-side decode and persisted in the store. The follow-up `r c`
# succeeded too, but the satellite reconciler then hung in
#
#   waiting for controller-side DRBD-ID allocation
#     resource=<rd>.<node> nodeID=0 port=20000 minor=null
#
# indefinitely because no positive minor can be derived from a
# negative VlmNr (and DRBD-9 caps the per-resource volume namespace
# at 16 bits — values above 65535 trigger the same stall).
#
# Reproduction on dev stand (HEAD 6f69c5678, 2026-06-01):
#
#   $ linstor vd c -n -1 probe-bug363 64M
#   SUCCESS: volume definition created
#   $ linstor r c dev-worker-1 probe-bug363 --storage-pool stand
#   SUCCESS: resource(s) created on resource-definition: probe-bug363
#   $ linstor r l -r probe-bug363
#     probe-bug363 | dev-worker-1 | DRBD,STORAGE | Unused | | Unknown
#
# Fix: validate explicit volume_number is in [0, 65535] at the REST
# wire boundary, before any partial state lands.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

NS=${NS:-blockstor-system}

require_workers 1

# ---- port-forward the apiserver ----

LPORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "${LPORT}:3370" >/dev/null 2>&1 &
PF_PID=$!
_wait_port_forward "$LPORT" "$PF_PID" || {
    echo "FAIL: could not port-forward apiserver"
    exit 1
}

RD=e2e-bug363-rd

cleanup() {
    set +e
    kill "$PF_PID" 2>/dev/null || true
    delete_rd "$RD" 2>/dev/null || true
    set -e
}
trap cleanup EXIT

API="http://127.0.0.1:${LPORT}"

echo ">> seed RD $RD"
kubectl apply -f - >/dev/null <<EOF
apiVersion: blockstor.cozystack.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec: {}
EOF

# assert_reject VLMNR DESC
#   POST vd-create with `volume_number=VLMNR`; expect 400 envelope
#   that names the rule. Asserts no VD landed in the store.
assert_reject() {
    local vn=$1 desc=$2
    local code body_file body
    body_file=$(mktemp)

    code=$(curl -sS -o "$body_file" -w "%{http_code}" \
        -X POST -H 'Content-Type: application/json' \
        "${API}/v1/resource-definitions/${RD}/volume-definitions" \
        -d "{\"volume_definition\":{\"volume_number\":${vn},\"size_kib\":65536}}" || echo "000")

    body=$(cat "$body_file")
    rm -f "$body_file"

    if [[ "$code" -ne 400 ]]; then
        echo "FAIL: $desc — expected 400, got HTTP $code"
        echo "$body"
        return 1
    fi

    if ! grep -qE 'minimum|maximum' <<<"$body"; then
        echo "FAIL: $desc — envelope must name the rule (minimum/maximum); got:"
        echo "$body"
        return 1
    fi

    echo "   ok: $desc refused (HTTP $code)"
}

# assert_accept VLMNR DESC
#   POST vd-create with `volume_number=VLMNR`; expect 200. Removes
#   the VD afterwards so subsequent asserts can reuse the parent RD.
assert_accept() {
    local vn=$1 desc=$2
    local code body_file body
    body_file=$(mktemp)

    code=$(curl -sS -o "$body_file" -w "%{http_code}" \
        -X POST -H 'Content-Type: application/json' \
        "${API}/v1/resource-definitions/${RD}/volume-definitions" \
        -d "{\"volume_definition\":{\"volume_number\":${vn},\"size_kib\":65536}}" || echo "000")

    body=$(cat "$body_file")
    rm -f "$body_file"

    if [[ "$code" -ne 200 ]]; then
        echo "FAIL: $desc — expected 200, got HTTP $code"
        echo "$body"
        return 1
    fi

    # Drop the VD so the next assert can reuse the RD.
    curl -sS -X DELETE \
        "${API}/v1/resource-definitions/${RD}/volume-definitions/${vn}" >/dev/null

    echo "   ok: $desc accepted (HTTP $code)"
}

echo ">> Bug 363: refuse VlmNr outside DRBD-9 [0, 65535]"
assert_reject -1         "VlmNr=-1 (signed-int32 leak)"
assert_reject 65536      "VlmNr=65536 (above DRBD-9 16-bit cap)"
assert_reject 100000     "VlmNr=100000"
assert_reject 2147483647 "VlmNr=int32-max"

echo ">> Bug 363: accept VlmNr at the [0, 65535] boundaries"
assert_accept 0     "VlmNr=0"
assert_accept 1     "VlmNr=1"
assert_accept 65535 "VlmNr=65535 (upper boundary)"

echo ">> BUG-363 OK: explicit volume_number is gated at the wire"
