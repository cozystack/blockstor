#!/usr/bin/env bash
#
# usage: rd-name-length-48.sh WORK_DIR
#
# Bug 360 (hunt-v4) regression catcher: long RD names (>63 chars)
# slipped past the wire-side gate (which capped at 253 — the k8s
# metadata.name regime) and then exploded inside the store layer
# because the k8s store writes the RD-name into `metadata.labels`
# (LabelResourceDefinition in pkg/store/k8s/resources.go). k8s label
# VALUES are bounded to 63 chars — anything longer leaks the raw
# apimachinery error through the next `r c` and leaves a zombie RD
# behind that accepts `vd c` but never accepts a replica.
#
# Reproduction on dev stand (HEAD 6f69c5678, 2026-06-01):
#
#   $ linstor rd c $(printf 'a%.0s' {1..150})
#   SUCCESS: resource definition created
#   $ linstor vd c $LONG_NAME 64M
#   SUCCESS: volume definition created
#   $ linstor r c dev-worker-1 $LONG_NAME --storage-pool stand
#   ERROR: store error: <backend> "<150-a-name>.dev-worker-1" is
#     invalid: metadata.labels: Invalid value: "aaa…aaa": must be no
#     more than 63 characters
#
# Fix: cap maxLinstorName at 48 (matches upstream LINSTOR's
# DRBD_RES_NAME_MAX). The wire gate now refuses at rd-create time
# before any partial state lands.
#
# This e2e mirrors the unit test
# TestBug360RDCreateRefusedOnNameOver48 but exercises the live
# apiserver port (the REST layer drives the same gate the CLI does,
# so we hit the wire path the operator hits).

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

cleanup() {
    set +e
    kill "$PF_PID" 2>/dev/null || true
    # Best-effort cleanup if a borderline RD slipped through.
    for n in 48 49; do
        local name
        # shellcheck disable=SC2155
        name=$(printf 'a%.0s' $(seq 1 "$n"))
        kubectl delete resourcedefinition.blockstor.cozystack.io "$name" \
            --ignore-not-found --timeout=10s 2>/dev/null || true
    done
    set -e
}
trap cleanup EXIT

API="http://127.0.0.1:${LPORT}"

# ---- helpers ----

# assert_reject_long NAME_LEN
#   POST rd-create with an `a` * NAME_LEN name; expect a 4xx envelope
#   that mentions the cap and does NOT leak `metadata.labels`. Then
#   assert no RD landed in the store under that name.
assert_reject_long() {
    local n=$1
    local name body_file code body
    name=$(printf 'a%.0s' $(seq 1 "$n"))
    body_file=$(mktemp)

    code=$(curl -sS -o "$body_file" -w "%{http_code}" \
        -X POST -H 'Content-Type: application/json' \
        "${API}/v1/resource-definitions" \
        -d "{\"resource_definition\":{\"name\":\"${name}\"}}" || echo "000")

    body=$(cat "$body_file")
    rm -f "$body_file"

    if [[ "$code" -lt 400 || "$code" -ge 500 ]]; then
        echo "FAIL: len=$n — expected 4xx, got HTTP $code"
        echo "$body"
        return 1
    fi

    if ! grep -qE 'exceeds|valid identifier' <<<"$body"; then
        echo "FAIL: len=$n — envelope must name the rule (exceeds N chars); got:"
        echo "$body"
        return 1
    fi

    if grep -q 'metadata\.labels' <<<"$body"; then
        echo "FAIL: len=$n — envelope leaks k8s metadata.labels error:"
        echo "$body"
        return 1
    fi

    # Zombie RD assert: the store must NOT hold a row for this name
    # after the refusal.
    if kubectl get resourcedefinition.blockstor.cozystack.io "$name" >/dev/null 2>&1; then
        echo "FAIL: len=$n — zombie RD CRD survived a refused rd-create"
        return 1
    fi

    echo "   ok: len=$n refused at wire gate (HTTP $code)"
}

# assert_accept_at_boundary NAME_LEN
#   POST rd-create with an `a` * NAME_LEN name; expect 201 + the RD
#   visible via `kubectl get`.
assert_accept_at_boundary() {
    local n=$1
    local name body_file code body
    name=$(printf 'a%.0s' $(seq 1 "$n"))
    body_file=$(mktemp)

    code=$(curl -sS -o "$body_file" -w "%{http_code}" \
        -X POST -H 'Content-Type: application/json' \
        "${API}/v1/resource-definitions" \
        -d "{\"resource_definition\":{\"name\":\"${name}\"}}" || echo "000")

    body=$(cat "$body_file")
    rm -f "$body_file"

    if [[ "$code" -ne 201 ]]; then
        echo "FAIL: len=$n — boundary name was refused; HTTP $code"
        echo "$body"
        return 1
    fi

    # Best-effort visibility check; informer cache settles in <1s on the
    # dev stand.
    deadline=$(( $(date +%s) + 10 ))
    while (( $(date +%s) < deadline )); do
        if kubectl get resourcedefinition.blockstor.cozystack.io "$name" >/dev/null 2>&1; then
            echo "   ok: len=$n accepted at boundary (HTTP $code)"
            kubectl delete resourcedefinition.blockstor.cozystack.io "$name" \
                --ignore-not-found --timeout=10s >/dev/null 2>&1 || true
            return 0
        fi
        sleep 1
    done

    echo "FAIL: len=$n — boundary RD not visible after accept"
    return 1
}

# ---- run the matrix ----

echo ">> Bug 360: refuse names strictly over 48 chars"
for n in 49 64 150 250; do
    assert_reject_long "$n"
done

echo ">> Bug 360: accept names at the 48-char boundary"
assert_accept_at_boundary 48

echo ">> Bug 360: rg-spawn entry point also refuses long names"
# Seed a tiny RG so the spawn handler reaches the name-validation gate.
RG=e2e-bug360-rg
kubectl apply -f - >/dev/null <<EOF
apiVersion: blockstor.cozystack.io/v1alpha1
kind: ResourceGroup
metadata: {name: ${RG}}
spec:
  selectFilter:
    placeCount: 1
EOF
trap 'kubectl delete resourcegroup '"$RG"' --ignore-not-found --timeout=10s >/dev/null 2>&1 || true; kill "$PF_PID" 2>/dev/null || true' EXIT

long_spawn_name=$(printf 'b%.0s' $(seq 1 64))
body_file=$(mktemp)
code=$(curl -sS -o "$body_file" -w "%{http_code}" \
    -X POST -H 'Content-Type: application/json' \
    "${API}/v1/resource-groups/${RG}/spawn" \
    -d "{\"resource_definition_name\":\"${long_spawn_name}\"}" || echo "000")
body=$(cat "$body_file")
rm -f "$body_file"

if [[ "$code" -lt 400 || "$code" -ge 500 ]]; then
    echo "FAIL: rg-spawn — expected 4xx for long RD name, got HTTP $code"
    echo "$body"
    exit 1
fi

if ! grep -qE 'exceeds|valid identifier' <<<"$body"; then
    echo "FAIL: rg-spawn envelope must name the rule; got:"
    echo "$body"
    exit 1
fi

if kubectl get resourcedefinition.blockstor.cozystack.io "$long_spawn_name" >/dev/null 2>&1; then
    echo "FAIL: rg-spawn left a zombie RD CRD behind"
    exit 1
fi
echo "   ok: rg-spawn refused (HTTP $code)"

echo ">> BUG-360 OK: long RD names are now refused at the wire gate"
