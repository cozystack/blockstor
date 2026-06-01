#!/usr/bin/env bash
#
# usage: rd-name-validation-bulk.sh WORK_DIR
#
# Bug-hunt v3 finding C.4: the lowercase-RD-name validator that
# `POST /v1/resource-definitions` enforces (Bug 97) is bypassed on two
# sibling REST handlers that ALSO mint a fresh RD entry from a user-
# supplied name:
#
#   - linstor s r rst SRC --fs SNAP --tr <NAME>   (snapshot-restore)
#   - linstor rg spawn  <RG>  <NAME>             (rg-spawn)
#
# Both flow the raw name straight into Store.ResourceDefinitions().
# Create(), the k8s CRD store lowercases the metadata.name, and the
# CRD admission webhook then rejects on
# `metadata.name must equal <spec.resourceDefinitionName>.<spec.nodeName>`.
# By the time the rejection fires, a partial RD entry has already
# landed in the linstor view and the operator sees a raw
# `metadata.name must equal …` leak.
#
# This scenario hits all four RD-minting REST entry points with an
# invalid name AND a valid name. Each invalid attempt MUST:
#   - return a 4xx envelope (NOT a 500/2xx),
#   - NOT leave an RD row in the linstor view (no orphan / partial state),
#   - NOT leak the internal `metadata.name` k8s error string.
#
# Each valid attempt MUST succeed (guards against an over-strict gate
# that would break the production `pvc-<uuid>` csi clone flow).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

NS=${NS:-blockstor-system}

# RG + source RD + snapshot we reuse across the invalid attempts. The
# snapshot is taken on a 1-replica RD so we don't need a healthy DRBD
# cluster — these gates fire at the wire boundary before any satellite
# touches state.
RG=e2e-c4-rg
SRC=e2e-c4-src
SNAP=e2e-c4-snap

# Worker count is mostly irrelevant here — we don't even autoplace —
# but require ≥1 so the source RD lands somewhere.
require_workers 1

cleanup() {
    set +e
    # Best-effort cleanup of every RD this scenario MIGHT have created,
    # including the orphan rows the bug used to leak. The orphan check
    # itself happens BEFORE this, so cleanup here just keeps the stand
    # tidy for the next scenario.
    for rd in e2e-c4-src e2e-c4-good-spawn e2e-c4-good-restore; do
        delete_rd "$rd" 2>/dev/null || true
    done
    kubectl delete resourcegroup "$RG" --ignore-not-found --timeout=30s 2>/dev/null
    set -e
}
trap cleanup EXIT

# ---- set up the fixtures ----

echo ">> seed RG $RG"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.cozystack.io/v1alpha1
kind: ResourceGroup
metadata: {name: ${RG}}
spec:
  selectFilter:
    placeCount: 1
EOF

echo ">> seed source RD $SRC + 1 VD"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.cozystack.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${SRC}}
spec:
  resourceGroupName: ${RG}
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}
EOF

echo ">> seed Snapshot $SNAP on $SRC"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.cozystack.io/v1alpha1
kind: Snapshot
metadata: {name: ${SNAP}}
spec:
  resourceName: ${SRC}
  nodes: [${WORKER_1}]
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}
EOF

# ---- port-forward the apiserver so we can drive REST directly ----
#
# We hit /v1 directly rather than the `linstor` CLI because the bug is
# REST-side and we want to assert on the response envelope shape — the
# CLI's own client-side validation could mask the wire surface.

LPORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "${LPORT}:3370" >/dev/null 2>&1 &
PF_PID=$!
_wait_port_forward "$LPORT" "$PF_PID" || {
    echo "FAIL: could not port-forward apiserver"
    exit 1
}
trap 'kill "$PF_PID" 2>/dev/null || true; cleanup' EXIT

API="http://127.0.0.1:${LPORT}"

# ---- helpers ----

# assert_reject ENDPOINT METHOD BODY DESCRIPTION
#   Expect a 4xx with no `metadata.name` leak. Prints the body for
#   debugging on failure.
assert_reject() {
    local path=$1 method=$2 body=$3 desc=$4
    local code body_file
    body_file=$(mktemp)
    code=$(curl -sS -o "$body_file" -w "%{http_code}" \
        -X "$method" -H 'Content-Type: application/json' \
        "${API}${path}" -d "$body" || echo "000")

    if [[ "$code" -lt 400 || "$code" -ge 500 ]]; then
        echo "FAIL: $desc — expected 4xx, got HTTP $code"
        cat "$body_file"
        rm -f "$body_file"
        return 1
    fi

    if grep -q 'metadata.name' "$body_file"; then
        echo "FAIL: $desc — envelope leaks internal k8s metadata.name path:"
        cat "$body_file"
        rm -f "$body_file"
        return 1
    fi

    echo "   ok: $desc (HTTP $code)"
    rm -f "$body_file"
}

# assert_no_orphan_rd RDNAME
#   The Bug C.4 wedge: a partial RD entry survived the CRD admission
#   rejection. After the call returns 4xx, the linstor view (== the
#   ResourceDefinition CRDs) MUST hold zero rows for that name.
assert_no_orphan_rd() {
    local rd=$1
    # Direct CRD read — the API surface we trust most. `linstor rd l`
    # would also work but pulls extra translation glue we don't need.
    if kubectl get resourcedefinition "$rd" -o name 2>/dev/null | grep -q .; then
        echo "FAIL: orphan RD '$rd' persisted despite 4xx — Bug C.4 leaks state"
        kubectl get resourcedefinition "$rd" -o yaml
        return 1
    fi
    echo "   ok: no orphan RD '$rd'"
}

# assert_accept ENDPOINT BODY DESCRIPTION
#   Sanity-check the happy path so the gate isn't over-strict.
assert_accept() {
    local path=$1 body=$2 desc=$3
    local code body_file
    body_file=$(mktemp)
    code=$(curl -sS -o "$body_file" -w "%{http_code}" \
        -X POST -H 'Content-Type: application/json' \
        "${API}${path}" -d "$body" || echo "000")

    if [[ "$code" -ne 201 ]]; then
        echo "FAIL: $desc — expected HTTP 201, got $code"
        cat "$body_file"
        rm -f "$body_file"
        return 1
    fi

    echo "   ok: $desc (HTTP $code)"
    rm -f "$body_file"
}

# ---- the matrix ----
#
# Invalid name shapes — each entry point hits each shape. Adding cases
# here is the recommended way to widen the gate; mirrors the
# TestSnapshotRestoreRejectsInvalidRdName / TestResourceGroupSpawn
# RejectsInvalidRdName table in the unit-test sibling.
INVALID_NAMES=(
    "hunt3-Bad"        # mixed-case, the operator-poke repro
    "BADNAME"          # pure uppercase
    "bad_underscore"   # underscore
    "bad-"             # trailing hyphen
    "-bad"             # leading hyphen
    "bad.name"         # embedded dot
)

# 1. rd c — already gated (Bug 97); assert the contract still holds so
#    a regression of the existing gate gets caught here too.
echo
echo ">> 1. POST /v1/resource-definitions (rd c) with invalid names"
for n in "${INVALID_NAMES[@]}"; do
    assert_reject "/v1/resource-definitions" "POST" \
        "{\"resource_definition\":{\"name\":\"$n\"}}" \
        "rd c '$n'"
    assert_no_orphan_rd "$n"
done

# 2. snapshot-restore-resource (s r rst) — NEW gate.
echo
echo ">> 2. POST /v1/resource-definitions/${SRC}/snapshot-restore-resource/${SNAP} (s r rst) with invalid to_resource"
for n in "${INVALID_NAMES[@]}"; do
    assert_reject \
        "/v1/resource-definitions/${SRC}/snapshot-restore-resource/${SNAP}" \
        "POST" \
        "{\"to_resource\":\"$n\"}" \
        "s r rst → '$n'"
    assert_no_orphan_rd "$n"
done

# 3. snapshot-restore-volume-definition — sibling endpoint, same gate.
echo
echo ">> 3. POST /v1/resource-definitions/${SRC}/snapshot-restore-volume-definition/${SNAP} with invalid to_resource"
for n in "${INVALID_NAMES[@]}"; do
    assert_reject \
        "/v1/resource-definitions/${SRC}/snapshot-restore-volume-definition/${SNAP}" \
        "POST" \
        "{\"to_resource\":\"$n\"}" \
        "s vd rst → '$n'"
done

# 4. rg spawn — NEW gate.
echo
echo ">> 4. POST /v1/resource-groups/${RG}/spawn (rg spawn) with invalid resource_definition_name"
for n in "${INVALID_NAMES[@]}"; do
    assert_reject \
        "/v1/resource-groups/${RG}/spawn" \
        "POST" \
        "{\"resource_definition_name\":\"$n\",\"volume_sizes\":[1048576]}" \
        "rg spawn '$n'"
    assert_no_orphan_rd "$n"
done

# 5. happy path — valid names must still round-trip.
echo
echo ">> 5. valid names still pass (happy-path guard)"
assert_accept "/v1/resource-groups/${RG}/spawn" \
    '{"resource_definition_name":"e2e-c4-good-spawn","volume_sizes":[1048576]}' \
    "rg spawn 'e2e-c4-good-spawn'"

assert_accept \
    "/v1/resource-definitions/${SRC}/snapshot-restore-resource/${SNAP}" \
    '{"to_resource":"e2e-c4-good-restore"}' \
    "s r rst → 'e2e-c4-good-restore'"

# Confirm those two DID land — guards against the test being a no-op.
if ! kubectl get resourcedefinition e2e-c4-good-spawn -o name >/dev/null 2>&1; then
    echo "FAIL: valid spawn name 'e2e-c4-good-spawn' did NOT persist (gate is over-strict)"
    exit 1
fi
echo "   ok: valid RD 'e2e-c4-good-spawn' persisted"

if ! kubectl get resourcedefinition e2e-c4-good-restore -o name >/dev/null 2>&1; then
    echo "FAIL: valid restore name 'e2e-c4-good-restore' did NOT persist (gate is over-strict)"
    exit 1
fi
echo "   ok: valid RD 'e2e-c4-good-restore' persisted"

echo
echo "PASS: rd-name-validation-bulk — all 4 RD-minting REST entry points reject invalid names without leaking state"
