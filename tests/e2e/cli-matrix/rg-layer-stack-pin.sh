#!/usr/bin/env bash
#
# usage: rg-layer-stack-pin.sh WORK_DIR
#
# L6 cli-matrix cell — a definition's layer stack is decided once, from
# its resource group, and never reshapes a volume that already exists.
#
# The controller writes the group's stack onto a definition that leaves
# it empty (internal/controller/resourcedefinition_layer_stack.go). A
# definition that holds no data takes the group's stack; one that already
# went to disk under the default stack keeps it, carries the
# blockstor.io/unapplied-group-layer-stack annotation and gets a
# LayerStackNotInherited warning Event, so a LUKS group added over a
# plaintext volume never has the satellite luksFormat that volume.
#
# Steps:
#   a) rg c <plain-rg> (no stack) and rg c <luks-rg> --layer-list
#      drbd,luks,storage; rd c in <plain-rg> stays without a stack; rd
#      modify --resource-group <luks-rg> → spec.layerStack becomes
#      DRBD,LUKS,STORAGE. The REST create copies a group's stack itself,
#      so only a move into the group shows the controller's pin.
#   b) rd c + vd c + r c --auto-place=2 in <plain-rg>; write data.
#   c) rg modify <plain-rg> --layer-list drbd,luks,storage.
#   d) rd clone of the data RD straight after (c). Inside the window before
#      the controller pins it the clone is refused, saying the stack is not
#      pinned yet; it then succeeds. A stand fast enough to pin before the
#      clone lands sees the success directly.
#   e) the data RD's spec.layerStack stays DRBD,STORAGE, the annotation
#      names the group's stack, the Event is there, the data reads back
#      unchanged; the clone has DRBD,STORAGE too.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

LUKS_RG=cli-matrix-ls-luks
PLAIN_RG=cli-matrix-ls-plain
RD_FRESH=cli-matrix-ls-fresh
RD_DATA=cli-matrix-ls-data
RD_CLONE=cli-matrix-ls-clone
POOL=${POOL:-lvm-thin}
PASSPHRASE='cli-matrix-ls-pp!'
ANNOTATION=blockstor.io/unapplied-group-layer-stack

cleanup() {
    delete_rd "$RD_CLONE"
    delete_rd "$RD_DATA"
    delete_rd "$RD_FRESH"
    "${LCTL[@]}" resource-group delete "$PLAIN_RG" >/dev/null 2>&1 || true
    "${LCTL[@]}" resource-group delete "$LUKS_RG" >/dev/null 2>&1 || true
    assert_no_orphans "$RD_DATA"
    assert_no_orphans "$RD_CLONE"
    cleanup_encryption_state
    linstor_cli_teardown
}
trap cleanup EXIT

cleanup_encryption_state

# rd_layer_stack RD — the stack the CRD carries, comma-joined.
rd_layer_stack() {
    kubectl get "resourcedefinitions.blockstor.org/$1" \
        -o jsonpath='{.spec.layerStack}' 2>/dev/null \
        | jq -r 'join(",") | ascii_upcase' 2>/dev/null || echo ""
}

# wait_layer_stack RD EXPECTED [TIMEOUT=60]
wait_layer_stack() {
    local rd=$1 expected=$2 timeout=${3:-60} got=""
    local deadline=$(( $(date +%s) + timeout ))
    while (( $(date +%s) < deadline )); do
        got=$(rd_layer_stack "$rd")
        [[ "$got" == "$expected" ]] && return 0
        sleep 2
    done
    echo "FAIL: $rd layer stack '$got', want '$expected' within ${timeout}s" >&2
    return 1
}

echo ">> pre-flight: 2 healthy $POOL SPs"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 2 )); then
    echo "SKIP: $POOL SP not on >=2 nodes (got $ok_nodes)"
    exit 0
fi

"${LCTL[@]}" encryption create-passphrase --passphrase "$PASSPHRASE" >/dev/null 2>&1 || true
"${LCTL[@]}" controller set-property DrbdOptions/EncryptPassphrase "$PASSPHRASE" >/dev/null 2>&1 || true

echo ">> a) an RD with nothing on disk moved into a LUKS group takes its stack"
"${LCTL[@]}" resource-group create "$PLAIN_RG" --storage-pool "$POOL" >/dev/null
"${LCTL[@]}" resource-group create "$LUKS_RG" --layer-list drbd,luks,storage --storage-pool "$POOL" >/dev/null
"${LCTL[@]}" resource-definition create --resource-group "$PLAIN_RG" "$RD_FRESH" >/dev/null
sleep 10
fresh_stack=$(rd_layer_stack "$RD_FRESH")
if [[ -n "$fresh_stack" ]]; then
    echo "FAIL: $RD_FRESH in a group without a stack has '$fresh_stack', want none" >&2
    exit 1
fi
# Nothing on the REST side copies the stack on a modify: the controller's
# pin does.
"${LCTL[@]}" resource-definition modify --resource-group "$LUKS_RG" "$RD_FRESH" >/dev/null
wait_layer_stack "$RD_FRESH" "DRBD,LUKS,STORAGE"

echo ">> b) an RD with data under a group that sets no stack"
"${LCTL[@]}" resource-definition create --resource-group "$PLAIN_RG" "$RD_DATA" >/dev/null
"${LCTL[@]}" volume-definition create "$RD_DATA" 64M >/dev/null
"${LCTL[@]}" resource create --auto-place=2 --storage-pool="$POOL" "$RD_DATA" >/dev/null

deadline=$(( $(date +%s) + 60 ))
placed=()
while (( $(date +%s) < deadline )); do
    mapfile -t placed < <(linstor_diskful_nodes "$RD_DATA")
    if (( ${#placed[@]} == 2 )); then break; fi
    sleep 2
done
if (( ${#placed[@]} != 2 )); then
    echo "FAIL: $RD_DATA did not autoplace 2 replicas" >&2
    exit 1
fi
N1="${placed[0]}"
N2="${placed[1]}"

wait_uptodate "$RD_DATA" "$N1" "$N2"
DEV=$(device_for_rd "$RD_DATA" "$N1")
RD=$RD_DATA
md5_before=$(write_random "$N1" "$DEV" 262144)

echo ">> c) rg modify --layer-list drbd,luks,storage over the group holding data"
"${LCTL[@]}" resource-group modify "$PLAIN_RG" --layer-list drbd,luks,storage >/dev/null

echo ">> d) clone the data RD while its stack may still be undecided"
refused=0
deadline=$(( $(date +%s) + 60 ))
err_file=$(mktemp)
until "${LCTL[@]}" resource-definition clone "$RD_DATA" "$RD_CLONE" >/dev/null 2>"$err_file"; do
    if ! grep -q "not pinned yet" "$err_file"; then
        echo "FAIL: clone refused for another reason than the undecided stack:" >&2
        cat "$err_file" >&2
        rm -f "$err_file"
        exit 1
    fi
    refused=1
    if (( $(date +%s) >= deadline )); then
        echo "FAIL: clone still refused as undecided after 60s" >&2
        rm -f "$err_file"
        exit 1
    fi
    sleep 2
done
rm -f "$err_file"
echo "   clone refused as undecided before the pin: $refused"

echo ">> e) the data RD keeps the stack its data has"
wait_layer_stack "$RD_DATA" "DRBD,STORAGE"

annotation=$(kubectl get "resourcedefinitions.blockstor.org/$RD_DATA" -o json 2>/dev/null \
    | jq -r --arg k "$ANNOTATION" '.metadata.annotations[$k] // ""')
if [[ "${annotation^^}" != "DRBD,LUKS,STORAGE" ]]; then
    echo "FAIL: $RD_DATA annotation $ANNOTATION = '$annotation', want DRBD,LUKS,STORAGE" >&2
    exit 1
fi

deadline=$(( $(date +%s) + 30 ))
events=0
while (( $(date +%s) < deadline )); do
    events=$(kubectl get events -A --field-selector "reason=LayerStackNotInherited,involvedObject.name=$RD_DATA" \
        --no-headers 2>/dev/null | wc -l)
    (( events > 0 )) && break
    sleep 2
done
if (( events == 0 )); then
    echo "FAIL: no LayerStackNotInherited event on $RD_DATA" >&2
    exit 1
fi

md5_after=$(read_md5 "$N1" "$DEV" 262144)
if [[ "$md5_before" != "$md5_after" ]]; then
    echo "FAIL: $RD_DATA data changed after the group gained LUKS (before=$md5_before after=$md5_after)" >&2
    exit 1
fi

wait_layer_stack "$RD_CLONE" "DRBD,STORAGE"

echo ">> rg-layer-stack-pin OK"
