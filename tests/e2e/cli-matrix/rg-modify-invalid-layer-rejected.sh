#!/usr/bin/env bash
#
# usage: rg-modify-invalid-layer-rejected.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 434: `rg modify` must reject an invalid
# select_filter.layer_stack the create path already refuses, and no RD
# created against the RG may inherit an invalid stack.
#
# Pre-fix, handleRGUpdate gated only place_count, so an invalid
# layer_stack was merged + persisted unvalidated; and handleRDCreate
# validated an EXPLICIT layer_list BEFORE inheritLayerStackFromRG, so an
# RD created against such an RG inherited the invalid stack unchecked. On
# the stand the satellite/dispatcher cannot materialise an out-of-order
# layer chain (drbdadm / LVM config error) → spawn failure / hot-loop.
#
# Sequence (STORAGE-before-DRBD is the invalid ordering — STORAGE must be
# terminal, DRBD must be first):
#   1. rg c <rg> --layer-list drbd,storage        (valid, accepted)
#   2. rg m <rg> --layer-list storage,drbd         MUST be rejected
#   3. the RG keeps its valid stack (the reject never persisted)
#   4. rd c <rd> --resource-group <rg> (inherit) → RD stack is the valid
#      [DRBD,STORAGE], never [STORAGE,DRBD]

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

linstor_cli_setup

RG=cli-b434-rg
RD=cli-b434-rd

cleanup() {
    delete_rd "$RD" 2>/dev/null || true
    "${LCTL[@]}" resource-group delete "$RG" >/dev/null 2>&1 || true
    linstor_cli_teardown
}
trap cleanup EXIT

# rg_layer_stack / rd_layer_stack render the stored layer stack as a
# comma-joined string (empty when unset).
rg_layer_stack() {
    kubectl get resourcegroup "$1" -o json 2>/dev/null \
        | jq -r '.spec.selectFilter.layerStack // [] | join(",")' 2>/dev/null || echo ""
}
rd_layer_stack() {
    kubectl get resourcedefinition "$1" -o json 2>/dev/null \
        | jq -r '.spec.layerStack // [] | join(",")' 2>/dev/null || echo ""
}

# ---- 1. RG with a VALID layer stack ---------------------------------------
echo ">> rg c $RG --layer-list drbd,storage"
_out=$("${LCTL[@]}" resource-group create "$RG" --layer-list drbd,storage 2>&1) \
    || { echo "FAIL: rg c $RG: $_out" >&2; exit 1; }

# ---- 2. rg modify to an INVALID stack MUST be rejected --------------------
echo ">> rg m $RG --layer-list storage,drbd (must be rejected)"
out=$(mktemp)
set +e
"${LCTL[@]}" resource-group modify "$RG" --layer-list storage,drbd >"$out" 2>&1
rc=$?
set -e
if (( rc == 0 )); then
    echo "FAIL (Bug 434): rg modify ACCEPTED an invalid layer stack storage,drbd" >&2
    cat "$out" >&2; rm -f "$out"; exit 1
fi
if ! grep -qiE 'invalid layer order|layer' "$out"; then
    echo "FAIL (Bug 434): rg-modify rejection must mention the layer fault; got:" >&2
    cat "$out" >&2; rm -f "$out"; exit 1
fi
rm -f "$out"
echo ">>   rg modify rejected OK"

# ---- 3. the rejected modify must NOT have persisted -----------------------
stored=$(rg_layer_stack "$RG")
if [[ "$stored" != "DRBD,STORAGE" ]]; then
    echo "FAIL (Bug 434): RG $RG layer stack changed after a REJECTED modify: got '$stored', want 'DRBD,STORAGE'" >&2
    exit 1
fi
echo ">>   RG stack still 'DRBD,STORAGE' OK"

# ---- 4. an RD inheriting from the RG gets the VALID stack ------------------
echo ">> rd c $RD --resource-group $RG (inherit)"
_out=$("${LCTL[@]}" resource-definition create "$RD" --resource-group "$RG" 2>&1) \
    || { echo "FAIL: rd c $RD inherit: $_out" >&2; exit 1; }

rd_stack=""
deadline=$(( $(date +%s) + 30 ))
while (( $(date +%s) < deadline )); do
    rd_stack=$(rd_layer_stack "$RD")
    [[ -n "$rd_stack" ]] && break
    sleep 2
done
if [[ "$rd_stack" == "STORAGE,DRBD" ]]; then
    echo "FAIL (Bug 434): RD $RD inherited the INVALID layer stack [$rd_stack]" >&2
    exit 1
fi
echo ">>   RD inherited stack '[$rd_stack]' (not STORAGE,DRBD) OK"

echo ">> rg-modify-invalid-layer-rejected (Bug 434) OK"
