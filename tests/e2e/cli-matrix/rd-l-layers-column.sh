#!/usr/bin/env bash
#
# usage: rd-l-layers-column.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 349.
#
# Reproduction from the e2e2 stand (user-reported, 2026-05-19):
#
#   $ linstor rd l
#   ╭───────────────────────────────────────────────╮
#   ┊ ResourceName ┊ ResourceGroup ┊ Layers ┊ State ┊
#   ╞═══════════════════════════════════════════════╡
#   ┊ test         ┊ DfltRscGrp    ┊        ┊ ok    ┊
#   ┊ test2        ┊ DfltRscGrp    ┊        ┊ ok    ┊
#   ╰───────────────────────────────────────────────╯
#
# The Layers column is empty. Upstream LINSTOR renders the RD's
# layer-list (e.g. `DRBD,STORAGE`) here so the operator sees the
# stack at a glance without `rd lp` / `rd l --pastable`. blockstor's
# REST GET /v1/resource-definitions response omits or empties the
# `layer_data[].type` array (or the apiserver-side aggregator drops
# it before rendering).
#
# Contract:
#   1. Create an RD with `--layer-list DRBD,STORAGE` explicit.
#   2. Run `linstor rd l --machine-readable` and `linstor rd l`.
#   3. Assert the machine-readable JSON contains a non-empty
#      `layer_data[].type` array including at least DRBD and
#      STORAGE (or top-level `resource_definition.layer_data`).
#   4. Assert the plain-text Layers column contains both DRBD
#      and STORAGE — no empty cell.
#
# Bug 58 closed `layer_data[]` serialisation on RD/VD/Resource
# wire (task #215), and Bug 222 closed RD layer-list inheritance
# (task #211). Run 40 caught a regression in the render or one
# specific code-path — the layer_data is present in the CRD but
# not in the `rd l` aggregator output.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

linstor_cli_setup
trap linstor_cli_teardown EXIT

RD=cli-matrix-rd-l-layers

cleanup() {
    "${LCTL[@]}" resource-definition delete "$RD" 2>/dev/null || true
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> [Bug 349] rd c $RD --layer-list DRBD,STORAGE"
_out=$("${LCTL[@]}" resource-definition create "$RD" --layer-list DRBD,STORAGE 2>&1) \
    || { echo "FAIL: rd c with --layer-list DRBD,STORAGE: $_out" >&2; exit 1; }

# --- machine-readable check -----------------------------------------------
echo ">> [Bug 349] machine-readable: layer_data must include DRBD and STORAGE"
mr=$("${LCTL[@]}" --machine-readable resource-definition list --resource-definitions "$RD" 2>/dev/null)

# Layer types may be exposed as either top-level `layer_data[].type`
# on the RD object OR nested under .resource_definitions[]. Probe
# both shapes — blockstor's apiserver wraps results in a
# top-level array, so the actual RD entry is one level deep.
layer_types=$(jq -r --arg rd "$RD" '
    [
        ( .[0].rsc_dfns[]? | select(.name==$rd) | .layer_data[]?.type ),
        ( .[0].resource_definitions[]? | select(.name==$rd) | .layer_data[]?.type ),
        ( .[]? | select(.name==$rd) | .layer_data[]?.type ),
        # golinstor `--machine-readable` v0 wraps the apiserver array in
        # an outer array → `[[{name,layer_data:[{type}]}]]`. The earlier
        # probes only reach one level; add the double-array path.
        ( .[0][]? | select(.name==$rd) | .layer_data[]?.type )
    ]
    | flatten
    | map(ascii_upcase)
    | unique
    | .[]' <<<"$mr" | sort -u | tr '\n' ',' | sed 's/,$//')
echo "   layer_data.type = '${layer_types}'"

if [[ -z "$layer_types" ]]; then
    echo "FAIL (Bug 349): rd l machine-readable layer_data[].type empty for $RD" >&2
    echo "----- raw machine-readable response -----" >&2
    echo "$mr" | jq '.' >&2 || echo "$mr" >&2
    exit 1
fi
for want in DRBD STORAGE; do
    if ! grep -qE "(^|,)${want}(,|$)" <<<"$layer_types"; then
        echo "FAIL (Bug 349): layer_data missing '${want}' (got '$layer_types')" >&2
        exit 1
    fi
done

# --- plain-text Layers column check ---------------------------------------
echo ">> [Bug 349] plain-text 'rd l' Layers column must show DRBD,STORAGE for $RD"
plain=$("${LCTL[@]}" resource-definition list 2>/dev/null || echo "")
# The row for our RD; tolerate any column ordering by grepping the
# RD name out and inspecting that line only.
rd_row=$(grep -F " $RD " <<<"$plain" || echo "")
if [[ -z "$rd_row" ]]; then
    # Fall back to a relaxed match (column-aligned tables may have
    # different surrounding whitespace).
    rd_row=$(grep -E "(^|[[:space:]│┊])${RD}([[:space:]│┊]|$)" <<<"$plain" || echo "")
fi
if [[ -z "$rd_row" ]]; then
    echo "FAIL (Bug 349): RD $RD missing from \`linstor rd l\` output" >&2
    echo "$plain" >&2
    exit 1
fi
echo "   row: $rd_row"

if ! grep -qE 'DRBD' <<<"$rd_row"; then
    echo "FAIL (Bug 349): rd l Layers column does not contain 'DRBD' for $RD" >&2
    echo "   row: $rd_row" >&2
    echo "----- full linstor rd l -----" >&2
    echo "$plain" >&2
    exit 1
fi
if ! grep -qE 'STORAGE' <<<"$rd_row"; then
    echo "FAIL (Bug 349): rd l Layers column does not contain 'STORAGE' for $RD" >&2
    echo "   row: $rd_row" >&2
    echo "----- full linstor rd l -----" >&2
    echo "$plain" >&2
    exit 1
fi

echo ">> rd-l-layers-column OK (Bug 349 pinned: rd l Layers column shows DRBD,STORAGE)"
