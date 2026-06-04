#!/usr/bin/env bash
#
# usage: rg-delete-with-rds-rejected.sh WORK_DIR
#
# L6 cli-matrix cell — corner-case E3 (deletion semantics + escape).
#
# UG9 linstor-administration.adoc ~1403-1429 documents:
#
#   1. `rg delete <rg>` is REFUSED while any ResourceDefinition still
#      references it. Upstream raises FAIL_EXISTS_RSC_DFN with the text
#      "Cannot delete resource group '<rg>' because it has existing
#      resource definitions." There is NO `--force`.
#
#   2. The escape is to REASSIGN the RD to a different group:
#      `rd modify <rd> --resource-group <other>`. After the reassign the
#      RD starts inheriting the NEW group's properties retroactively
#      (inheritance is a live reference, not a create-time copy), and the
#      now-empty original group can be deleted.
#
# This cell pins both on the live operator-CLI path:
#   a) rg c src_rg (with a marker property), rg c dst_rg (different marker),
#      rd c under src_rg.
#   b) rg d src_rg → non-zero, error names "existing resource definitions",
#      src_rg still present.
#   c) rd modify <rd> --resource-group dst_rg → exit 0.
#   d) rd lp <rd> now surfaces dst_rg's inherited marker (retroactive
#      inheritance), NOT src_rg's.
#   e) rg d src_rg → exit 0 (now empty).
#
# Why a dedicated cell: the rejection guard
# (pkg/rest/resource_groups.go::refuseRGDeleteIfReferenced) and the
# retroactive-inheritance read path
# (pkg/rest/effective_props.go::effectivePropsForRD) are separate code
# paths; a regression in either silently breaks the documented escape
# hatch operators rely on to retire a resource group.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

linstor_cli_setup

SRC_RG=cce-rg-src
DST_RG=cce-rg-dst
RD=cce-rg-escape-rd

# Distinct inheritable marker props so we can prove the RD switches which
# group it inherits from after the reassign. Aux/* keys are arbitrary
# operator metadata that inherit Controller→RG→RD without DRBD semantics.
SRC_MARK_KEY="Aux/cce-origin"
SRC_MARK_VAL="src-group"
DST_MARK_KEY="Aux/cce-origin"
DST_MARK_VAL="dst-group"

cleanup() {
    delete_rd "$RD"
    "${LCTL[@]}" resource-group delete "$SRC_RG" 2>/dev/null || true
    "${LCTL[@]}" resource-group delete "$DST_RG" 2>/dev/null || true
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> Phase 1: create src_rg + dst_rg with distinct marker props, rd c under src_rg"
_out=$("${LCTL[@]}" resource-group create "$SRC_RG" 2>&1) \
    || { echo "FAIL: rg c $SRC_RG: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource-group create "$DST_RG" 2>&1) \
    || { echo "FAIL: rg c $DST_RG: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource-group set-property "$SRC_RG" "$SRC_MARK_KEY" "$SRC_MARK_VAL" 2>&1) \
    || { echo "FAIL: rg sp $SRC_RG: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource-group set-property "$DST_RG" "$DST_MARK_KEY" "$DST_MARK_VAL" 2>&1) \
    || { echo "FAIL: rg sp $DST_RG: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource-definition create "$RD" --resource-group "$SRC_RG" 2>&1) \
    || { echo "FAIL: rd c $RD --resource-group $SRC_RG: $_out" >&2; exit 1; }

# Sanity: the RD inherits src_rg's marker before the reassign.
rd_lp() { "${LCTL[@]}" resource-definition list-properties "$RD" 2>/dev/null; }
if ! rd_lp | grep -q "$SRC_MARK_VAL"; then
    echo "FAIL (E3 pre): RD does not inherit src_rg's marker '$SRC_MARK_VAL' before reassign" >&2
    rd_lp >&2 || true
    exit 1
fi
echo "   OK: RD inherits src_rg marker '$SRC_MARK_VAL' before reassign"

# =====================================================================
# Contract 1: rg d src_rg is REFUSED while the RD references it.
# =====================================================================
echo ">> Phase 2: rg d $SRC_RG must be REJECTED (RD still references it)"
set +e
rg_d_out=$("${LCTL[@]}" resource-group delete "$SRC_RG" 2>&1)
rg_d_rc=$?
set -e
echo "$rg_d_out"

if [[ $rg_d_rc -eq 0 ]]; then
    echo "FAIL (E3): rg d $SRC_RG returned 0 despite a referencing RD — guard missing" >&2
    exit 1
fi
if ! grep -qiE 'existing resource definitions' <<<"$rg_d_out"; then
    echo "FAIL (E3): rg d rejection text does not mention 'existing resource definitions': $rg_d_out" >&2
    exit 1
fi
if ! "${LCTL[@]}" resource-group list 2>/dev/null | grep -q "$SRC_RG"; then
    echo "FAIL (E3): src_rg disappeared after a REFUSED rg d" >&2
    exit 1
fi
echo "   OK: rg d refused, src_rg intact"

# =====================================================================
# Contract 2: reassign the RD to dst_rg via rd modify --resource-group.
# =====================================================================
echo ">> Phase 3: rd modify $RD --resource-group $DST_RG (the escape)"
set +e
mv_out=$("${LCTL[@]}" resource-definition modify "$RD" --resource-group "$DST_RG" 2>&1)
mv_rc=$?
set -e
echo "$mv_out"
if [[ $mv_rc -ne 0 ]]; then
    echo "FAIL (E3): rd modify --resource-group $DST_RG returned non-zero ($mv_rc): $mv_out" >&2
    exit 1
fi

# =====================================================================
# Contract 3: retroactive inheritance — RD now inherits dst_rg's marker,
# NOT src_rg's. Inheritance is a live reference (UG9 ~1403-1429).
# =====================================================================
echo ">> Phase 4: rd lp $RD must now inherit dst_rg's marker, not src_rg's"
deadline=$(( $(date +%s) + 30 ))
flipped=false
while (( $(date +%s) < deadline )); do
    lp=$(rd_lp || true)
    if grep -q "$DST_MARK_VAL" <<<"$lp" && ! grep -q "$SRC_MARK_VAL" <<<"$lp"; then
        flipped=true
        break
    fi
    sleep 2
done
if ! $flipped; then
    echo "FAIL (E3): after reassign, rd lp did not flip to dst_rg's marker '$DST_MARK_VAL' (still '$SRC_MARK_VAL'?)" >&2
    rd_lp >&2 || true
    exit 1
fi
echo "   OK: RD retroactively inherits dst_rg marker '$DST_MARK_VAL'"

# =====================================================================
# Contract 4: src_rg is now empty and can be deleted.
# =====================================================================
echo ">> Phase 5: rg d $SRC_RG must now SUCCEED (empty)"
set +e
rg_d2_out=$("${LCTL[@]}" resource-group delete "$SRC_RG" 2>&1)
rg_d2_rc=$?
set -e
echo "$rg_d2_out"
if [[ $rg_d2_rc -ne 0 ]]; then
    echo "FAIL (E3): rg d $SRC_RG still refused after the RD was reassigned away: $rg_d2_out" >&2
    exit 1
fi
if "${LCTL[@]}" resource-group list 2>/dev/null | grep -q "$SRC_RG"; then
    echo "FAIL (E3): src_rg still present after a successful rg d" >&2
    exit 1
fi

echo ">> PASS: rg-delete-with-rds-rejected (E3: rejection + reassign escape + retroactive inheritance)"
