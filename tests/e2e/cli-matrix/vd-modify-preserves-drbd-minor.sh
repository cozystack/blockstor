#!/usr/bin/env bash
#
# usage: vd-modify-preserves-drbd-minor.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 433 (P2 correctness+availability): a legal,
# in-bounds VD-scoped modify (`vd set-size` grow or `vd set-property`)
# must NOT change the per-volume DRBDMinor — the /dev/drbd<N> device
# identity.
#
# The pre-fix store round-trips the inline VolumeDefinition through
# wireToCRDVD on the VD-scoped write paths (Update /
# PatchVolumeDefinitionSpec); the wire shape carries no DRBDMinor, so the
# round-trip zeroed it. In isolation the allocator re-picks the same
# value (self-heals), but once a LOWER minor has been freed by routine RD
# churn the allocator hands the resized volume that lower minor instead —
# a permanent device-identity change on a live volume, driven purely by a
# modify.
#
# Scenario (minor allocation is lowest-free + cluster-scoped, so this is
# robust to the stand's starting minor without hard-coding values):
#
#   1. rd-a c + vd c 1G + r c (grabs the lowest free minor Ma).
#   2. rd-b c + vd c 1G + r c (grabs the next minor Mb > Ma — rd-b's
#      stable device identity). Capture Mb and rd-b's /dev/drbdN.
#   3. rd-a d — frees the LOWER minor Ma, which the pre-fix reheal would
#      pull rd-b down to.
#   4. linstor vd s rd-b 0 2G   → assert rd-b's DRBDMinor AND device path
#      stay == Mb over a settle window (pre-fix: flips to Ma).
#   5. linstor vd set-property rd-b 0 Aux/... x → assert the same.
#   6. Cleanup; assert_no_orphans.
#
# If Bug 433 regressed, step 4 or 5 catches the minor moving off Mb (to
# the freed lower Ma, or transiently to nil) within the settle window.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

linstor_cli_setup

POOL="${POOL:-lvm-thin}"
RD_A="cli-matrix-minor-a"
RD_B="cli-matrix-minor-b"
SIZE_2G_KIB=2097152

cleanup() {
    [[ -n "${RD_B:-}" ]] && delete_rd "$RD_B" 2>/dev/null || true
    [[ -n "${RD_A:-}" ]] && delete_rd "$RD_A" 2>/dev/null || true
    [[ -n "${RD_B:-}" ]] && assert_no_orphans "$RD_B"
    [[ -n "${RD_A:-}" ]] && assert_no_orphans "$RD_A"
}
trap 'cleanup; linstor_cli_teardown' EXIT

# rd_vol0_minor reads the authoritative per-volume DRBDMinor off the RD
# spec (nil/unset reads as "").
rd_vol0_minor() {
    local rd=$1
    kubectl get resourcedefinition "$rd" \
        -o jsonpath='{.spec.volumeDefinitions[0].drbdMinor}' 2>/dev/null || echo ""
}

# wait_rd_vol0_minor blocks until the controller has allocated a non-empty
# minor for rd vol0, then echoes it.
wait_rd_vol0_minor() {
    local rd=$1 t=${2:-120} m deadline
    deadline=$(( $(date +%s) + t ))
    while (( $(date +%s) < deadline )); do
        m=$(rd_vol0_minor "$rd")
        if [[ -n "$m" ]]; then echo "$m"; return 0; fi
        sleep 2
    done
    echo "FAIL: $rd vol0 DRBDMinor never allocated within ${t}s" >&2
    exit 1
}

# resource_vol0_device resolves rd@node's volume-0 /dev/drbdN identity
# from the Resource CRD status (devicePath, falling back to drbdMinor).
resource_vol0_device() {
    local rd=$1 node=$2 dev minor
    dev=$(kubectl get resource "${rd}.${node}" \
        -o jsonpath='{.status.volumes[0].devicePath}' 2>/dev/null || echo "")
    if [[ -z "$dev" ]]; then
        minor=$(kubectl get resource "${rd}.${node}" \
            -o jsonpath='{.status.drbdMinor}' 2>/dev/null || echo "")
        [[ -n "$minor" ]] && dev="/dev/drbd${minor}"
    fi
    echo "$dev"
}

# assert_minor_stable holds for a settle window and FAILs the instant the
# minor or device path drifts off the captured identity — catching both a
# transient wipe-to-nil and a reheal-to-a-different-minor.
assert_minor_stable() {
    local rd=$1 node=$2 want_minor=$3 want_dev=$4 label=$5 deadline m d
    deadline=$(( $(date +%s) + 12 ))
    while (( $(date +%s) < deadline )); do
        m=$(rd_vol0_minor "$rd")
        if [[ "$m" != "$want_minor" ]]; then
            echo "FAIL (Bug 433): $label CHANGED $rd vol0 DRBDMinor: '$want_minor' -> '$m'" >&2
            echo "      The per-volume DRBDMinor is the /dev/drbd<N> device identity; a legal" >&2
            echo "      VD-scoped modify must never move it. wireToCRDVD dropped it on the" >&2
            echo "      write-back — the fix carries it across via wireToCRDVDPreserving." >&2
            exit 1
        fi
        d=$(resource_vol0_device "$rd" "$node")
        if [[ -n "$want_dev" && -n "$d" && "$d" != "$want_dev" ]]; then
            echo "FAIL (Bug 433): $label CHANGED $rd vol0 device identity: '$want_dev' -> '$d'" >&2
            exit 1
        fi
        sleep 2
    done
    echo "   $label: $rd vol0 minor stable at '$want_minor' (dev '$want_dev') OK"
}

echo "============================================================"
echo ">> vd-modify-preserves-drbd-minor (Bug 433) :: POOL=$POOL"
echo "============================================================"

echo ">> pre-flight: $POOL on >=1 node"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 1 )); then
    echo "SKIP ($POOL): pool not on any node (got $ok_nodes)"
    exit 0
fi
N1=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | first // ""' <<<"$sp_json" 2>/dev/null || echo "")
if [[ -z "$N1" ]]; then
    echo "SKIP ($POOL): could not resolve a diskful node"
    exit 0
fi
echo ">> target node: $N1"

# ---- rd-a grabs the lowest free minor Ma ----------------------------------
echo ">> rd-a: rd c + vd c 1G + r c $N1"
_out=$("${LCTL[@]}" resource-definition create "$RD_A" 2>&1) \
    || { echo "FAIL: rd c $RD_A: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" volume-definition create "$RD_A" 1G 2>&1) \
    || { echo "FAIL: vd c $RD_A 1G: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N1" "$RD_A" --storage-pool="$POOL" 2>&1) \
    || { echo "FAIL: r c $N1 $RD_A: $_out" >&2; exit 1; }
wait_disk_state "$RD_A" "$N1" "UpToDate" 240 0
MINOR_A=$(wait_rd_vol0_minor "$RD_A")
echo ">> rd-a vol0 minor Ma=$MINOR_A"

# ---- rd-b grabs the next minor Mb (> Ma) — its stable identity ------------
echo ">> rd-b: rd c + vd c 1G + r c $N1"
_out=$("${LCTL[@]}" resource-definition create "$RD_B" 2>&1) \
    || { echo "FAIL: rd c $RD_B: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" volume-definition create "$RD_B" 1G 2>&1) \
    || { echo "FAIL: vd c $RD_B 1G: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N1" "$RD_B" --storage-pool="$POOL" 2>&1) \
    || { echo "FAIL: r c $N1 $RD_B: $_out" >&2; exit 1; }
wait_disk_state "$RD_B" "$N1" "UpToDate" 240 0
MINOR_B=$(wait_rd_vol0_minor "$RD_B")
DEV_B=$(resource_vol0_device "$RD_B" "$N1")
echo ">> rd-b vol0 minor Mb=$MINOR_B dev=$DEV_B (stable device identity)"

# Sanity: we need Ma < Mb so freeing Ma creates a lower-free minor the
# pre-fix reheal would pull rd-b down to. If not (dirty range), the
# scenario can't reproduce the divergence — skip rather than false-pass.
if ! [[ "$MINOR_A" =~ ^[0-9]+$ && "$MINOR_B" =~ ^[0-9]+$ ]] || (( MINOR_A >= MINOR_B )); then
    echo "SKIP: need Ma < Mb to free a lower minor (got Ma=$MINOR_A Mb=$MINOR_B); dirty minor range"
    exit 0
fi

# ---- free the lower minor Ma via rd-a delete ------------------------------
echo ">> rd-a delete — frees the lower minor Ma=$MINOR_A"
delete_rd "$RD_A"
RD_A=""   # deleted; keep cleanup from re-deleting/orphan-checking it

# ---- legal grow on rd-b vol0: minor MUST stay Mb --------------------------
echo ">> linstor vd s $RD_B 0 2G"
"${LCTL[@]}" volume-definition set-size "$RD_B" 0 2G >/dev/null
wait_vd_size "$RD_B" 0 "$SIZE_2G_KIB" 60
assert_minor_stable "$RD_B" "$N1" "$MINOR_B" "$DEV_B" "\`vd set-size\` grow"

# ---- legal set-property on rd-b vol0: minor MUST stay Mb ------------------
echo ">> linstor vd set-property $RD_B 0 Aux/bug433-probe x"
"${LCTL[@]}" volume-definition set-property "$RD_B" 0 Aux/bug433-probe x >/dev/null
assert_minor_stable "$RD_B" "$N1" "$MINOR_B" "$DEV_B" "\`vd set-property\`"

echo ">> vd-modify-preserves-drbd-minor (Bug 433) OK"
cleanup
trap 'linstor_cli_teardown' EXIT
echo ">> vd-modify-preserves-drbd-minor COMPLETE"
