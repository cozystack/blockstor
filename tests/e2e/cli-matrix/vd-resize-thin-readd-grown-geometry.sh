#!/usr/bin/env bash
#
# usage: vd-resize-thin-readd-grown-geometry.sh WORK_DIR
#
# L6 cli-matrix cell — U389 (P2) re-add-after-grow picks up LIVE geometry.
#
# Upstream LINSTOR report: after a `vd set-size` grow, dropping a replica
# (`r d`) and re-adding it (autoplace) could re-provision the new replica
# at the STALE pre-grow size (a cached VD snapshot), so the re-added
# replica's backing device was smaller than the live volume — DRBD then
# refused to attach it / it could never reach UpToDate.
#
# blockstor derives the new replica's volume size straight from the LIVE
# VolumeDefinition (dispatcher buildVolumes → DesiredVolume.SizeKib =
# vd.SizeKib), so a freshly-staged replica is provisioned at the current
# grown geometry by construction. This cell pins it on the live stand for
# the THIN-LVM provider (the adjacent THICK variant is
# vd-resize-thick-lvm-grown-region-consistent.sh):
#
#   1. rd c + vd c 1G + r c --auto-place=2 -s lvm-thin. Wait UpToDate.
#   2. Grow 1G → 2G. Wait converge (vd size + both UpToDate).
#   3. `r d` one of the two diskful replicas. Wait it's gone.
#   4. Re-add a replica via autoplace (`r c --auto-place=2` tops the
#      count back up onto a fresh node, or explicit r c on a free node).
#   5. Assert the re-added replica:
#        - provisions a backing LV sized to the GROWN 2 GiB (lvs ground
#          truth), NOT the stale 1 GiB
#        - reaches UpToDate (DRBD attaches the correctly-sized lower disk
#          and resyncs the full 2 GiB)
#   6. Cleanup RD; assert_no_orphans.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

POOL="${POOL:-lvm-thin}"
RD="cli-matrix-resize-readd-geometry"
SIZE_2G_KIB=2097152

cleanup() {
    [[ -n "${RD:-}" ]] && delete_rd "$RD"
    [[ -n "${RD:-}" ]] && assert_no_orphans "$RD"
}
trap 'cleanup; linstor_cli_teardown' EXIT

echo "============================================================"
echo ">> vd-resize-thin-readd-grown-geometry (U389) :: POOL=$POOL RD=$RD"
echo "============================================================"

echo ">> pre-flight: $POOL on >=3 nodes (need a free node for the re-add)"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 3 )); then
    echo "SKIP ($POOL): pool not on >=3 nodes (got $ok_nodes) — re-add needs a free node"
    exit 0
fi

echo ">> rd c + vd c 1G + r c --auto-place=2 -s $POOL"
_out=$("${LCTL[@]}" resource-definition create "$RD" 2>&1) \
    || { echo "FAIL: rd c $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" volume-definition create "$RD" 1G 2>&1) \
    || { echo "FAIL: vd c $RD 1G: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create --auto-place=2 --storage-pool="$POOL" "$RD" 2>&1) \
    || { echo "FAIL: r c --auto-place=2 -s $POOL $RD: $_out" >&2; exit 1; }

deadline=$(( $(date +%s) + 90 ))
diskful=()
while (( $(date +%s) < deadline )); do
    mapfile -t diskful < <(linstor_diskful_nodes "$RD")
    if (( ${#diskful[@]} >= 2 )); then break; fi
    sleep 2
done
if (( ${#diskful[@]} < 2 )); then
    echo "FAIL: autoplace did not stage 2 diskful replicas (got ${#diskful[@]})" >&2
    exit 1
fi
N1="${diskful[0]}"
N2="${diskful[1]}"
echo ">> initial diskful replicas: N1=$N1 N2=$N2"
wait_uptodate "$RD" "$N1" "$N2"

# ---- Grow 1G → 2G ---------------------------------------------------------
echo ">> linstor vd s $RD 0 2G"
"${LCTL[@]}" volume-definition set-size "$RD" 0 2G >/dev/null
wait_vd_size "$RD" 0 "$SIZE_2G_KIB" 60
wait_uptodate "$RD" "$N1" "$N2"

# ---- Drop one replica, re-add via autoplace -------------------------------
DROP="$N2"
KEEP="$N1"
echo ">> r d $DROP $RD (drop one diskful replica)"
_out=$("${LCTL[@]}" resource delete "$DROP" "$RD" 2>&1) \
    || { echo "FAIL: r d $DROP $RD: $_out" >&2; exit 1; }

echo ">> wait $DROP's replica is gone"
drop_deadline=$(( $(date +%s) + 90 ))
while (( $(date +%s) < drop_deadline )); do
    if ! kubectl get "resources.blockstor.cozystack.io/${RD}.${DROP}" >/dev/null 2>&1; then
        break
    fi
    sleep 2
done

echo ">> re-add a replica via autoplace (top count back to 2)"
_out=$("${LCTL[@]}" resource create --auto-place=2 --storage-pool="$POOL" "$RD" 2>&1) \
    || { echo "FAIL: re-add r c --auto-place=2 -s $POOL $RD: $_out" >&2; exit 1; }

# Resolve the re-added node: the new diskful replica that is NOT $KEEP.
echo ">> resolve the re-added node"
readd_deadline=$(( $(date +%s) + 120 ))
READD=""
while (( $(date +%s) < readd_deadline )); do
    mapfile -t cur_diskful < <(linstor_diskful_nodes "$RD")
    for n in "${cur_diskful[@]}"; do
        if [[ "$n" != "$KEEP" ]]; then
            READD="$n"
            break
        fi
    done
    if [[ -n "$READD" ]]; then break; fi
    sleep 3
done
if [[ -z "$READD" ]]; then
    echo "FAIL: autoplace did not re-add a second diskful replica" >&2
    exit 1
fi
echo ">> re-added replica on $READD"

# ---- The U389 assertion: re-added LV matches the GROWN size ---------------
echo ">> assert re-added replica $READD backing LV == grown 2 GiB (not stale 1 GiB)"
lv_deadline=$(( $(date +%s) + 120 ))
lv_kib=0
while (( $(date +%s) < lv_deadline )); do
    lv_kib=$(on_node "$READD" bash -c "
        lvs --units k --nosuffix --noheadings -o lv_name,lv_size 2>/dev/null \
            | awk '/${RD}_00000/{gsub(/\..*/,\"\",\$2); print \$2; exit}'
    " 2>/dev/null || echo 0)
    [[ -z "$lv_kib" ]] && lv_kib=0
    if (( lv_kib >= SIZE_2G_KIB )); then break; fi
    sleep 3
done
if (( lv_kib < SIZE_2G_KIB )); then
    echo "FAIL (U389): re-added replica $READD backing LV is ${lv_kib} KiB, want >= 2 GiB" >&2
    echo "      The re-add provisioned a STALE pre-grow size instead of the live" >&2
    echo "      grown VD geometry — the re-added replica can never attach/UpToDate." >&2
    exit 1
fi
echo "   re-added LV = ${lv_kib} KiB (>= 2 GiB grown geometry) OK"

echo ">> assert re-added replica reaches UpToDate (full 2 GiB resync)"
wait_uptodate "$RD" "$KEEP" "$READD"

echo ">> vd-resize-thin-readd-grown-geometry (U389) OK"
cleanup
trap 'linstor_cli_teardown' EXIT
echo ">> vd-resize-thin-readd-grown-geometry COMPLETE"
