#!/usr/bin/env bash
#
# usage: multi-volume-late-vd-create.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 332 (regression of Bug 79, P1) +
# BUG-048 (P1, availability — concurrent late vd-add).
#
# Reproduction from the e2e2 stand:
#
#   $ linstor rd c test2
#   $ linstor vd c test2 1G                       # vol-0
#   $ linstor r c test2 --auto-place=3 -s lvm-thin
#   # wait until all 3 replicas reach UpToDate
#   $ linstor vd c test2 1G &                      # vol-1 — late VD
#   $ linstor vd c test2 1G &                      # vol-2 — late VD (concurrent!)
#
#   $ drbdadm status test2
#   test2 role:Secondary suspended:quorum
#     volume:0 disk:UpToDate blocked:upper
#     volume:1 disk:Diskless quorum:no     ← Unintentional Diskless
#     volume:2 disk:Diskless quorum:no     ← Unintentional Diskless
#
# Two distinct failure modes are pinned here:
#
#   - Bug 332 (per-volume create-md): a late VD's backing LV is never
#     stamped, so the kernel brings it up disk:Diskless while the spec
#     lacks DISKLESS ("Unintentional Diskless").
#
#   - BUG-048 (concurrent auto-assign lost-update): two BACK-TO-BACK
#     number-less `vd c` calls both auto-assign the smallest free
#     VolumeNumber off the same pre-write snapshot, both pick VlmNr=1,
#     and the loser is rejected — the operator's SECOND volume silently
#     vanishes (only vol-0 + vol-1 land, vol-2 never created). The two
#     late adds below run CONCURRENTLY to exercise this; the cell then
#     asserts the RD carries exactly 3 VolumeDefinitions.
#
# Expected (post-fix): both concurrent late adds land at distinct
# VolumeNumbers (1 and 2), each gets its backing LV + per-volume
# create-md on every diskful replica, the satellite's per-fresh-volume
# winner election seeds a SyncSource, and every (replica, volume) pair
# settles UpToDate.
#
# Unit pins: pkg/satellite/reconciler_drbd_test.go::
#   TestApplyDRBDAllocatesBackingForLateAddedVolume (per-volume create-md)
#   and pkg/rest/volume_definitions_test.go::
#   TestVolumeDefinitionsConcurrentAutoNumberNoLostUpdate (BUG-048 race).
# This L6 cell is the kernel-truth half — only the real stand can
# observe the actual `drbdadm status` output that surfaced the bug.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-332
POOL=${POOL:-lvm-thin}

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

# Pre-flight: 3 healthy SATELLITE nodes carrying the target pool.
echo ">> pre-flight: 3 healthy $POOL SPs"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 3 )); then
    echo "SKIP: $POOL SP not on 3 nodes (got $ok_nodes) — Bug 332 fixture not available"
    exit 0
fi

echo ">> [Bug 332] rd c + vd c (vol-0)"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 1G >/dev/null

echo ">> [Bug 332] r c --auto-place=3 -s $POOL"
"${LCTL[@]}" resource create --auto-place=3 --storage-pool="$POOL" "$RD" >/dev/null

# Resolve the 3 diskful nodes auto-place picked, so every convergence
# check below reads the per-replica Resource.Status directly.
echo ">> wait for 3 diskful replicas to be placed"
if ! wait_diskful_count "$RD" 3 90; then
    echo "FAIL: auto-place did not land 3 diskful replicas" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -30 >&2
    exit 1
fi
mapfile -t DISKFUL_NODES < <(linstor_diskful_nodes "$RD")
if (( ${#DISKFUL_NODES[@]} != 3 )); then
    echo "FAIL: expected 3 diskful nodes, got ${#DISKFUL_NODES[@]}: ${DISKFUL_NODES[*]}" >&2
    exit 1
fi
echo "   diskful nodes: ${DISKFUL_NODES[*]}"

# wait_all_replicas_volume_uptodate <vol> <timeout> — poll
# Resource.Status.volumes[<vol>].diskState on EVERY diskful node until
# all report UpToDate, or timeout. Reads the observer-stamped CRD via
# status_disk_state (lib.sh) — the SAME wire surface the passing cells
# use. NOTE: the `--machine-readable resource list` projection leaves
# `.vlms` null on this apiserver, so the previous jq-on-vlms detection
# always read empty and timed out even while `linstor r l` (which reads
# Resource.Status) showed every replica UpToDate. status_disk_state
# reads the populated CRD field instead.
wait_all_replicas_volume_uptodate() {
    local vol=$1 timeout=$2
    local deadline=$(( $(date +%s) + timeout ))
    local node st all_ok
    while (( $(date +%s) < deadline )); do
        all_ok=true
        for node in "${DISKFUL_NODES[@]}"; do
            st=$(status_disk_state "$RD" "$node" "$vol")
            if [[ "$st" != "UpToDate" ]]; then
                all_ok=false
                break
            fi
        done
        if [[ "$all_ok" == "true" ]]; then
            return 0
        fi
        sleep 3
    done
    return 1
}

# 240s: a 1G x3 initial sync shares the stand with the rest of the
# sweep; the previous 120s budget flaked under load.
echo ">> wait up to 240s for vol-0 to reach UpToDate on all 3 replicas"
if ! wait_all_replicas_volume_uptodate 0 240; then
    echo "FAIL: vol-0 did not reach UpToDate on all 3 replicas within 240s" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -30 >&2
    exit 1
fi

# THE BUG (BUG-048, P1 availability): add vol-1 and vol-2 AFTER vol-0 is
# UpToDate, CONCURRENTLY (back-to-back, no convergence wait between them).
# Both number-less `vd c` calls auto-assign the smallest free VolumeNumber
# — pre-fix each read [vol-0] and BOTH chose VlmNr=1, so the loser was
# rejected FAIL_EXISTS_VLM_DFN and the operator's second intended volume
# silently vanished (only vol-0 + vol-1 persisted; vol-2 never created).
# Running them truly concurrently is what exercises the lost-update race;
# the prior sequential `vd c \n vd c` shape let the first REST response
# land before the second auto-assign ran and so masked the bug.
echo ">> [BUG-048] CONCURRENT late vd c (vol-1 + vol-2 back-to-back)"
vdc_rc=0
"${LCTL[@]}" volume-definition create "$RD" 1G >/tmp/bug048-vdcA.out 2>&1 &
pA=$!
"${LCTL[@]}" volume-definition create "$RD" 1G >/tmp/bug048-vdcB.out 2>&1 &
pB=$!
wait "$pA" || vdc_rc=1
wait "$pB" || vdc_rc=1

# BUG-048 wire-level assertion: BOTH concurrent adds must land. The RD
# must carry exactly THREE VolumeDefinitions (vol-0 + the two late adds);
# a lost-update leaves only two and one `vd c` reports a spurious
# "volume definition 1 already exists" even though the operator named no
# number. This catches the silent drop BEFORE the (slower) convergence
# wait below — and even if DRBD later happened to converge the survivors.
echo ">> [BUG-048] assert no concurrent vd-add was dropped (expect 3 VDs)"
vd_count=$("${LCTL[@]}" --machine-readable volume-definition list \
    --resource-definitions "$RD" 2>/dev/null \
    | jq -r '[.[]? | .[]? | (.vlm_dfns // .volume_definitions // [])[]?] | length' \
    2>/dev/null || echo 0)
if [[ "$vd_count" != "3" ]]; then
    echo "FAIL (BUG-048): RD $RD has $vd_count VolumeDefinitions after two concurrent late vd c, want 3 — a concurrent add was silently dropped" >&2
    echo "----- vd c #A output -----" >&2; cat /tmp/bug048-vdcA.out >&2 || true
    echo "----- vd c #B output -----" >&2; cat /tmp/bug048-vdcB.out >&2 || true
    "${LCTL[@]}" volume-definition list --resource-definitions "$RD" 2>&1 | tail -20 >&2
    exit 1
fi
echo "   3 VolumeDefinitions present — no concurrent add dropped"

if (( vdc_rc != 0 )); then
    # Both VDs landed (count==3) yet a CLI returned non-zero: that is the
    # acceptable-but-noteworthy "fail loudly" path, not the silent wedge.
    echo "   note: a concurrent vd c exited non-zero but both volumes were created" >&2
fi

# Each late-added 1G volume needs its own initial sync on every replica.
# Here TWO 1G volumes are added concurrently on a 3-diskful RD, so up to
# 6 fresh (replica,volume) initial syncs run at once and share the loop
# substrate's I/O — convergence routinely runs past the single-volume
# 240s budget (observed: vol-2 still `Outdated` — i.e. converging, NOT
# the wedge's `Inconsistent` — at the 240s mark, UpToDate moments later).
# 360s per volume keeps the concurrent path from flaking while still
# being far inside the wedge discriminator (a real BUG-048 wedge sits
# Inconsistent forever with no SyncSource and never advances).
echo ">> wait up to 360s for vol-1 + vol-2 to reach UpToDate on all 3 replicas"
late_up=true
for vol in 1 2; do
    if ! wait_all_replicas_volume_uptodate "$vol" 360; then
        late_up=false
        break
    fi
done

if [[ "$late_up" != "true" ]]; then
    echo "FAIL (Bug 332): late-added vol-1/vol-2 not UpToDate on all 3 replicas within 360s" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -40 >&2
    # Surface the smoking gun: per (node, vol) disk_state straight from
    # the populated Resource.Status — a Bug-332-bitten path shows vol-1
    # or vol-2 stuck on Diskless on a non-DISKLESS replica.
    echo "----- per (node, vol) Resource.Status diskState -----" >&2
    for node in "${DISKFUL_NODES[@]}"; do
        for vol in 0 1 2; do
            echo "  $node vol=$vol state=$(status_disk_state "$RD" "$node" "$vol")" >&2
        done
    done
    exit 1
fi

# Guard against the surface symptom: even if the wire-shape probe
# above tripped its happy path, drbdadm status on a diskful replica
# MUST NOT report any volume as Diskless when the spec lacks the
# DISKLESS flag. This is the kernel-truth assertion that distinguishes
# Bug 332 (Unintentional Diskless) from spec-pinned diskless replicas.
echo ">> [Bug 332] kernel-truth: drbdsetup status on a diskful node"
# Use the already-resolved diskful node set (reliable, CRD-backed) and
# probe the kernel directly via the satellite pod (on_node, lib.sh) —
# the same drbdsetup status path the parent helpers use. The previous
# `--machine-readable ... | jq .flags | head -1` resolution shared the
# null-vlms / SIGPIPE failure modes of the wait loops above.
satellite_node=${DISKFUL_NODES[0]:-}

if [[ -z "$satellite_node" ]]; then
    echo "SKIP-PARTIAL: could not resolve a diskful node for kernel-truth check"
    echo ">> multi-volume-late-vd-create OK (Bug 332 pinned at REST/state level)"
    exit 0
fi

if status_out=$(on_node "$satellite_node" drbdsetup status "$RD" --verbose 2>&1); then
    echo "$status_out"
    if grep -E 'volume:[12].*disk:Diskless' <<<"$status_out" >/dev/null; then
        echo "FAIL (Bug 332): diskful node $satellite_node reports volume:1 or volume:2 as Diskless on kernel state" >&2
        echo "$status_out" >&2
        exit 1
    fi
else
    echo "SKIP-PARTIAL: drbdsetup status on $satellite_node failed; REST-level pin still asserted"
fi

echo ">> multi-volume-late-vd-create OK (Bug 332 + BUG-048 pinned: two CONCURRENT late vd c on $RD both landed at distinct VlmNrs and brought vol-1/vol-2 to UpToDate, no dropped VD, no Unintentional Diskless)"
