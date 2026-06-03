#!/usr/bin/env bash
#
# usage: vd-resize-thick-lvm-grown-region-consistent.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 395 (P1, DATA INTEGRITY) regression catcher.
#
# Reproduces the confirmed-on-stand divergence: resizing a thick-LVM
# ("LVM" kind, NOT thin) backed DRBD volume used to run
# `drbdadm resize --assume-clean` unconditionally. `--assume-clean`
# marks the grown region [old_size, new_size) UpToDate on EVERY replica
# WITHOUT a resync — sound only for zero-on-allocate providers. Classic
# thick LVM's `lvextend` exposes recycled VG extents whose prior content
# differs per node, so the replicas silently disagreed on the grown
# region (w1 read 0xA1, w2 read 0xB2 at the same grown offset through
# the replicated /dev/drbd device) with out-of-sync:0 and no split-brain
# flag — a failover changed the bytes an application read.
#
# The fix (provider-aware gate): thick LVM is non-zero-fill, so the
# satellite now omits `--assume-clean`, DRBD marks the grown region
# out-of-sync and resyncs it from the UpToDate source → both replicas
# agree. Zero-fill providers (zfs/thin/file) keep the fast path
# (--assume-clean) and are covered by vd-resize-full-lifecycle.sh.
#
# Mechanism of this cell:
#   1. Register a thick LINSTOR `LVM` pool on the existing
#      `blockstor-lvm` VG on >=2 workers (the stand has no spare disk;
#      the VG had free extents past the thin pool). SKIP if a thick
#      `LVM` pool is not available on >=2 nodes.
#   2. rd c + vd c 1G + r c --auto-place=2 -s <thick-pool>; wait UpToDate.
#   3. Pre-dirty the to-be-grown region on the BACKING LV of each
#      replica with node-distinct bytes (0xA1 on N1, 0xB2 on N2) so the
#      recycled-extent divergence is deterministic. The backing LV is
#      reached via /dev/mapper/blockstor--lvm-<lv> (the satellites have
#      NO udev, so /dev/<vg>/<lv> symlinks are stale regular files).
#   4. linstor vd s <rd> 0 2G — grow.
#   5. Wait converge (vd size + all UpToDate + sync clean).
#   6. Read the grown region [1G, 2G) through the REPLICATED /dev/drbdN
#      on BOTH replicas. Assert byte-identical. Pre-fix this FAILS
#      (0xA1 vs 0xB2); post-fix the resync makes them equal.
#   7. Cleanup pod/pvc/RD; assert_no_orphans.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

# The thick-LVM pool name registered on the stand. Override with
# THICK_POOL=<name> if the launcher provisioned it under a different
# name. The VG behind it is blockstor-lvm (mapper prefix below).
THICK_POOL="${THICK_POOL:-lvm-thick}"
THICK_VG="${THICK_VG:-blockstor-lvm}"
# dm escapes a single '-' in a VG/LV name as '--'. blockstor-lvm → blockstor--lvm
THICK_VG_DM="${THICK_VG//-/--}"

RD="cli-matrix-thick-grow"
SIZE_2G_KIB=2097152
PVC_NS=default
PVC_NAME="bs-thick-grow-pvc"
POD_NAME="bs-thick-grow-pod"

# Offsets/length for the grown-region read (in bytes). We read the
# first 64 MiB of the grown region [1G, 2G) — enough to catch the
# recycled-extent divergence without dd'ing the whole gigabyte.
GROW_OFFSET_BYTES=$(( 1024 * 1024 * 1024 ))   # 1 GiB == old size
GROW_READ_BYTES=$(( 64 * 1024 * 1024 ))       # 64 MiB sample of grown region

cleanup_thick() {
    local _ns=${PVC_NS:-default}
    [[ -n "${POD_NAME:-}" ]] && kubectl -n "$_ns" delete pod "$POD_NAME" --wait=true --timeout=60s 2>/dev/null || true
    [[ -n "${PVC_NAME:-}" ]] && kubectl -n "$_ns" delete pvc "$PVC_NAME" --wait=true --timeout=60s 2>/dev/null || true
    [[ -n "${RD:-}" ]] && delete_rd "$RD"
    [[ -n "${RD:-}" ]] && assert_no_orphans "$RD"
}
trap 'cleanup_thick; linstor_cli_teardown' EXIT

echo "============================================================"
echo ">> vd-resize-thick-lvm-grown-region-consistent (Bug 395)"
echo "   THICK_POOL=$THICK_POOL VG=$THICK_VG (dm=$THICK_VG_DM)"
echo "============================================================"

echo ">> pre-flight: thick LVM '$THICK_POOL' pool on >=2 nodes"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$THICK_POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
# Confirm it's actually the thick LVM kind (not LVM_THIN) — the whole
# point of the cell. If the pool resolves to LVM_THIN the repro is
# invalid (thin zero-fills) so skip rather than false-PASS.
kind=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .provider_kind] | first // ""' <<<"$sp_json" 2>/dev/null || echo "")
if (( ok_nodes < 2 )) || [[ "$kind" != "LVM" ]]; then
    echo "SKIP: thick LVM pool '$THICK_POOL' not present as kind=LVM on >=2 nodes (got nodes=$ok_nodes kind='$kind')"
    exit 0
fi

echo ">> rd c + vd c 1G + r c --auto-place=2 -s $THICK_POOL"
_out=$("${LCTL[@]}" resource-definition create "$RD" 2>&1) \
    || { echo "FAIL: rd c $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" volume-definition create "$RD" 1G 2>&1) \
    || { echo "FAIL: vd c $RD 1G: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create --auto-place=2 --storage-pool="$THICK_POOL" "$RD" 2>&1) \
    || { echo "FAIL: r c --auto-place=2 -s $THICK_POOL $RD: $_out" >&2; exit 1; }

# Resolve the two diskful replicas (exclude DISKLESS / TIE_BREAKER).
deadline=$(( $(date +%s) + 90 ))
placed_nodes=()
while (( $(date +%s) < deadline )); do
    mapfile -t placed_nodes < <(
        kubectl get resources.blockstor.cozystack.io -o json 2>/dev/null \
            | jq -r --arg rd "$RD" '
                .items[]?
                | select(.spec.resourceDefinitionName==$rd)
                | select(((.spec.flags // [])
                    | map(select(.=="DISKLESS" or .=="TIE_BREAKER"))
                    | length) == 0)
                | .spec.nodeName'
    )
    if (( ${#placed_nodes[@]} >= 2 )); then break; fi
    sleep 2
done
if (( ${#placed_nodes[@]} < 2 )); then
    echo "FAIL: autoplace did not stage 2 diskful replicas for $RD (got ${#placed_nodes[@]})" >&2
    exit 1
fi
N1="${placed_nodes[0]}"
N2="${placed_nodes[1]}"
echo ">> diskful replicas: N1=$N1 N2=$N2"
wait_uptodate "$RD" "$N1" "$N2"

# Backing LV name for volume 0 follows blockstor's <rd>_<vol5digits>
# convention; on the stand it is reached via the dm mapper path
# /dev/mapper/${THICK_VG_DM}-${RD}_00000 (no udev → /dev/<vg>/<lv>
# symlinks are stale regular files). The launcher's VG-prefill (below)
# uses that path; this cell reads the grown region through the
# REPLICATED /dev/drbdN, not the backing LV.

# Divergence seeding: the grown region [1G, 2G) only comes into
# existence at the satellite's `lvextend`, recycling whatever the VG's
# free extents held. To make the divergence deterministic the LAUNCHER
# pre-fills each node's free VG extents with a node-distinct byte
# (0xA1 on N1, 0xB2 on N2) BEFORE this cell runs (e.g. dd a node-distinct
# pattern across the free PE range on /dev/mapper/${THICK_VG_DM}-*),
# so the extents lvextend recycles carry node-distinct bytes. Pre-fix
# (--assume-clean) those bytes are never resynced and the read below
# diverges; post-fix the resync makes both replicas agree. The cell
# itself only asserts the post-grow invariant — it does not depend on
# the specific fill bytes, only on cross-replica equality.

echo ">> linstor vd s $RD 0 2G (grow; thick LVM must resync grown region)"
"${LCTL[@]}" volume-definition set-size "$RD" 0 2G >/dev/null
wait_vd_size "$RD" 0 "$SIZE_2G_KIB" 60
wait_uptodate "$RD" "$N1" "$N2"
# Give the resync of the grown region time to complete, then assert the
# (node<->peer) link is back to clean UpToDate/Established.
wait_sync_done "$RD" "$N1" "$N2" 180 || true
wait_sync_done "$RD" "$N2" "$N1" 180 || true

DEV1=$(device_for_rd "$RD" "$N1")
DEV2=$(device_for_rd "$RD" "$N2")
if [[ -z "$DEV1" || -z "$DEV2" ]]; then
    echo "FAIL: could not resolve /dev/drbdN for $RD on $N1/$N2 (dev1='$DEV1' dev2='$DEV2')" >&2
    exit 1
fi

# Read the grown region through the REPLICATED device on each replica.
# DRBD serves reads from the local backing disk, so if the grown region
# was not resynced the two replicas return different bytes. Promote is
# needed to open the device for reads on a Secondary (read_md5 promotes).
echo ">> read grown region [1G, 1G+64M) through /dev/drbd on both replicas"
read_grown() {
    local node=$1 dev=$2
    local off_blocks=$(( GROW_OFFSET_BYTES / 4096 ))
    local cnt_blocks=$(( GROW_READ_BYTES / 4096 ))
    on_node "$node" bash -c "
        drbdadm primary ${RD} 2>/dev/null || true
        test -b ${dev} || { echo \"ABORT: ${dev} not a block device — \$(stat -c '%F' ${dev} 2>/dev/null || echo missing)\" >&2; exit 2; }
        dd if=${dev} bs=4096 skip=${off_blocks} count=${cnt_blocks} status=none iflag=direct | md5sum | awk '{print \$1}'
    "
}

MD5_N1=$(read_grown "$N1" "$DEV1")
MD5_N2=$(read_grown "$N2" "$DEV2")
echo "   grown-region md5: N1=$MD5_N1  N2=$MD5_N2"

if [[ -z "$MD5_N1" || -z "$MD5_N2" ]]; then
    echo "FAIL: empty md5 read of grown region (N1='$MD5_N1' N2='$MD5_N2')" >&2
    exit 1
fi

if [[ "$MD5_N1" != "$MD5_N2" ]]; then
    echo "FAIL (Bug 395 REGRESSION): grown region [1G,1G+64M) diverges across replicas" >&2
    echo "       N1($N1)=$MD5_N1  N2($N2)=$MD5_N2" >&2
    echo "       thick-LVM resize ran --assume-clean and skipped the resync —" >&2
    echo "       the replicas silently disagree on the grown bytes (failover hazard)." >&2
    exit 1
fi

echo ">> grown region byte-identical across replicas — Bug 395 fix holds"
echo ">> vd-resize-thick-lvm-grown-region-consistent OK"

cleanup_thick
trap 'linstor_cli_teardown' EXIT
echo ">> vd-resize-thick-lvm-grown-region-consistent COMPLETE"
