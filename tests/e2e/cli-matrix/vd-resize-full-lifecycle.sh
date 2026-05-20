#!/usr/bin/env bash
#
# usage: vd-resize-full-lifecycle.sh WORK_DIR
#
# L6 cli-matrix cell — P0 full volume-resize lifecycle catcher.
#
# Audit gap: luks-resize-encrypted.sh covers `linstor vd s` on a
# [DRBD,LUKS,STORAGE] stack only. The plain [DRBD,STORAGE] resize path
# through real `linstor vd s` — including the operator's view (pod
# attached + PVC.Status.Capacity propagation + lsblk inside the pod)
# and the multi-step grow 1G → 2G → 4G with data preservation — was
# never exercised end-to-end. Per-provider regressions on the
# extend chain (pkg/storage/{lvm,zfs}/*.go) → drbdadm resize
# (pkg/satellite/reconciler.go) → Status size reporting
# (pkg/satellite/controllers/observer.go) would all pass L1-L5 and
# silently flip the operator-visible behaviour.
#
# Steps (mirrors r-full-lifecycle.sh shape):
#   1.  rd c + vd c 1G + r c --auto-place=2 -s lvm-thin (zfs-thin
#       optional block at the tail if SP available).
#   2.  Wait UpToDate on both placed nodes.
#   3.  Attach a pod via PVC referencing the placed RD, write 256 MiB
#       of random data with an md5 anchor captured before any grow.
#   4.  linstor vd s <rd> 0 2G; assert (≤60s):
#         - linstor vd l shows new SizeKib == 2 GiB
#         - On every satellite: backing LV `lvs --units k -o lv_size`
#           grew to ≥ 2 GiB
#         - drbdsetup status shows the new disk size
#         - PVC.Status.Capacity == 2Gi
#         - lsblk inside the pod sees the device at ≥ 2 GiB
#         - md5 over the originally-written 256 MiB region still
#           matches (no data loss)
#   5.  Repeat grow 2G → 4G with the same assertions.
#   6.  Attempt shrink 4G → 1G — MUST exit non-zero and stderr must
#       mention "shrink" / "cannot reduce" / similar; size unchanged.
#   7.  Cleanup pod + pvc + RD; assert_no_orphans.
#
# Optional zfs-thin re-run guarded by SP availability — same fixture,
# different POOL.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

# ---- main scenario, parameterised by storage pool -------------------------

run_resize_lifecycle() {
    local POOL=$1
    local RD="cli-matrix-resize-${POOL//[^a-z0-9]/}"
    # All sizes in KiB so we can compare against `lvs --units k` output
    # and `linstor vd l -o json` SizeKib field directly.
    local SIZE_1G_KIB=1048576
    local SIZE_2G_KIB=2097152
    local SIZE_4G_KIB=4194304
    local PVC_NS=default
    local PVC_NAME="bs-resize-${POOL//[^a-z0-9]/}-pvc"
    local POD_NAME="bs-resize-${POOL//[^a-z0-9]/}-pod"
    local MOUNT_PATH="/data"
    local ANCHOR_FILE="${MOUNT_PATH}/anchor.bin"
    local ANCHOR_BYTES=$(( 256 * 1024 * 1024 ))

    echo "============================================================"
    echo ">> vd-resize-full-lifecycle :: POOL=$POOL RD=$RD"
    echo "============================================================"

    cleanup_resize() {
        # Why: trap fires after run_resize_lifecycle returns, so its
        # `local` vars are out of scope under set -u. Default-expand
        # so cleanup of a half-built state never crashes the cell
        # before the real error is surfaced.
        local _ns=${PVC_NS:-default}
        local _pvc=${PVC_NAME:-}
        local _pod=${POD_NAME:-}
        local _rd=${RD:-}
        [[ -n "$_pod" ]] && kubectl -n "$_ns" delete pod "$_pod" --wait=true --timeout=60s 2>/dev/null || true
        [[ -n "$_pvc" ]] && kubectl -n "$_ns" delete pvc "$_pvc" --wait=true --timeout=60s 2>/dev/null || true
        [[ -n "$_rd" ]] && delete_rd "$_rd"
        [[ -n "$_rd" ]] && assert_no_orphans "$_rd"
    }
    # Per-pool cleanup runs at the end of this function (and on any
    # error via the EXIT trap below).
    trap 'cleanup_resize; linstor_cli_teardown' EXIT

    echo ">> pre-flight: 2 healthy $POOL SPs"
    sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
    ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
    if (( ok_nodes < 2 )); then
        echo "SKIP ($POOL): pool not on >=2 nodes (got $ok_nodes)"
        return 0
    fi

    echo ">> rd c + vd c 1G + r c --auto-place=2 -s $POOL"
    # Why: linstor writes server-side ERROR envelopes to stdout (not
    # stderr), so >/dev/null hides them. Capture and surface on
    # failure so a silent fail like "no eligible nodes" doesn't
    # masquerade as a downstream "autoplace did not stage" assert.
    local _rc_out
    _rc_out=$("${LCTL[@]}" resource-definition create "$RD" 2>&1) \
        || { echo "FAIL: rd c $RD: $_rc_out" >&2; return 1; }
    _rc_out=$("${LCTL[@]}" volume-definition create "$RD" 1G 2>&1) \
        || { echo "FAIL: vd c $RD 1G: $_rc_out" >&2; return 1; }
    _rc_out=$("${LCTL[@]}" resource create --auto-place=2 --storage-pool="$POOL" "$RD" 2>&1) \
        || { echo "FAIL: r c --auto-place=2 -s $POOL $RD: $_rc_out" >&2; return 1; }

    # Resolve the placed pair. --auto-place=2 stages 2 diskful peers
    # + (optionally) a TieBreaker diskless on the 3rd node — filter to
    # diskful only via Spec.Diskful=true so the tiebreaker row doesn't
    # bump the count and force a stale-state FAIL.
    local deadline placed_nodes=()
    deadline=$(( $(date +%s) + 90 ))
    while (( $(date +%s) < deadline )); do
        mapfile -t placed_nodes < <(
            kubectl get resources.blockstor.io.blockstor.io -o json 2>/dev/null \
                | jq -r --arg rd "$RD" '
                    .items[]?
                    | select(.spec.resourceDefinitionName==$rd)
                    | select((.spec.diskful // false)==true)
                    | .spec.nodeName'
        )
        if (( ${#placed_nodes[@]} >= 2 )); then break; fi
        sleep 2
    done
    if (( ${#placed_nodes[@]} < 2 )); then
        echo "FAIL: autoplace did not stage 2 diskful replicas for $RD (got ${#placed_nodes[@]})" >&2
        return 1
    fi
    local N1="${placed_nodes[0]}"
    local N2="${placed_nodes[1]}"
    wait_uptodate "$RD" "$N1" "$N2"

    echo ">> create PVC + pod, write 256 MiB random with md5 anchor"
    # The PVC binds via the existing storage-class machinery — same
    # mechanics as tests/e2e/pvc-lifecycle.sh. We don't care which SC
    # is used as long as it lands on this RD's storage pool; for the
    # operator-visible PVC.Status.Capacity bit we just need a PVC that
    # IS bound to a PV backed by this RD. To avoid coupling to whatever
    # SC the stand has provisioned, we pre-create the PV directly with
    # the CSI driver and bind the PVC to it.
    create_pvc_for_rd "$PVC_NS" "$PVC_NAME" "$RD" 1Gi || {
        echo "SKIP ($POOL): could not bind PVC to existing RD (CSI plumbing not present on stand)"
        return 0
    }
    create_writer_pod "$PVC_NS" "$POD_NAME" "$PVC_NAME" "$MOUNT_PATH" || {
        echo "FAIL: could not start writer pod for $PVC_NAME" >&2
        return 1
    }

    kubectl -n "$PVC_NS" exec "$POD_NAME" -- \
        dd if=/dev/urandom of="$ANCHOR_FILE" bs=1M count=256 status=none conv=fsync
    local md5_pre
    md5_pre=$(pod_md5 "$PVC_NS" "$POD_NAME" "$ANCHOR_FILE")
    echo "   md5_pre=$md5_pre"

    # ---- Grow 1G → 2G ----------------------------------------------------
    echo ">> linstor vd s $RD 0 2G"
    "${LCTL[@]}" volume-definition set-size "$RD" 0 2G >/dev/null
    assert_resize_converged "$RD" 0 "$SIZE_2G_KIB" "$PVC_NS" "$PVC_NAME" "$POD_NAME" "$MOUNT_PATH" \
        "$N1" "$N2" "$md5_pre" "$ANCHOR_FILE" "2Gi"

    # ---- Grow 2G → 4G ----------------------------------------------------
    echo ">> linstor vd s $RD 0 4G"
    "${LCTL[@]}" volume-definition set-size "$RD" 0 4G >/dev/null
    assert_resize_converged "$RD" 0 "$SIZE_4G_KIB" "$PVC_NS" "$PVC_NAME" "$POD_NAME" "$MOUNT_PATH" \
        "$N1" "$N2" "$md5_pre" "$ANCHOR_FILE" "4Gi"

    # ---- Shrink 4G → 1G must fail ---------------------------------------
    echo ">> linstor vd s $RD 0 1G (MUST fail — DRBD cannot shrink past meta)"
    local err_file
    err_file=$(mktemp)
    if "${LCTL[@]}" volume-definition set-size "$RD" 0 1G 2>"$err_file" >/dev/null; then
        echo "FAIL: shrink 4G→1G unexpectedly succeeded — DRBD protocol violation not surfaced" >&2
        cat "$err_file" >&2
        rm -f "$err_file"
        return 1
    fi
    if ! grep -qiE 'shrink|cannot.*(reduce|shrink)|smaller|reduction' "$err_file"; then
        echo "FAIL: shrink rejected but error text does not mention shrink/reduce:" >&2
        cat "$err_file" >&2
        rm -f "$err_file"
        return 1
    fi
    rm -f "$err_file"

    echo ">> verify SizeKib still 4G after rejected shrink"
    local cur_kib
    cur_kib=$(linstor_vd_size_kib "$RD" 0)
    if (( cur_kib != SIZE_4G_KIB )); then
        echo "FAIL: post-shrink-reject SizeKib=$cur_kib != $SIZE_4G_KIB (size mutated by failed call)" >&2
        return 1
    fi

    echo ">> vd-resize-full-lifecycle ($POOL) OK"
    cleanup_resize
    trap 'linstor_cli_teardown' EXIT
}

# Primary: lvm-thin run (P0 nightly gate).
run_resize_lifecycle "${POOL:-lvm-thin}"

# Optional: zfs-thin re-run if a zfs-thin SP exists on ≥2 nodes.
# Same fixture, exercises the ZFS extend path (pkg/storage/zfs/*.go).
if command -v jq >/dev/null 2>&1; then
    zfs_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools zfs-thin 2>/dev/null || echo "[]")
    zfs_ok=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$zfs_json" 2>/dev/null || echo 0)
    if (( zfs_ok >= 2 )); then
        echo ">> zfs-thin SP available on $zfs_ok nodes — running second iteration"
        run_resize_lifecycle zfs-thin
    else
        echo ">> zfs-thin SP not on >=2 nodes (got $zfs_ok) — skipping optional zfs iteration"
    fi
fi

echo ">> vd-resize-full-lifecycle COMPLETE"
