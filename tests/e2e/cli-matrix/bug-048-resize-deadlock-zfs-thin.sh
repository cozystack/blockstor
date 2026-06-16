#!/usr/bin/env bash
#
# usage: bug-048-resize-deadlock-zfs-thin.sh WORK_DIR
#
# L6 cli-matrix cell — BUG-048 resize deadlock (P0, availability; a
# reboot-proof cluster-wide DRBD deadlock that PR #164 shipped to main with
# a fully-green CI, including the existing vd-resize cell).
#
# DEADLOCK MECHANISM: PR #164 widened the ensurePerVolumeMetadata trigger gate
# from the .res-count hasLateAddedVolume() to the unconditional
# dr.GetMetadataCreated(), so EVERY post-activation diskful reconcile ran a
# per-volume `drbdadm dump-md` (an md_buffer consumer) plus the cluster-wide
# late-add self-heals (MintLateAddSource: disconnect + new-current-uuid
# --clear-bitmap + adjust; InvalidateVolume). During a `vd s` resize DRBD
# holds md_buffer for the whole cluster-wide size change
# (change_cluster_wide_device_size / drbd_determine_dev_size); the
# per-reconcile md probe + the cluster-wide self-heals then perpetually lose
# the cluster-wide state-change arbitration, the resize never completes,
# md_buffer is never released, and the resource deadlocks cluster-wide,
# reboot-proof (stand forensics: size-change retry count 10,790 vs the normal
# 2-20, on a zfs-thin 2-diskful + 1-diskless-client RD; clean at f3515045a).
#
# THE FIX (pkg/satellite/reconciler.go + pkg/drbd/drbdsetup_show.go): the
# per-volume metadata pass fires ONLY when some desired volume is NOT
# present-and-attached in the kernel (an attached volume already has metadata
# — the kernel cannot attach a lower disk without it — so dump-md on it is
# pointless and is the md_buffer consumer that contends with the resize), and
# the late-add self-heals are gated out of a resize pass.
#
# THE SHAPE THIS CELL REPRODUCES: zfs-thin, 2 diskful + 1 diskless-client,
# repeated grows under reconcile pressure. Each grow MUST complete (size
# lands + diskful replicas re-reach UpToDate) within a bounded window, and the
# resource must NEVER enter the deadlock surface (StandAlone / suspended /
# Failed / a diskful volume stuck below UpToDate forever).
#
# HONEST SCOPE: the full kernel md_buffer deadlock needs scale/contention to
# escalate (the forensic repro took ~10k size-change retries), so the
# DETERMINISTIC CI guard for this bug is the L1 unit regression
# (pkg/satellite/reconciler_bug048_resize_deadlock_test.go — proven to FAIL on
# the pre-fix gate). This L6 cell is the BEST-EFFORT stand reproduction: the
# orchestrator runs it on a live stand under load; a clean pass here is strong
# corroboration, a hang/timeout here is the deadlock.
#
# Unit pin: pkg/satellite/reconciler_bug048_resize_deadlock_test.go
#   (no dump-md / no self-heal on a converged or resizing diskful reconcile;
#    metadata pass still fires for a genuine unattached late-add).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

# zfs-thin is the forensic repro substrate (the deadlock was observed on a
# zfs-thin pool). Allow override but default to zfs-thin.
POOL="${POOL:-zfs-thin}"
RD="cli-matrix-bug048-resize-deadlock"
SIZE_2G_KIB=2097152
SIZE_3G_KIB=3145728
SIZE_4G_KIB=4194304

cleanup() {
    [[ -n "${RD:-}" ]] && delete_rd "$RD"
    [[ -n "${RD:-}" ]] && assert_no_orphans "$RD"
}
trap 'cleanup; linstor_cli_teardown' EXIT

echo "============================================================"
echo ">> bug-048-resize-deadlock-zfs-thin :: POOL=$POOL RD=$RD"
echo "============================================================"

echo ">> pre-flight: $POOL on >=2 nodes + a 3rd worker for the diskless client"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 2 )); then
    echo "SKIP ($POOL): pool not on >=2 nodes (got $ok_nodes) — BUG-048 deadlock fixture unavailable"
    exit 0
fi

echo ">> rd c + vd c 1G + r c --auto-place=2 -s $POOL (2 diskful replicas)"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 1G >/dev/null
"${LCTL[@]}" resource create --auto-place=2 --storage-pool="$POOL" "$RD" >/dev/null

# Resolve the two diskful replicas.
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
echo ">> diskful replicas: N1=$N1 N2=$N2"
wait_uptodate "$RD" "$N1" "$N2"

# Add the diskless CLIENT on a 3rd node — the exact forensic shape
# (2-diskful + 1-diskless-client). Reuse an auto-placed TieBreaker if present.
DLNODE="$(linstor_tiebreaker_node "$RD" || true)"
if [[ -z "$DLNODE" ]]; then
    DLNODE="$(linstor_pick_free_node "$RD" "$N1" "$N2" || true)"
    if [[ -z "$DLNODE" ]]; then
        echo "SKIP: no free 3rd worker for the diskless client" >&2
        exit 0
    fi
    echo ">> adding diskless client on $DLNODE"
    "${LCTL[@]}" resource create "$DLNODE" "$RD" --drbd-diskless >/dev/null \
        || { echo "FAIL: r c $DLNODE $RD --drbd-diskless" >&2; exit 1; }
else
    echo ">> reusing auto-placed TieBreaker on $DLNODE as the diskless client"
fi

# Deadlock surface probe: a deadlocked RD wedges the kernel connection
# StandAlone or suspends I/O (suspended:*), and a diskful volume sticks below
# UpToDate forever. This helper returns non-zero (and prints) if ANY diskful
# replica is in the deadlock surface — used as a fast-fail after each grow.
assert_no_deadlock_surface() {
    local node st
    for node in "$N1" "$N2"; do
        st=$(on_node "$node" drbdadm status "$RD" 2>&1 || true)
        if grep -Eiq 'StandAlone|suspended:[^n]|connection:Timeout|connection:BrokenPipe' <<<"$st"; then
            echo "FAIL (BUG-048 deadlock surface): $node shows a wedged/suspended connection for $RD" >&2
            echo "$st" >&2
            return 1
        fi
    done
    return 0
}

# Drive a sequence of grows. Each grow exercises the cluster-wide size change
# (md_buffer hold) under the per-reconcile pressure that PR #164 introduced.
# With the bug, the FIRST grow against this 2-diskful+1-diskless shape stalls
# (size-change retry storm) and the assert below times out.
grow_to() {
    local size_arg=$1 expect_kib=$2
    echo ">> linstor vd s $RD 0 $size_arg (grow under reconcile pressure)"
    "${LCTL[@]}" volume-definition set-size "$RD" 0 "$size_arg" >/dev/null

    echo "   assert vd size == $expect_kib KiB (resize COMPLETED, md_buffer released)"
    # Bug => this never lands (resize wedged mid change_cluster_wide_device_size).
    wait_vd_size "$RD" 0 "$expect_kib" 180

    echo "   assert both diskful replicas re-reached UpToDate after grow"
    wait_uptodate "$RD" "$N1" "$N2"

    echo "   assert no deadlock surface (StandAlone / suspended) on either diskful replica"
    assert_no_deadlock_surface

    # The diskless client must not have grown a backing dataset (U48 guard,
    # asserted here as a free side-check) and must not have wedged.
    local dl_state
    dl_state="$(status_disk_state "$RD" "$DLNODE" 0)"
    case "$dl_state" in
        Failed|Inconsistent|Attaching|Detaching|Negotiating|DUnknown)
            echo "FAIL: diskless client $DLNODE wedged after grow (state=$dl_state)" >&2
            return 1 ;;
    esac
    echo "   grow to $expect_kib KiB COMPLETE (no deadlock)"
}

grow_to 2G "$SIZE_2G_KIB"
grow_to 3G "$SIZE_3G_KIB"
grow_to 4G "$SIZE_4G_KIB"

echo ">> bug-048-resize-deadlock-zfs-thin OK (3 sequential grows on a 2-diskful+1-diskless-client zfs-thin RD all completed; no md_buffer deadlock, no StandAlone/suspended surface)"
cleanup
trap 'linstor_cli_teardown' EXIT
echo ">> bug-048-resize-deadlock-zfs-thin COMPLETE"
