#!/usr/bin/env bash
#
# usage: snap-create-thick-pool-rejected.sh WORK_DIR
#
# L6 cli-matrix cell — corner-case G5 (snapshot create on a thick pool).
#
# Upstream LINSTOR only allows snapshots on snapshot-capable providers
# (thin LVM, ZFS — and FILE_THIN downstream). A `snapshot create` on a
# THICK pool (classic `LVM`, plain thick `FILE`) is refused at the
# controller with FAIL_SNAPSHOTS_NOT_SUPPORTED. blockstor's thick-LVM
# provider CAN technically `lvcreate --snapshot` a 25%ORIGIN COW overlay
# that silently invalidates on overflow (Bug 245 footgun), so the REST
# surface must refuse the create at the API edge.
#
# The dev stand ships only thin pools (lvm-thin, stand=FILE_THIN,
# zfs-thin), so this cell SELF-PROVISIONS a throwaway thick LVM pool on
# a loop device on worker-1, asserts the rejection, and tears it down.
# If the thick pool cannot be provisioned (no loop device, no spare
# disk), the cell SKIPS (exit 0) rather than false-FAIL.
#
# Contract:
#   1. provision a loop-backed thick LVM VG + register a blockstor `LVM`
#      pool (CanSnapshots=False) on worker-1.
#   2. spawn a tiny RD diskful on that thick pool.
#   3. `snapshot create` on it → MUST be rejected (non-zero), error
#      mentions snapshots-not-supported / thin / capability.
#   4. teardown: rd, pool, VG, loop device.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

linstor_cli_setup

N1=$WORKER_1
THICKVG=clim_g5_thickvg
THICKPOOL=cli-matrix-g5-thickpool
RD=cli-matrix-g5-thick
SNAP=snap-g5-1
IMG=/tmp/cli-matrix-g5-thick.img

provisioned=false

teardown_thick() {
    "${LCTL[@]}" snapshot delete "$RD" "$SNAP" 2>/dev/null || true
    delete_rd "$RD"
    "${LCTL[@]}" storage-pool delete "$N1" "$THICKPOOL" 2>/dev/null || true
    if $provisioned; then
        on_node "$N1" bash -c "
            vgremove -f $THICKVG 2>/dev/null || true
            LD=\$(losetup -j $IMG 2>/dev/null | cut -d: -f1)
            [ -n \"\$LD\" ] && losetup -d \"\$LD\" 2>/dev/null || true
            rm -f $IMG
        " 2>/dev/null || true
    fi
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap teardown_thick EXIT

echo ">> [G5] provision throwaway thick LVM VG on $N1 (loop-backed)"
if ! on_node "$N1" bash -c "
    set -e
    truncate -s 1G $IMG
    LD=\$(losetup --find --show $IMG)
    pvcreate -f \"\$LD\" >/dev/null
    vgcreate $THICKVG \"\$LD\" >/dev/null
    vgs $THICKVG >/dev/null
" 2>/dev/null; then
    echo "SKIP (G5): could not provision a loop-backed thick VG on $N1 (no losetup/lvm or no space)"
    provisioned=false
    exit 0
fi
provisioned=true

echo ">> [G5] register thick LVM pool $THICKPOOL on $N1"
if ! _out=$("${LCTL[@]}" storage-pool create lvm "$N1" "$THICKPOOL" "$THICKVG" 2>&1); then
    echo "SKIP (G5): blockstor refused to register a thick LVM pool: $_out"
    exit 0
fi
sleep 3

# Confirm the pool reports CanSnapshots=False (the whole premise of G5).
if "${LCTL[@]}" storage-pool list --storage-pools "$THICKPOOL" 2>/dev/null \
        | grep -qi "True"; then
    echo "note (G5): thick pool unexpectedly reports CanSnapshots=True; continuing — REST gate is authoritative"
fi

echo ">> [G5] spawn tiny RD diskful on the thick pool"
_out=$("${LCTL[@]}" resource-definition create "$RD" 2>&1) \
    || { echo "FAIL: rd c $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" volume-definition create "$RD" 32M 2>&1) \
    || { echo "FAIL: vd c $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N1" "$RD" --storage-pool="$THICKPOOL" 2>&1) \
    || { echo "FAIL: r c $N1 $RD on thick pool: $_out" >&2; exit 1; }
# Single-replica diskful on the thick pool: poll the single-node disk
# state (wait_uptodate is a 2-peer helper and needs a third positional).
wait_disk_state "$RD" "$N1" "UpToDate" 180

echo ">> [G5] snapshot create on the thick pool MUST be REJECTED"
err_file=$(mktemp)
if "${LCTL[@]}" snapshot create "$RD" "$SNAP" >"$err_file" 2>&1; then
    echo "FAIL (G5): snapshot create on a thick LVM pool was ACCEPTED" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi

# Error should explain the capability gap. Soft-check wording.
if ! grep -qiE "snapshot|support|thin" "$err_file"; then
    echo "note (G5): rejection did not mention snapshot capability:" >&2
    cat "$err_file" >&2
fi
rm -f "$err_file"

# No orphan snapshot CRD may survive the rejected create.
if kubectl get snapshots.blockstor.cozystack.io -o json 2>/dev/null \
        | jq -e --arg rd "$RD" --arg s "$SNAP" \
            '[.items[]? | select(.spec.resourceDefinitionName==$rd) | select(.spec.snapshotName==$s)] | length > 0' \
        >/dev/null 2>&1; then
    echo "FAIL (G5): rejected snapshot create left an orphan Snapshot CRD" >&2
    exit 1
fi

echo ">> [G5] OK — thick-pool snapshot create rejected, no orphan snapshot"
echo "PASS: snap-create-thick-pool-rejected"
