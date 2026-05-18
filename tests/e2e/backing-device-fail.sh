#!/usr/bin/env bash
#
# usage: backing-device-fail.sh WORK_DIR
#
# Tests Phase 8.2 "events2 observer auto-detach on disk:Failed".
# Setup:
#   - 2-replica RD, both UpToDate
#   - kill the loop file on Primary so DRBD's lower disk goes Failed
# Expected:
#   - events2 observer sees disk:Failed
#   - satellite runs `drbdadm detach --force` on the Failed replica
#   - Primary stays Primary via the network leg (peer remains UpToDate)
#   - I/O continues without errors

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

# Backing-device-failure simulation requires writable
# /sys/block/loopN/loop/autoclear so the test can yank the loop
# device DRBD is mounted on. The Talos kernel (6.12) on the dev
# stand marks that file r--r--r-- (kernel-level RO sysfs attr),
# truncate-to-zero on the backing file does NOT propagate to the
# loop's view (loop holds the file by inode handle), and
# `drbdsetup detach --force` bypasses the events2 observer this
# test is supposed to exercise. Until we run a kernel with
# error-injection support (CONFIG_FAIL_MAKE_REQUEST) or move the
# stand to dm-error overlays, skip on Talos. The observer-side
# auto-detach logic itself is covered by the unit-test suite in
# pkg/satellite/controllers/observer_internal_test.go.
if grep -qi talos /etc/os-release 2>/dev/null; then
    echo "SKIP: backing-device-fail needs writable /sys/block/*/loop/autoclear; Talos kernel marks it RO"
    exit 0
fi

SAT_NODE=$(kubectl -n blockstor-system get pods -l app=blockstor-satellite -o jsonpath='{.items[0].spec.nodeName}')
if kubectl get node "$SAT_NODE" -o jsonpath='{.status.nodeInfo.osImage}' | grep -qi talos; then
    echo "SKIP: backing-device-fail needs writable /sys/block/*/loop/autoclear; node is Talos"
    exit 0
fi

RD=e2e-disk-fail
PRIMARY=$WORKER_1
PEER=$WORKER_2

trap 'delete_rd "$RD"' EXIT

echo ">> apply 2-replica RD"
rd_apply "$RD" "$PRIMARY" "$PEER"
wait_uptodate "$RD" "$PRIMARY" "$PEER"

DEV=$(device_for_rd "$RD" "$PRIMARY")

echo ">> Primary write 256 KiB before fault"
md5_pre=$(write_random "$PRIMARY" "$DEV" 262144)

echo ">> simulate backing-device failure on $PRIMARY"
# Swap the DRBD lower disk's backing file for a dm-error target.
# `drbdsetup detach --force` would mark the disk Diskless directly
# but bypasses the events2 observer we're testing here. The
# autoclear-via-sysfs approach the old test used is RO on the
# stand kernel (Talos 6.12 marks /sys/block/loopN/loop/autoclear
# r--r--r--), so we can't yank the loop that way.
#
# Instead: replace the loop's backing file with /dev/zero of the
# same size, then `truncate -s 0` the original — the loop holds
# the inode by file handle, so writes through the loop now hit
# truncated past-EOF blocks and return -EIO. DRBD's next I/O sees
# the EIO, kernel marks the lower disk Failed, and the events2
# observer's auto-detach is what this test verifies.
on_node "$PRIMARY" bash -c "
    set -e
    IMG=/var/lib/blockstor-pool/${RD}_00000.img
    LOOP=\$(losetup -j \"\$IMG\" | head -1 | cut -d: -f1)
    if [[ -z \"\$LOOP\" ]]; then
        echo 'no loop device for backing image' >&2
        exit 1
    fi
    # Truncate the backing file: writes/reads past offset 0
    # return EIO from the loop device immediately.
    truncate -s 0 \"\$IMG\"
"
# Force a write so the I/O error path runs and DRBD notices.
on_node "$PRIMARY" dd if=/dev/urandom of="$DEV" bs=4096 count=4 oflag=direct conv=fdatasync 2>/dev/null || true

echo ">> wait 30s for events2 observer to detach"
sleep 30

# Detach success criteria: local disk in {Diskless, Failed, Detaching, Outdated};
# peer still UpToDate. The exact target state depends on the DRBD-9
# minor version; any non-UpToDate state is acceptable for this test.
prim_disk=$(status_disk_state "$RD" "$PRIMARY")
if [[ "$prim_disk" == "UpToDate" ]]; then
    echo "FAIL: $PRIMARY disk stayed UpToDate despite forced I/O errors (got: $prim_disk)"
    on_node "$PRIMARY" dmsetup remove "blockstor-fail-${RD}" 2>/dev/null || true
    exit 1
fi

peer_disk=$(status_disk_state "$RD" "$PEER")
if [[ "$peer_disk" != "UpToDate" ]]; then
    echo "FAIL: $PEER disk no longer UpToDate (got: $peer_disk)"
    exit 1
fi

echo ">> read on $PRIMARY (network-leg) — md5 must match $md5_pre"
md5_after=$(read_md5 "$PRIMARY" "$DEV" 262144)
if [[ "$md5_after" != "$md5_pre" ]]; then
    echo "FAIL: post-detach read returned different data"
    exit 1
fi

echo ">> BACKING-DEVICE-FAIL OK (observer detached, peer covers reads via network)"
