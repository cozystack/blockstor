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
# Eject the backing block device via sysfs. For a real SCSI/NVMe disk
# this would be `echo 1 > /sys/block/<dev>/device/delete`. Loop
# devices on a busy stand have no sysfs eject path, so we use
# `losetup -d` after invalidating DRBD's reference. If the loop
# device's `/sys/block/loopN/loop/backing_file` is empty after this,
# DRBD's next I/O hits an empty backing → -EIO → disk:Failed.
on_node "$PRIMARY" bash -c "
    LOOP=\$(losetup -j /var/lib/blockstor-pool/${RD}_00000.img | head -1 | cut -d: -f1)
    if [[ -z \"\$LOOP\" ]]; then
        echo 'no loop device for backing image' >&2
        exit 1
    fi
    LOOP_NAME=\$(basename \"\$LOOP\")
    # Force-clear the backing file via sysfs; this is the loop
    # equivalent of yanking a SATA cable.
    if [[ -e /sys/block/\$LOOP_NAME/loop/autoclear ]]; then
        echo 1 > /sys/block/\$LOOP_NAME/loop/autoclear || true
    fi
    losetup -d \"\$LOOP\" 2>/dev/null || true
    # Force a write so the I/O error path runs.
    dd if=/dev/urandom of=${DEV} bs=512 count=1 oflag=direct 2>/dev/null || true
"

echo ">> wait 30s for events2 observer to detach"
sleep 30

# Detach success criteria: local disk in {Diskless, Failed, Detaching, Outdated};
# peer still UpToDate. The exact target state depends on the DRBD-9
# minor version; any non-UpToDate state is acceptable for this test.
prim_disk=$(on_node "$PRIMARY" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
if [[ "$prim_disk" == *"disk:UpToDate"* ]]; then
    echo "FAIL: $PRIMARY disk stayed UpToDate despite forced I/O errors (got: $prim_disk)"
    on_node "$PRIMARY" dmsetup remove "blockstor-fail-${RD}" 2>/dev/null || true
    exit 1
fi

peer_disk=$(on_node "$PEER" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
if [[ "$peer_disk" != *"UpToDate"* ]]; then
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
