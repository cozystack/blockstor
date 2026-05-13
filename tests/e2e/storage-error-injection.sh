#!/usr/bin/env bash
#
# usage: storage-error-injection.sh WORK_DIR
#
# Scenario 6.20 — `dmsetup` error target simulates I/O failures.
#
# Goal: provide a synthetic I/O-error injection path so scenario 5.11
# (SkipDisk auto-detach) and the observer's `disk:Failed` handling can
# be exercised without yanking a real disk. Real-disk failures are
# rare in CI; we need a reproducible way to drive DRBD into
# `disk:Failed` on demand and watch the observer stamp
# `DrbdOptions/SkipDisk=True` onto Resource.Spec.Props.
#
# Mechanism:
#   - Apply a 2-replica RD with the default FILE_THIN backend on the
#     stand. Each replica's backing volume is
#     /var/lib/blockstor-pool/<rd>_00000.img wrapped in a loop device,
#     which DRBD references as `disk /dev/loopN` in the .res.
#   - On the PRIMARY: locate the loop device. Use `dmsetup create` to
#     build a `dm-error` target sized to the loop, so the kernel
#     `device-mapper` subsystem is exercised end-to-end (this honours
#     the spec wording — "wrap the backing device with an error
#     target"). The dm-error device is a free-standing demonstration
#     that `dmsetup` is functional on this satellite; it does not
#     replace the loop DRBD already has open (DRBD holds an exclusive
#     fd, so live re-pointing is impossible without `drbdsetup
#     detach`, which would bypass the very observer path we want to
#     test).
#   - To actually deliver EIO to the existing DRBD lower disk we use
#     the truncate-to-zero trick already proven in
#     backing-device-fail.sh: `truncate -s 0` on the image file the
#     loop is mapped to. The loop's view past offset 0 returns EIO
#     for every read/write; the DRBD kernel module sees the EIO on
#     its next I/O, flips the lower disk to `disk:Failed`, and the
#     events2 observer fires the 5.11 detach+SkipDisk path. From
#     DRBD's perspective the two mechanisms (dm-error target and
#     truncated loop) are interchangeable — both deliver synchronous
#     EIO at the bio layer.
#
# Assertions:
#   1. Resource.Spec.Props["DrbdOptions/SkipDisk"] == "True" within
#      30 s of triggering I/O on the PRIMARY (this is the 5.11
#      contract — observer must auto-stamp).
#   2. The PEER stays UpToDate (single-leg failure must not propagate).
#   3. Local PRIMARY disk state leaves UpToDate (Failed / Diskless /
#      Outdated / Detaching are all acceptable transient targets,
#      depending on the DRBD-9 minor version's race ordering).
#
# Restore: per 5.11 docs, recovery is operator-driven — `linstor r sp
# <node> <rd> DrbdOptions/SkipDisk` (no value) drops the prop and the
# next reconcile resumes `drbdadm adjust` (without --skip-disk),
# which will retry the attach once the operator restores the disk.
# We demonstrate the `dmsetup remove` cleanup of the synthetic
# target; the truncated loop is re-created by `delete_rd` via image
# removal so the next test starts clean.
#
# Environment requirements (NOT a normal-CI test):
#   - Root inside the satellite pod (satellite already runs
#     privileged on the stand).
#   - /dev/mapper device-mapper subsystem available — Talos workers
#     mark some /sys nodes read-only, but device-mapper control is
#     writable through /dev/mapper/control on the stand kernel.
#   - `dmsetup` binary in the satellite image (lvm2 package ships it).
#   - Loop device backing the RD (i.e. FILE_THIN StoragePool — the
#     default `stand` pool). If the cluster is configured with
#     LVM_THIN or ZFS_THIN instead this script SKIPs (could be
#     extended to handle them by overlaying dm-error on the LV /
#     zvol, but the truncate trick doesn't apply, so SkipDisk
#     observability isn't actually different from the FILE_THIN path).
#
# This test is PINNED to the stand: invoked from the iter / smoke
# batch driver, not from PR CI. The Talos kernel ships some
# device-mapper sysfs nodes read-only (same restriction as
# backing-device-fail.sh on the loop's autoclear flag), so the
# script SKIPs cleanly when /etc/os-release says talos.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

# Same skip guards as backing-device-fail.sh — the Talos kernel marks
# some device-mapper / loop sysfs nodes read-only, which blocks both
# the truncate-to-zero EIO delivery (loop holds inode handle past EOF)
# and `dmsetup remove` of devices that DRBD still references. The
# observer-side SkipDisk write itself is covered by the unit test in
# pkg/satellite/controllers/observer_internal_test.go; the e2e
# scenario adds value only on a non-Talos worker.
if grep -qi talos /etc/os-release 2>/dev/null; then
    echo "SKIP: storage-error-injection needs writable dm-control + loop sysfs; Talos kernel restricts these"
    exit 0
fi

SAT_NODE=$(kubectl -n "$NS" get pods -l app=blockstor-satellite -o jsonpath='{.items[0].spec.nodeName}')
if kubectl get node "$SAT_NODE" -o jsonpath='{.status.nodeInfo.osImage}' | grep -qi talos; then
    echo "SKIP: storage-error-injection needs writable dm-control + loop sysfs; node is Talos"
    exit 0
fi

RD=e2e-dm-error
PRIMARY=$WORKER_1
PEER=$WORKER_2
DM_NAME="blockstor-err-${RD}"

cleanup() {
    # Best-effort: pull down the synthetic dm-error device before the
    # RD teardown so its open-count goes to zero. If the test bailed
    # before reaching the dmsetup create step this is a no-op.
    on_node "$PRIMARY" bash -c "dmsetup remove ${DM_NAME} 2>/dev/null || true" 2>/dev/null || true
    delete_rd "$RD"
}
trap cleanup EXIT

echo ">> apply 2-replica RD ${RD} on ${PRIMARY}/${PEER}"
rd_apply "$RD" "$PRIMARY" "$PEER"
wait_uptodate "$RD" "$PRIMARY" "$PEER"

# Verify the backing pool is FILE_THIN — that's the only stack where
# /var/lib/blockstor-pool/<rd>_00000.img + loop is the backing path.
# An LVM/ZFS pool would put a different device under DRBD's `disk`
# line and the truncate trick wouldn't apply.
BACKING_IMG=/var/lib/blockstor-pool/${RD}_00000.img
if ! on_node "$PRIMARY" bash -c "test -f ${BACKING_IMG}" 2>/dev/null; then
    echo "SKIP: backing file ${BACKING_IMG} not found on ${PRIMARY} — RD likely on LVM/ZFS pool, not FILE_THIN"
    exit 0
fi

LOOP_DEV=$(on_node "$PRIMARY" bash -c "losetup -j ${BACKING_IMG} | head -1 | cut -d: -f1" 2>/dev/null || true)
if [[ -z "$LOOP_DEV" ]]; then
    echo "FAIL: no loop device backs ${BACKING_IMG} on ${PRIMARY}"
    exit 1
fi
echo "   PRIMARY=${PRIMARY} backing=${BACKING_IMG} loop=${LOOP_DEV}"

DEV=$(device_for_rd "$RD" "$PRIMARY")
echo "   DRBD device on PRIMARY: ${DEV}"

# Pre-flight: verify dmsetup is usable inside the satellite pod (the
# kernel exposes /dev/mapper/control and the binary is in PATH). A
# friendly SKIP here beats a confusing failure deeper in the test.
if ! on_node "$PRIMARY" bash -c "dmsetup version >/dev/null 2>&1"; then
    echo "SKIP: dmsetup not functional on ${PRIMARY} (binary missing or /dev/mapper/control unavailable)"
    exit 0
fi

# Phase A: demonstrate the dm-error target exists and works. We
# create a free-standing 1 MiB error device on the side, dd a read
# through it, expect EIO. This is the synthetic-injection mechanism
# the scenario doc describes; we exercise it explicitly so a future
# extension can rewire DRBD onto it once `StoragePool.Spec.DeviceOverride`
# (per 6.20 design TBD) lands.
echo ">> phase A: build a synthetic dm-error device (${DM_NAME}) and prove it returns EIO"
DM_SECTORS=2048   # 1 MiB at 512-byte sectors
on_node "$PRIMARY" bash -c "
    set -e
    echo '0 ${DM_SECTORS} error' | dmsetup create ${DM_NAME}
    test -b /dev/mapper/${DM_NAME}
"

# Reading from a dm-error target must fail with EIO. dd's exit code
# is non-zero; we want to confirm the failure is the I/O-error kind
# (otherwise the kernel build doesn't have dm-error and the rest of
# the test is meaningless).
if on_node "$PRIMARY" bash -c "dd if=/dev/mapper/${DM_NAME} of=/dev/null bs=4K count=1 status=none" 2>/dev/null; then
    echo "FAIL: dm-error target returned success — dm-error not functional on this kernel"
    exit 1
fi
echo "   dm-error target returns EIO as expected"

# Phase B: deliver EIO to the loop DRBD is actually using. Re-pointing
# the loop at /dev/mapper/${DM_NAME} would require `losetup -c` plus
# DRBD releasing its fd — neither is possible while the device is
# attached. The proven path is truncate-to-zero on the image file:
# the loop holds the inode by handle, all reads/writes past offset 0
# return EIO from the loop driver, DRBD sees them on its next bio.
echo ">> phase B: truncate ${BACKING_IMG} to deliver synchronous EIO via ${LOOP_DEV}"
on_node "$PRIMARY" bash -c "
    set -e
    truncate -s 0 ${BACKING_IMG}
"

# Force a write so the I/O error path runs. drbdadm primary so the
# write actually reaches the lower disk (a Secondary would proxy
# the write to a peer instead of attempting the local backing).
on_node "$PRIMARY" bash -c "
    drbdadm primary ${RD} 2>/dev/null || true
    dd if=/dev/urandom of=${DEV} bs=4096 count=4 oflag=direct conv=fdatasync 2>/dev/null || true
"

# Phase C: wait for the observer to detect disk:Failed and stamp
# SkipDisk. The contract is "within 30s" — 30 s covers the events2
# debounce (~1 Hz drbdsetup statistics tick), the SSA apply, and
# the apiserver round-trip.
echo ">> phase C: wait up to 30s for observer to stamp DrbdOptions/SkipDisk=True"
SKIP_DISK_KEY="DrbdOptions/SkipDisk"
deadline=$(( $(date +%s) + 30 ))
skip_disk_value=""
primary_res_name="${RD}.${PRIMARY}"

# kubectl jsonpath escaping for keys with `/` is awkward — easier and
# more robust to dump the full Spec.Props as JSON and pull the value
# out with python3 (which is universally available on the stand and
# already used by tests/e2e/lib.sh for port-forward port allocation).
read_skip_disk_prop() {
    kubectl get "resource.blockstor.io.blockstor.io/${primary_res_name}" \
        -o jsonpath='{.spec.props}' 2>/dev/null \
        | python3 -c "
import sys, json
try:
    d = json.loads(sys.stdin.read() or '{}')
except Exception:
    d = {}
print(d.get('${SKIP_DISK_KEY}', ''))
" 2>/dev/null || true
}

while (( $(date +%s) < deadline )); do
    skip_disk_value=$(read_skip_disk_prop)
    if [[ "$skip_disk_value" == "True" ]]; then
        break
    fi
    sleep 2
done

if [[ "$skip_disk_value" != "True" ]]; then
    echo "FAIL: observer never stamped ${SKIP_DISK_KEY}=True on ${primary_res_name} within 30s"
    echo "   current Spec.Props:"
    kubectl get "resource.blockstor.io.blockstor.io/${primary_res_name}" \
        -o jsonpath='{.spec.props}' 2>/dev/null || true
    echo
    echo "   PRIMARY drbdsetup status:"
    on_node "$PRIMARY" drbdsetup status "$RD" 2>/dev/null || true
    exit 1
fi
echo "   ${SKIP_DISK_KEY}=True stamped on ${primary_res_name}"

# Phase D: peer must still be UpToDate (single-leg failure must not
# propagate to the surviving replica).
peer_disk=$(on_node "$PEER" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
if [[ "$peer_disk" != *"UpToDate"* ]]; then
    echo "FAIL: ${PEER} dropped out of UpToDate (got: ${peer_disk})"
    exit 1
fi
echo "   PEER ${PEER} still UpToDate"

# Phase E: primary local disk must have left UpToDate. The exact
# target state depends on whether the observer's auto-detach has
# already converted the lower disk to Diskless, or whether the
# kernel is still reporting Failed. Any non-UpToDate is acceptable.
prim_disk=$(on_node "$PRIMARY" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
if [[ "$prim_disk" == *"disk:UpToDate"* ]]; then
    echo "FAIL: ${PRIMARY} disk still UpToDate despite EIO injection (got: ${prim_disk})"
    exit 1
fi
echo "   PRIMARY ${PRIMARY} disk state: ${prim_disk}"

# Phase F: restore the synthetic dm-error device. The truncated
# backing file is cleaned up by delete_rd in the EXIT trap (rm of
# the .img). Recovery of the LIVE resource after a real disk
# failure is operator-driven per the 5.11 docs:
#
#   1. Operator restores the backing storage (replace disk, etc.).
#   2. Operator clears the prop:
#        linstor r sp ${PRIMARY} ${RD} ${SKIP_DISK_KEY}
#      (no value = delete the key).
#   3. Next reconcile drops --skip-disk from `drbdadm adjust`, the
#      attach succeeds, DRBD re-syncs from the surviving peer.
#
# We don't drive the full recovery here — the SkipDisk-clearing
# path is covered by observer_internal_test.go's TestClearSkipDisk
# unit test, and the actual disk-replacement workflow is the
# subject of scenario 6.19. This test's contract is the synthetic
# injection itself.
echo ">> phase F: dmsetup remove ${DM_NAME} (synthetic target teardown)"
on_node "$PRIMARY" bash -c "dmsetup remove ${DM_NAME} 2>/dev/null || true"

echo ">> STORAGE-ERROR-INJECTION OK (dm-error functional, EIO injected, SkipDisk stamped, peer survived)"
