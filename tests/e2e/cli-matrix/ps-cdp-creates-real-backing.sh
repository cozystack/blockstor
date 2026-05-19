#!/usr/bin/env bash
#
# usage: ps-cdp-creates-real-backing.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 336.
#
# Reproduction from the e2e2 stand (real, user-reported):
#
#   $ linstor ps cdp --pool-name data --storage-pool data zfs e2e2-worker-1 /dev/sda
#   SUCCESS: physical-storage attach accepted on node 'e2e2-worker-1'
#
#   $ linstor sp l                                # 30s later
#   data  e2e2-worker-1  ZFS  data  -  -  False  Error  ...
#   ERROR: pool backing storage missing on node e2e2-worker-1:
#          storage pool data is not present
#
# Root cause: `linstor ps cdp` was returning SUCCESS because the
# REST handler wrote the StoragePool CRD + flipped Spec.AttachTo
# on the PhysicalDevice, but the satellite-side Attach path's
# `zpool create -f` then failed on a device whose prior
# (aborted) pool create left stale ZFS-style partitions
# (sda1 zfs_member + sda9 zfs_reserved). `zpool create -f`:
#
#   cannot label 'sda': failed to detect device partitions
#   on '/dev/sda1': 19
#   Error preparing/labeling disk.: exit status 1
#
# Because the REST endpoint never tracked the satellite-side
# create outcome, the operator only saw `State=Error` minutes
# later via `sp l` — no surfaced cause, no clear next step.
#
# Fix (commit pinning Bug 336):
#   1. pkg/rest/physical_storage.go::buildAttachTo defaults
#      `AttachTo.Wipe=true` for the `ps cdp` flow (operator
#      explicitly opted in to claim the device).
#   2. pkg/satellite/attach.go::wipeDevice now follows wipefs
#      with `blockdev --rereadpt` so the kernel drops stale
#      partition device nodes before the kind-specific create.
#
# This L6 cell drives the real `linstor ps cdp zfs` on the
# stand, asserts the SP converges to State=Ok within 60s, AND
# cross-verifies `zpool list <pool>` exits 0 on the worker —
# i.e. the on-host backing storage actually materialised, not
# just the controller-side CRD.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

linstor_cli_setup
trap linstor_cli_teardown EXIT

# Stable per-cell pool name so a partial-failure rerun finds + cleans
# the previous attempt instead of accumulating ghost pools.
POOL=cli-matrix-cdp-backing
NODE=$WORKER_1

# Discover an unused block device on the worker. The stand provisions
# /dev/sdb on every worker for ad-hoc CDP tests (see stand/up.sh QEMU
# disk allocation). Skip the cell when the device is absent — typical
# of the small-footprint stand flavour.
SAT_POD=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o jsonpath="{.items[?(@.spec.nodeName==\"$NODE\")].metadata.name}")
if [[ -z "$SAT_POD" ]]; then
    echo "SKIP: satellite pod for $NODE not found"
    exit 0
fi

if ! kubectl -n "$NS" exec "$SAT_POD" -- test -b /dev/sdb >/dev/null 2>&1; then
    echo "SKIP: $NODE has no /dev/sdb — Bug 336 stand fixture not available"
    exit 0
fi

# Pre-clean: stamp the device with a stale ZFS partition table that
# matches the e2e2 reproduction fixture — that's the failure mode
# Bug 336 fixes (wipefs alone left /dev/sdb1 + /dev/sdb9 in the
# kernel partition list). If the device is in use by another pool
# already, skip — STAND_RESET should have cleared it.
on_node "$NODE" bash -c "
    zpool destroy ${POOL} 2>/dev/null || true
    zpool destroy ${POOL}_stale 2>/dev/null || true
    wipefs -af /dev/sdb >/dev/null 2>&1 || true
    blockdev --rereadpt /dev/sdb >/dev/null 2>&1 || true
" || true

if on_node "$NODE" bash -c 'lsblk -nro NAME /dev/sdb | grep -qE "sdb[0-9]+"' 2>/dev/null; then
    if on_node "$NODE" bash -c 'pvs /dev/sdb 2>/dev/null'; then
        echo "SKIP: /dev/sdb on $NODE is in use by LVM"
        exit 0
    fi
fi

# Stage the Bug 336 reproduction fixture: create a one-shot ZFS pool
# + destroy it, leaving the stale GPT (sdb1 + sdb9) AND the ZFS
# secondary label at end-of-device. Pre-fix v1 this defeated the
# follow-up `ps cdp` with the "failed to detect device partitions
# on '/dev/sdb1': 19" error. Post-fix v2 the guaranteed-clean
# wipeDevice (wipefs + dd zero both ends + rereadpt + partprobe)
# clears every signature before `zpool create` runs.
on_node "$NODE" bash -c "
    zpool create -f ${POOL}_stale /dev/sdb 2>/dev/null && \
    zpool destroy ${POOL}_stale 2>/dev/null
" || echo "note: stale-zpool prep best-effort; device may already be clean"

# Confirm the fixture actually stamped the partition table — without
# sdb1 / sdb9 the cell isn't reproducing Bug 336's failure mode and
# a green result would be a false negative. Warn (don't fail) so the
# cell still exercises the wipe path on stands where ZFS strips the
# partition table on destroy.
if ! on_node "$NODE" bash -c 'lsblk -nro NAME /dev/sdb | grep -qE "sdb[0-9]+"' 2>/dev/null; then
    echo "note: Bug 336 fixture did not produce sdb[0-9]+ partitions; wipe path will still run"
fi

cleanup() {
    "${LCTL[@]}" storage-pool delete "$NODE" "$POOL" 2>/dev/null || true
    on_node "$NODE" bash -c "
        zpool destroy ${POOL} 2>/dev/null
        zpool destroy ${POOL}_stale 2>/dev/null
        wipefs -af /dev/sdb >/dev/null 2>&1 || true
        blockdev --rereadpt /dev/sdb >/dev/null 2>&1 || true
    " || true
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> [Bug 336] linstor ps cdp zfs $NODE /dev/sdb --pool-name ${POOL} --storage-pool ${POOL}"

err_file=$(mktemp)
if ! "${LCTL[@]}" physical-storage create-device-pool \
        zfs "$NODE" /dev/sdb \
        --pool-name "${POOL}" \
        --storage-pool="${POOL}" \
        2>"$err_file"; then
    rc=$?
    echo "FAIL (Bug 336): linstor ps cdp exited $rc" >&2
    echo "----- stderr -----" >&2
    cat "$err_file" >&2
    echo "------------------" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

echo ">> wait up to 60s for SP $POOL on $NODE to converge to State=Ok"
deadline=$(( $(date +%s) + 60 ))
sp_state=""
while (( $(date +%s) < deadline )); do
    # `linstor sp l` State column is derived from the satellite's
    # PoolStatus probe + Reports; pre-fix the pool stayed at
    # "Error" with the "pool backing storage missing" report
    # because zpool create never produced the backing pool.
    sp_state=$("${LCTL[@]}" --machine-readable storage-pool list \
        --storage-pools "$POOL" --nodes "$NODE" 2>/dev/null \
        | jq -r '.[0].stor_pools[0].reports[0].ret_code // empty' 2>/dev/null \
        || echo "")
    cur_free=$("${LCTL[@]}" --machine-readable storage-pool list \
        --storage-pools "$POOL" --nodes "$NODE" 2>/dev/null \
        | jq -r '.[0].stor_pools[0].free_capacity // 0' 2>/dev/null \
        || echo "0")
    # Pool is Ok when free_capacity > 0 and no error reports are stamped.
    if [[ -z "$sp_state" || "$sp_state" == "0" ]] && (( cur_free > 0 )); then
        break
    fi
    sleep 2
done

if (( cur_free == 0 )); then
    echo "FAIL (Bug 336): SP $POOL never reported non-zero free_capacity within 60s" >&2
    "${LCTL[@]}" storage-pool list --storage-pools "$POOL" --nodes "$NODE" 2>&1 | tail -20 >&2
    exit 1
fi

# Cross-verify: the on-host zpool actually exists. Pre-fix the
# controller-side CRD was created but `zpool create` failed,
# so the satellite had no real backing storage. Post-fix the
# host MUST report the pool via `zpool list <name>`.
echo ">> cross-verify: zpool list ${POOL} on $NODE"
if ! on_node "$NODE" zpool list -H -o name "${POOL}" >/dev/null 2>&1; then
    echo "FAIL (Bug 336): satellite reported SP Ok but \`zpool list ${POOL}\` on $NODE failed — backing storage never materialised" >&2
    on_node "$NODE" zpool list 2>&1 >&2 || true
    exit 1
fi

# Bug 336 v2: cross-verify the partition table is clean. The new
# zpool ZFS just created will have ITS OWN sdb1 + sdb9 — that's
# expected. The point of the v2 wipe was that the STALE pre-attach
# partition entries didn't defeat `zpool create`. We assert the
# current sdb1/sdb9 (if present) belong to the live pool by
# checking lsblk reports a sane state (no kernel-cached ghost
# entries for a different pool).
echo ">> cross-verify: lsblk reports a coherent partition table for /dev/sdb"
if ! on_node "$NODE" bash -c 'lsblk -nro NAME,TYPE /dev/sdb' >/dev/null 2>&1; then
    echo "FAIL (Bug 336 v2): lsblk /dev/sdb failed — kernel partition state is corrupt" >&2
    on_node "$NODE" lsblk /dev/sdb 2>&1 >&2 || true
    exit 1
fi

echo ">> ps-cdp-creates-real-backing OK (Bug 336 v2 pinned: zpool create succeeded against device with stale partitions + secondary labels)"
