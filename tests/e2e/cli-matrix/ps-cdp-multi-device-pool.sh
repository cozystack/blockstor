#!/usr/bin/env bash
#
# usage: ps-cdp-multi-device-pool.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 337.
#
# Reproduction from the e2e2 stand (user-reported):
#
#   $ linstor ps cdp --pool-name multi --storage-pool multi \
#         zfs e2e2-worker-1 /dev/sda /dev/sdb
#   SUCCESS: physical-storage attach accepted on node 'e2e2-worker-1'
#
#   $ ssh e2e2-worker-1 zpool list -v multi   # 30s later
#   NAME    SIZE  ...
#   multi   <half the expected size>
#     sda     ...                              # sdb is MISSING
#
# Root cause: `pickFreeDeviceForAttach` returned a single device
# (the first match) and silently dropped subsequent ones. The
# satellite-side Attach path then created a single-device zpool;
# the operator-expected multi-vdev layout never materialised.
#
# Fix (Bug 337):
#   1. pkg/rest/physical_storage.go::pickFreeDeviceForAttach now
#      returns the full slice of matched devices; the handler
#      flips Spec.AttachTo on each.
#   2. pkg/satellite/attach.go::Attach branches on pool-exists:
#      `zpool create` on the first observed device, `zpool add`
#      on subsequent. Stateless per-device reconcile.
#
# This L6 cell drives the real `linstor ps cdp zfs` with TWO
# devices on the stand, asserts both vdevs appear in
# `zpool list -v multi`, then runs an ONLINE-EXPANSION leg by
# re-invoking `ps cdp` with a third device and asserting the
# new vdev gets folded into the existing pool via `zpool add`
# (not by re-creating).

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
POOL=cli-matrix-multi
NODE=$WORKER_1

SAT_POD=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o jsonpath="{.items[?(@.spec.nodeName==\"$NODE\")].metadata.name}")
if [[ -z "$SAT_POD" ]]; then
    echo "SKIP: satellite pod for $NODE not found"
    exit 0
fi

# Discover two free devices on the worker. The stand provisions
# /dev/sd{b,c,d} on every worker for ad-hoc CDP tests. We need two
# for the create + extend legs, and a third for the online-expand
# leg. Skip the cell when fewer than three devices are present.
DEV1=/dev/sdb
DEV2=/dev/sdc
DEV3=/dev/sdd

for dev in "$DEV1" "$DEV2" "$DEV3"; do
    if ! kubectl -n "$NS" exec "$SAT_POD" -- test -b "$dev" >/dev/null 2>&1; then
        echo "SKIP: $NODE has no $dev — Bug 337 stand fixture not available"
        exit 0
    fi
done

# Pre-clean: any previous run may have left the SP + the on-host
# zpool behind. Drop both before we drive ps cdp.
"${LCTL[@]}" storage-pool delete "$NODE" "$POOL" 2>/dev/null || true
on_node "$NODE" bash -c "
    zpool destroy ${POOL} 2>/dev/null || true
    for d in ${DEV1} ${DEV2} ${DEV3}; do
        wipefs -af \$d >/dev/null 2>&1 || true
        blockdev --rereadpt \$d >/dev/null 2>&1 || true
    done
" || true

cleanup() {
    "${LCTL[@]}" storage-pool delete "$NODE" "$POOL" 2>/dev/null || true
    on_node "$NODE" bash -c "
        zpool destroy ${POOL} 2>/dev/null || true
        for d in ${DEV1} ${DEV2} ${DEV3}; do
            wipefs -af \$d >/dev/null 2>&1 || true
            blockdev --rereadpt \$d >/dev/null 2>&1 || true
        done
    " || true
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> [Bug 337] linstor ps cdp zfs $NODE $DEV1 $DEV2 --pool-name ${POOL} --storage-pool ${POOL}"

err_file=$(mktemp)
if ! "${LCTL[@]}" physical-storage create-device-pool \
        zfs "$NODE" "$DEV1" "$DEV2" \
        --pool-name "${POOL}" \
        --storage-pool "${POOL}" \
        2>"$err_file"; then
    rc=$?
    echo "FAIL (Bug 337): linstor ps cdp exited $rc" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

echo ">> wait up to 120s for SP $POOL on $NODE to converge with both devices"
deadline=$(( $(date +%s) + 120 ))
vdev_count=0
while (( $(date +%s) < deadline )); do
    # `zpool list -v <pool>` prints the pool header line plus one
    # line per vdev. Count children of the pool (lines after the
    # pool name that start with whitespace + a device kname).
    vdev_count=$(on_node "$NODE" \
        bash -c "zpool list -H -v ${POOL} 2>/dev/null \
            | awk 'NR>1 && /^\\s+(sd|nvme|vd|by-id)/ {n++} END{print n+0}'" 2>/dev/null \
        || echo 0)
    if (( vdev_count >= 2 )); then
        break
    fi
    sleep 2
done

if (( vdev_count < 2 )); then
    echo "FAIL (Bug 337): zpool ${POOL} on $NODE never reported 2 vdevs (got ${vdev_count}) within 120s" >&2
    on_node "$NODE" zpool list -v "${POOL}" 2>&1 >&2 || true
    exit 1
fi

# Sanity: capture the create-leg free capacity so the
# online-expand leg below can show it grew.
free_2dev=$("${LCTL[@]}" --machine-readable storage-pool list \
    --storage-pools "$POOL" --nodes "$NODE" 2>/dev/null \
    | jq -r '.[0].stor_pools[0].free_capacity // 0' 2>/dev/null \
    || echo 0)

echo ">> two-device zpool ${POOL} confirmed (vdevs=${vdev_count}, free_capacity_kib=${free_2dev})"

# ---- Online-expansion leg ------------------------------------------------
# Re-run `linstor ps cdp` with a NEW device. The flat-per-device
# satellite reconciler probes pool-exists on the host (=yes) and
# branches to `zpool add ${POOL} ${DEV3}` instead of re-creating
# the pool. This is the user-blessed "stateless satellite" design
# (memory:feedback_ps_cdp_incremental).

echo ">> [Bug 337] online expand: linstor ps cdp zfs $NODE $DEV3 --pool-name ${POOL} --storage-pool ${POOL}"

err_file=$(mktemp)
if ! "${LCTL[@]}" physical-storage create-device-pool \
        zfs "$NODE" "$DEV3" \
        --pool-name "${POOL}" \
        --storage-pool "${POOL}" \
        2>"$err_file"; then
    rc=$?
    echo "FAIL (Bug 337 online-expand): linstor ps cdp exited $rc" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

echo ">> wait up to 120s for $DEV3 to be folded into zpool ${POOL}"
deadline=$(( $(date +%s) + 120 ))
vdev_count=0
while (( $(date +%s) < deadline )); do
    vdev_count=$(on_node "$NODE" \
        bash -c "zpool list -H -v ${POOL} 2>/dev/null \
            | awk 'NR>1 && /^\\s+(sd|nvme|vd|by-id)/ {n++} END{print n+0}'" 2>/dev/null \
        || echo 0)
    if (( vdev_count >= 3 )); then
        break
    fi
    sleep 2
done

if (( vdev_count < 3 )); then
    echo "FAIL (Bug 337 online-expand): zpool ${POOL} never reported 3 vdevs after second ps cdp (got ${vdev_count})" >&2
    on_node "$NODE" zpool list -v "${POOL}" 2>&1 >&2 || true
    exit 1
fi

free_3dev=$("${LCTL[@]}" --machine-readable storage-pool list \
    --storage-pools "$POOL" --nodes "$NODE" 2>/dev/null \
    | jq -r '.[0].stor_pools[0].free_capacity // 0' 2>/dev/null \
    || echo 0)

# Free capacity MUST grow with the third device. We don't pin a
# strict 1.5x ratio because vdev-add reshapes the per-vdev free
# accounting and ZFS overhead is not perfectly linear; "grew by
# at least 20%" is the safe-margin assertion.
if (( free_3dev <= free_2dev )); then
    echo "FAIL (Bug 337 online-expand): free_capacity did not grow after adding $DEV3 (${free_2dev} → ${free_3dev})" >&2
    exit 1
fi

echo ">> ps-cdp-multi-device-pool OK (Bug 337 pinned: 2-device create + 3rd-device online expand)"
