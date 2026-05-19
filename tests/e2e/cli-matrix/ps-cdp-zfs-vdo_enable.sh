#!/usr/bin/env bash
#
# usage: ps-cdp-zfs-vdo_enable.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 326.
#
# Reproduction from the e2e2 stand:
#
#   $ linstor ps cdp zfs <worker> /dev/sdb --pool-name data --storage-pool data
#   ERROR: Bad Request — unknown field "vdo_enable"
#
# Root cause: the python-linstor CLI serialises a full upstream-shaped
# JSON body — vdo_enable, raid_level, lv/pv/vg/zpool sibling fields —
# regardless of whether the operator opted in to VDO. Pre-fix the
# blockstor REST handler used a strict-unknown decoder and rejected
# every `ps cdp` invocation from the python CLI.
#
# Fix: commit 84276ad63 + regression test at
# pkg/rest/physical_storage_test.go::TestPhysicalStorageCreateAcceptsVdoEnable.
# This L6 cell is the stand-side companion: drives the real
# `linstor ps cdp` invocation and asserts the StoragePool appears
# in `linstor sp l` without 400 / "unknown field".

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

linstor_cli_setup
trap linstor_cli_teardown EXIT

# Stable per-cell SP name so a partial-failure rerun finds + cleans
# the previous attempt instead of accumulating ghost pools.
POOL=cli-matrix-vdo-zfs
NODE=$WORKER_1

# Discover an unused block device on the worker. The stand provisions
# /dev/sdb on every worker for ad-hoc CDP tests (see stand/up.sh
# QEMU disk allocation). Skip the cell when the device is absent —
# this typically means the stand is the small-footprint flavour.
if ! kubectl -n "$NS" exec \
        "$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
            -o jsonpath="{.items[?(@.spec.nodeName==\"$NODE\")].metadata.name}")" \
        -- bash -c 'test -b /dev/sdb && ! pvs /dev/sdb >/dev/null 2>&1 && ! zpool labelclear -f /dev/sdb 2>/dev/null; test -b /dev/sdb' >/dev/null 2>&1; then
    echo "SKIP: $NODE has no /dev/sdb or it is in use — Bug 326 stand fixture not available"
    exit 0
fi

# Pre-clean: a previous run may have created the SP and never torn
# the underlying zpool; do best-effort cleanup so the cdp request
# below has a clean device to claim.
"${LCTL[@]}" storage-pool delete "$NODE" "$POOL" 2>/dev/null || true
on_node "$NODE" bash -c "zpool destroy ${POOL}_zpool 2>/dev/null; zpool labelclear -f /dev/sdb 2>/dev/null" || true

cleanup() {
    "${LCTL[@]}" storage-pool delete "$NODE" "$POOL" 2>/dev/null || true
    on_node "$NODE" bash -c "zpool destroy ${POOL}_zpool 2>/dev/null; zpool labelclear -f /dev/sdb 2>/dev/null" || true
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> [Bug 326] linstor ps cdp zfs $NODE /dev/sdb --pool-name ${POOL}_zpool --storage-pool $POOL"

# The pre-fix failure mode was an immediate `ERROR: Bad Request` from
# the REST decoder — the CLI never even reached the satellite. So
# the first leg of the assertion is "this command exits 0 and stderr
# carries no 'unknown field' message". The second leg is observer-
# layer convergence: the SP appears in `sp l` within 30s.
err_file=$(mktemp)
if ! "${LCTL[@]}" physical-storage create-device-pool \
        zfs "$NODE" /dev/sdb \
        --pool-name "${POOL}_zpool" \
        --storage-pool "$POOL" \
        2>"$err_file"; then
    rc=$?
    echo "FAIL (Bug 326 regression): linstor ps cdp exited $rc" >&2
    echo "----- stderr -----" >&2
    cat "$err_file" >&2
    echo "------------------" >&2
    rm -f "$err_file"
    exit 1
fi

if grep -qiE 'unknown field|vdo_enable|Bad Request|400' "$err_file"; then
    echo "FAIL (Bug 326 regression): unknown-field / 400 in stderr" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

echo ">> wait up to 30s for SP $POOL on $NODE to surface in 'sp l'"
deadline=$(( $(date +%s) + 30 ))
found=false
while (( $(date +%s) < deadline )); do
    out=$("${LCTL[@]}" --machine-readable storage-pool list \
        --storage-pools "$POOL" 2>/dev/null || echo "")
    if grep -q "\"$POOL\"" <<<"$out"; then
        found=true
        break
    fi
    sleep 2
done

if [[ "$found" != "true" ]]; then
    echo "FAIL (Bug 326): SP $POOL never surfaced in 'linstor sp l' within 30s" >&2
    "${LCTL[@]}" storage-pool list 2>&1 | tail -20 >&2
    exit 1
fi

echo ">> ps-cdp-zfs-vdo_enable OK (Bug 326 pinned: vdo_enable wire-shape body accepted, SP staged)"
