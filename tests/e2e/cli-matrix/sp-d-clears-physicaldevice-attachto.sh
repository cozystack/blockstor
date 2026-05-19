#!/usr/bin/env bash
#
# usage: sp-d-clears-physicaldevice-attachto.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 340.
#
# Reproduction from the production stand (real, user-reported):
#
#   1. `linstor ps cdp ... zfs <node> /dev/sdX` accepts the
#      request; the REST controller creates the SP CRD and
#      flips PhysicalDevice.Spec.AttachTo pointing at SP.
#   2. The downstream attach fails (e.g. Bug 336 v1 stale ZFS
#      labels, ZFS create error). The SP ends in State=Error,
#      the PhysicalDevice in Phase=Attaching.
#   3. Operator runs `linstor sp d <node> <pool>` to clean up
#      the failed pool. The SP CRD is deleted — BUT
#      PhysicalDevice.Spec.AttachTo is NOT cleared by anyone.
#   4. Satellite reconciler then loops
#      "target StoragePool not yet known; requeuing" every 10s
#      forever. The PhysicalDevice is stuck in Phase=Attaching,
#      invisible to `linstor ps l` (which filters Available
#      only).
#   5. Operator cannot re-use the device for another `ps cdp`
#      without manually `kubectl edit physicaldevices.
#      blockstor.io <name>` to clear AttachTo.
#
# Fix (Bug 340): when the satellite reconciler sees AttachTo
# set + Status.Phase=Attaching (i.e. the SP was observed at
# least once before) AND the target SP CRD is gone, clear
# AttachTo + transition the device back to Available so
# `linstor ps l` surfaces it again and the operator can
# immediately re-issue `ps cdp` against the same device.
#
# This L6 cell drives the real `linstor sp d` cascade on the
# stand, asserts the PhysicalDevice's AttachTo is cleared
# within 30s, and that the operator can re-attach the device
# straight after without manual intervention.

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
POOL=cli-matrix-bug340
NODE=$WORKER_1

# Discover the per-worker spare device the stand provisions for
# ad-hoc CDP tests. Skip the cell when the device is absent —
# small-footprint stand flavours skip the spare disk allocation.
SAT_POD=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o jsonpath="{.items[?(@.spec.nodeName==\"$NODE\")].metadata.name}")
if [[ -z "$SAT_POD" ]]; then
    echo "SKIP: satellite pod for $NODE not found"
    exit 0
fi

if ! kubectl -n "$NS" exec "$SAT_POD" -- test -b /dev/sdb >/dev/null 2>&1; then
    echo "SKIP: $NODE has no /dev/sdb — Bug 340 stand fixture not available"
    exit 0
fi

# Pre-clean any previous attempt so the cell is rerunnable.
on_node "$NODE" bash -c "
    zpool destroy ${POOL} 2>/dev/null || true
    wipefs -af /dev/sdb >/dev/null 2>&1 || true
    blockdev --rereadpt /dev/sdb >/dev/null 2>&1 || true
" || true

cleanup() {
    "${LCTL[@]}" storage-pool delete "$NODE" "$POOL" 2>/dev/null || true
    on_node "$NODE" bash -c "
        zpool destroy ${POOL} 2>/dev/null
        wipefs -af /dev/sdb >/dev/null 2>&1 || true
        blockdev --rereadpt /dev/sdb >/dev/null 2>&1 || true
    " || true
    linstor_cli_teardown
}
trap cleanup EXIT

# Step 1: drive `linstor ps cdp` — may succeed or fail. We don't
# care which; what matters is that the SP CRD + AttachTo flip
# happen even if the satellite-side attach errors out partway.
echo ">> [Bug 340] linstor ps cdp zfs $NODE /dev/sdb --pool-name ${POOL} --storage-pool ${POOL}"
"${LCTL[@]}" physical-storage create-device-pool \
    zfs "$NODE" /dev/sdb \
    --pool-name "${POOL}" \
    --storage-pool="${POOL}" \
    2>/dev/null || echo "note: ps cdp returned non-zero — Bug 340 reproduces either way"

# Step 2: even if ps cdp returned success, wait long enough for
# the satellite to publish AttachTo / mid-attach state. Then
# tear the SP down — this is the operator's "give up on this
# pool" cleanup gesture.
sleep 5

echo ">> [Bug 340] linstor sp d $NODE ${POOL} (operator cleanup)"
"${LCTL[@]}" storage-pool delete "$NODE" "${POOL}" 2>/dev/null || true

# Step 3: within 30s the satellite reconciler must self-heal —
# clear Spec.AttachTo + flip Status.Phase back to Available.
# Pre-fix the PhysicalDevice stays stuck in Phase=Attaching
# with a dangling AttachTo for the rest of the controller's
# lifetime.
echo ">> wait up to 30s for the satellite to self-heal Spec.AttachTo"
deadline=$(( $(date +%s) + 30 ))
attach_to=""
phase=""
while (( $(date +%s) < deadline )); do
    # Match the PhysicalDevice CRD for this node + /dev/sdb. The
    # name is derived from a stable id (wwn / scsi-SATA / nvme / by-path)
    # so we can't predict it exactly; filter by node label + lsblk path.
    pd_name=$(kubectl get physicaldevices.blockstor.io \
        -l "blockstor.io/node=${NODE}" \
        -o json 2>/dev/null \
        | jq -r '.items[] | select(.status.currentDevPath=="/dev/sdb" or .status.devicePath=="/dev/sdb" or (.status.currentDevPath|endswith("/sdb"))) | .metadata.name' \
        | head -1)
    if [[ -n "$pd_name" ]]; then
        attach_to=$(kubectl get physicaldevices.blockstor.io "$pd_name" \
            -o jsonpath='{.spec.attachTo.storagePoolName}' 2>/dev/null || echo "")
        phase=$(kubectl get physicaldevices.blockstor.io "$pd_name" \
            -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
        if [[ -z "$attach_to" ]]; then
            break
        fi
    fi
    sleep 2
done

if [[ -n "$attach_to" ]]; then
    echo "FAIL (Bug 340): PhysicalDevice $pd_name still has Spec.AttachTo='$attach_to' 30s after sp d" >&2
    echo "----- physicaldevice -----" >&2
    kubectl get physicaldevices.blockstor.io "$pd_name" -o yaml >&2 || true
    echo "--------------------------" >&2
    exit 1
fi

if [[ "$phase" != "Available" && -n "$phase" ]]; then
    echo "FAIL (Bug 340): PhysicalDevice $pd_name Phase=$phase 30s after sp d; want Available" >&2
    exit 1
fi

# Step 4: cross-verify the operator can immediately re-issue
# `ps cdp` against the same device — pre-fix this required a
# manual `kubectl edit physicaldevices.blockstor.io <name>`
# to clear AttachTo first.
echo ">> [Bug 340] verify re-issuing ps cdp works without manual intervention"
on_node "$NODE" bash -c "
    wipefs -af /dev/sdb >/dev/null 2>&1 || true
    blockdev --rereadpt /dev/sdb >/dev/null 2>&1 || true
" || true

if ! "${LCTL[@]}" physical-storage create-device-pool \
        zfs "$NODE" /dev/sdb \
        --pool-name "${POOL}" \
        --storage-pool="${POOL}" \
        2>/dev/null; then
    echo "FAIL (Bug 340): second ps cdp attempt rejected — device still stuck post-self-heal" >&2
    exit 1
fi

# Allow the second attempt to converge (or not — we don't care
# about end state here, just that the device was *available* to
# the request) and tear down.
sleep 5

assert_no_orphans

echo ">> sp-d-clears-physicaldevice-attachto OK (Bug 340 pinned: AttachTo self-heals + device re-usable within 30s)"
