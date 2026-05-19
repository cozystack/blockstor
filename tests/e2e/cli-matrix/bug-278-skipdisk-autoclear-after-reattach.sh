#!/usr/bin/env bash
#
# usage: bug-278-skipdisk-autoclear-after-reattach.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 278.
#
# Reproduction: after a satellite pod restart (simulating a Talos OS
# kernel upgrade), the existing defensive-stamp code path stamps
# `DrbdOptions/SkipDisk=True` onto Resource.Spec.Props. Pre-fix, the
# stamp survived the reattach forever — the satellite kept dispatching
# `drbdadm adjust --skip-disk` and the local volume stayed Diskless
# even though the kernel was healthy.
#
# Bug 278 fix: when the reconciler sees `SkipDisk=True` AND the kernel
# reports the local volume as non-Diskless (UpToDate / Inconsistent /
# Outdated), the satellite releases the observer's SSA claim on the
# SkipDisk key via SkipDiskClearer. The next dispatcher cycle
# re-resolves Spec.Props without SkipDisk, the FSM transitions
# PhaseSkipDisk → PhaseRunning, and the next reconcile dispatches plain
# `drbdadm adjust` to re-attach the lower disk.
#
# Contract this cell pins:
#
#   1. Steady-state: 2-replica diskful RD, both UpToDate.
#   2. Stamp SkipDisk=True onto Resource.Spec.Props on $N2 (simulates
#      the observer's defensive write on a transient Failed event the
#      kernel emits at Talos upgrade time).
#   3. Restart the satellite pod on $N2 (simulates Talos kernel
#      restart). The pod reattaches, sees the SkipDisk prop, but
#      kernel state on $N2 is healthy (disk:UpToDate, backing_dev
#      present — the lower LV survived the OS upgrade).
#   4. Within 60s assert: Spec.Props NO LONGER contains DrbdOptions/SkipDisk
#      AND Status.volumes[0].diskState is back to UpToDate.
#
# Pre-fix: SkipDisk stays pinned forever; the cell would FAIL at
# step 4 because Spec.Props still carries the key after 60s.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

RD=cli-matrix-278

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

N1=$WORKER_1
N2=$WORKER_2

echo ">> [Bug 278] 2-replica diskful RD on $N1+$N2"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 128M >/dev/null
"${LCTL[@]}" resource create "$N1" "$RD" --storage-pool=stand >/dev/null
"${LCTL[@]}" resource create "$N2" "$RD" --storage-pool=stand >/dev/null

echo ">> wait for both diskful UpToDate"
RD="$RD" wait_uptodate "$RD" "$N1" "$N2"

echo ">> stamp DrbdOptions/SkipDisk=True onto $N2 (simulates pre-upgrade defensive stamp)"
# Patch Resource.Spec.Props directly via kubectl — bypasses the
# linstor CLI's prop-set path so we mirror the observer's SSA write
# exactly. The Bug 278 fix gates auto-clear off "prop pinned AND
# kernel healthy", regardless of the prop's origin.
kubectl patch "resources.blockstor.io.blockstor.io/${RD}.${N2}" --type=merge -p \
    '{"spec":{"props":{"DrbdOptions/SkipDisk":"True"}}}'

echo ">> confirm SkipDisk is stamped on $N2 Spec.Props"
stamped=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N2}" \
    -o jsonpath='{.spec.props.DrbdOptions/SkipDisk}' 2>/dev/null || echo "")
if [[ "$stamped" != "True" ]]; then
    echo "FAIL (Bug 278 setup): SkipDisk stamp did not land (got '$stamped'); aborting" >&2
    exit 1
fi

echo ">> restart satellite pod on $N2 (simulates Talos kernel upgrade reattach)"
sat_pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    --field-selector "spec.nodeName=$N2" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
if [[ -z "$sat_pod" ]]; then
    echo "FAIL (Bug 278 setup): no satellite pod found on $N2; aborting" >&2
    exit 1
fi
kubectl -n "$NS" delete pod "$sat_pod" --wait=true >/dev/null

echo ">> wait for satellite back up on $N2"
for _ in $(seq 1 60); do
    new_pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        --field-selector "spec.nodeName=$N2,status.phase=Running" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    if [[ -n "$new_pod" && "$new_pod" != "$sat_pod" ]]; then
        break
    fi
    sleep 2
done
if [[ -z "$new_pod" || "$new_pod" == "$sat_pod" ]]; then
    echo "FAIL (Bug 278 setup): new satellite pod did not start on $N2 within 120s" >&2
    exit 1
fi

echo ">> wait up to 60s for the satellite to auto-clear SkipDisk on $N2"
# Poll Resource.Spec.Props.DrbdOptions/SkipDisk — Bug 278 contract:
# the satellite reconciler probes kernel state, sees healthy
# (disk:UpToDate, backing_dev set), and releases the observer's SSA
# claim on the SkipDisk key. After SSA release the apiserver removes
# the key from Spec.Props (no other owner claims it).
cleared=false
for _ in $(seq 1 30); do
    val=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N2}" \
        -o jsonpath='{.spec.props.DrbdOptions/SkipDisk}' 2>/dev/null || echo "")
    if [[ -z "$val" ]]; then
        cleared=true
        break
    fi
    sleep 2
done

if [[ "$cleared" != "true" ]]; then
    echo "FAIL (Bug 278): SkipDisk did NOT auto-clear on $N2 within 60s after satellite restart" >&2
    kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N2}" \
        -o json 2>/dev/null | jq '{props: .spec.props, status: .status}' >&2 || true
    exit 1
fi

echo ">> confirm $N2 disk state is back to UpToDate (re-attached after clear)"
if ! wait_status_state "$RD" "$N2" "UpToDate" 60 0; then
    echo "FAIL (Bug 278 deep): SkipDisk cleared but $N2 did not re-attach to UpToDate within 60s" >&2
    kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N2}" \
        -o json 2>/dev/null | jq '{props: .spec.props, status: .status}' >&2 || true
    exit 1
fi

# Sibling check: $N1 must still be UpToDate (the restart on $N2 must
# NOT affect the sibling's disk state — that would mean either the
# auto-clear path was overzealous or the cluster has another
# instability).
n1_disk=$(status_disk_state "$RD" "$N1" 0)
if [[ "$n1_disk" != "UpToDate" ]]; then
    echo "FAIL (Bug 278 sibling regression): $N1 disk_state=$n1_disk after $N2 reattach (want UpToDate)" >&2
    exit 1
fi

echo ">> bug-278-skipdisk-autoclear-after-reattach OK (auto-clear fires on healthy reattach, $N2 back to UpToDate)"
