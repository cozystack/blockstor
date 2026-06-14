#!/usr/bin/env bash
#
# usage: toggle-disk-solo-last-copy.sh WORK_DIR
#
# L6 cli-matrix cell — solo diskless→diskful toggle on the LAST/ONLY
# replica of an INITIALIZED RD (the r-full Phase 6 wedge).
#
# Reproduction (operator-CLI level):
#
#   $ linstor rd c <rd>; linstor vd c <rd> 512M
#   $ linstor r c <n1> <rd> -s <sp>     # diskful #1
#   $ linstor r c <n2> <rd> -s <sp>     # diskful #2  → RD.Initialized latches
#   # both reach UpToDate
#   $ linstor r d <n1> <rd>             # delete BOTH diskful
#   $ linstor r d <n2> <rd>             # (RD.Initialized stays latched)
#   $ linstor r c <n1> <rd> --diskless  # re-add a single diskless witness
#   $ linstor r td <n1> <rd> -s <sp>    # toggle the LONE replica to diskful
#
# Contract: the toggled replica is now the lone, peerless copy of an
# initialized RD. It MUST converge to UpToDate.
#
# Before the satellite solo-promote self-heal it wedged at Inconsistent
# forever: the dispatcher suppresses the auto-primary seed on an
# initialized RD (respawn-StandAlone fix), and the offline-safety
# seed-refusal (SkipInitialSync=false) declines the day0 UpToDate skip —
# so with no peer to SyncTarget from there was no path to UpToDate. The
# fix (maybeSoloPromote → NeedsSoloPromote → drbdadm primary --force on a
# zero-connection diskful-below-UpToDate slot) restores convergence.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-solo-last-copy
POOL=${POOL:-stand}

N1=$WORKER_1
N2=$WORKER_2

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> init: 2 diskful ($N1,$N2) on $RD, reach UpToDate"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 512M >/dev/null
"${LCTL[@]}" resource create "$N1" "$RD" --storage-pool="$POOL" >/dev/null
"${LCTL[@]}" resource create "$N2" "$RD" --storage-pool="$POOL" >/dev/null
wait_uptodate "$RD" "$N1" "$N2"

# RD.Spec.Initialized does NOT latch on bare UpToDate. The controller's
# ensureSkipInitSyncDecision flips it true only when a diskful peer is
# PROVEN data-bearing — i.e. its volume's DRBD CurrentGI has been
# observed AND differs from the deterministic day0 GI
# (anyProvenDataBearingDiskfulPeer / isDay0SeededDiskfulVolume in
# internal/controller/resource_controller.go). A freshly-created,
# never-written pair sits at the day0 GI forever, so the old
# "UpToDate ⇒ Initialized" precondition was wrong and always died here.
# Trigger the real latch by writing real data: promote $N1, write a few
# MiB to advance the generation past day0, demote. This is the same
# advance-past-day0 device write u145-write-then-add-replica-syncs uses.
echo ">> write real data on $N1 to advance GI past day0 (the actual Initialized latch trigger)"
N1_DEV=$(kubectl get resource "${RD}.${N1}" -o jsonpath='{.status.volumes[0].devicePath}' 2>/dev/null || echo "")
if [[ -z "$N1_DEV" ]]; then
    N1_MINOR=$(kubectl get resource "${RD}.${N1}" -o jsonpath='{.status.drbdMinor}' 2>/dev/null || echo "")
    [[ -n "$N1_MINOR" ]] && N1_DEV="/dev/drbd${N1_MINOR}"
fi
[[ -n "$N1_DEV" ]] || N1_DEV=$(resolve_drbd_device "$N1" "$RD" 0 2>/dev/null || echo "")
[[ -n "$N1_DEV" ]] \
    || die "precondition: could not resolve $N1 DRBD device to write the latch-triggering data"
on_node "$N1" sh -c "
    D=${N1_DEV};
    drbdsetup primary $RD --force 2>/dev/null || drbdadm primary --force $RD 2>/dev/null || true;
    dd if=/dev/urandom of=\$D bs=1M count=16 oflag=direct 2>/dev/null;
    sync;
    drbdadm secondary $RD 2>/dev/null || true;
" || die "precondition: write to $N1 DRBD device ($N1_DEV) failed"

echo ">> wait up to 60s for RD.Spec.Initialized to latch true after the write"
init=""
init_deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < init_deadline )); do
    init=$(kubectl get "resourcedefinitions.blockstor.cozystack.io/${RD}" \
        -o jsonpath='{.spec.initialized}' 2>/dev/null || echo "")
    [[ "$init" == "true" ]] && break
    sleep 2
done
[[ "$init" == "true" ]] \
    || die "precondition: RD.Spec.Initialized='$init', want true after a real data write past day0"

echo ">> collapse: delete BOTH diskful (RD.Initialized stays latched)"
"${LCTL[@]}" resource delete "$N1" "$RD" >/dev/null
wait_replica_absent "$RD" "$N1" 60 \
    || die "collapse: ${RD}.${N1} CRD never disappeared after r d"
"${LCTL[@]}" resource delete "$N2" "$RD" >/dev/null
wait_replica_absent "$RD" "$N2" 60 \
    || die "collapse: ${RD}.${N2} CRD never disappeared after r d"

diskful_left=$(linstor_diskful_count "$RD")
[[ "$diskful_left" == "0" ]] \
    || die "collapse: expected 0 diskful after deleting all, got $diskful_left"

echo ">> re-add a single diskless replica on $N1"
"${LCTL[@]}" resource create "$N1" "$RD" --diskless >/dev/null
wait_status_diskless "$RD" "$N1" 60 \
    || die "re-add: ${N1} never reached Diskless within 60s"

echo ">> r td $N1 $RD -s $POOL  (SOLO diskless→diskful on an initialized RD)"
"${LCTL[@]}" resource toggle-disk --storage-pool="$POOL" "$N1" "$RD" >/dev/null

# The solo-promote self-heal must drive the lone slot Inconsistent →
# UpToDate. 240s mirrors the r-full Phase-6 budget.
wait_status_state "$RD" "$N1" UpToDate 240 \
    || die "Phase-6 solo flip: ${N1} never reached UpToDate after r td -s $POOL (solo-promote wedge)"

# The DISKLESS flag must be gone (real diskful promote, not a flag no-op).
post_flags=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${N1}" \
    -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
[[ "$post_flags" != *"DISKLESS"* ]] \
    || die "solo flip: ${N1} Spec.Flags='$post_flags' still contains DISKLESS after toggle to diskful"

echo "PASS: solo diskless→diskful toggle on initialized RD converged UpToDate"
