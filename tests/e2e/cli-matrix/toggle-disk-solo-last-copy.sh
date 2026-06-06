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

echo ">> init: 2 diskful ($N1,$N2) on $RD, reach UpToDate (latches RD.Initialized)"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 512M >/dev/null
"${LCTL[@]}" resource create "$N1" "$RD" --storage-pool="$POOL" >/dev/null
"${LCTL[@]}" resource create "$N2" "$RD" --storage-pool="$POOL" >/dev/null
wait_uptodate "$RD" "$N1" "$N2"

init=$(kubectl get "resourcedefinitions.blockstor.cozystack.io/${RD}" \
    -o jsonpath='{.spec.initialized}' 2>/dev/null || echo "")
[[ "$init" == "true" ]] \
    || die "precondition: RD.Spec.Initialized='$init', want true after 2 diskful UpToDate"

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
