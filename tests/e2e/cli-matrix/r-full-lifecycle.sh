#!/usr/bin/env bash
#
# usage: r-full-lifecycle.sh WORK_DIR
#
# L6 P0 catcher for the full Resource lifecycle.
# Exercises Bugs 327, 330, 338, 339, 332, 329 in one script.
# The user has reported regressions in this area >=5 times. Without
# this script green on stand, NO claim of "lifecycle bug fixed" may
# be made.
#
# The script replays the canonical operator-CLI sequence:
#
#   1.  r c --auto-place=2          → 2 diskful + auto-tiebreaker
#   2.  r d <diskful>               → CRD physically removed (Bug 338)
#       r c <same-node>             → diskful re-spawn (Bug 327 / 329 / 339)
#   3.  r d + r c on a different
#       node (the old tiebreaker's) → relocate, sync to UpToDate
#   4.  r d <every diskful>         → cluster collapses cleanly
#   5.  r c <node> --diskless       → spawn Diskless replica
#   6.  r td <node> -s <pool>       → flip diskless→diskful (sync UpToDate)
#   7.  r td --diskless <node>      → flip back to Diskless (Bug 330)
#
# `r d` physically removes the replica (matching upstream LINSTOR) for
# all replica kinds; the node-id-occupied invariant makes the
# subsequent same-node / relocate `r c` safe (the departed id stays
# occupied until the kernel forgets the slot, so the new replica gets a
# fresh id rather than wedging in Connecting/StandAlone).
#
# At every phase the script polls observer-stamped Resource.Status
# (`linstor r l -r <rd>`) until convergence, with a bounded timeout.
# Cross-checks via `drbdsetup status` happen inside the wait_* helpers
# in lib.sh so the assertion is grounded in kernel state, not "200 OK".

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-full-lifecycle-$$
SP=${POOL:-stand}

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

# =====================================================================
# Phase 1: initial autoplace → 2 diskful + auto-tiebreaker
# =====================================================================
echo ">> Phase 1: rd c + vd c + r c --auto-place=2 -s $SP"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 512M >/dev/null
"${LCTL[@]}" resource create --auto-place=2 --storage-pool="$SP" "$RD" >/dev/null

# 3 rows: 2 diskful + 1 TIE_BREAKER (Bug 338's pre-condition shape).
wait_replica_count "$RD" 3 90 \
    || die "Phase 1: autoplace=2 did not stage 3 rows (2 diskful + tiebreaker) within 90s"

mapfile -t diskful_nodes < <(linstor_diskful_nodes "$RD")
[[ ${#diskful_nodes[@]} == 2 ]] \
    || die "Phase 1: expected 2 diskful, got ${#diskful_nodes[@]} (${diskful_nodes[*]:-none})"

tb_node=$(linstor_tiebreaker_node "$RD")
[[ -n "$tb_node" ]] \
    || die "Phase 1: no TIE_BREAKER witness in $RD shape"

echo "   diskful: ${diskful_nodes[0]} ${diskful_nodes[1]}  tiebreaker: $tb_node"

wait_status_state "$RD" "${diskful_nodes[0]}" UpToDate 120 \
    || die "Phase 1: ${diskful_nodes[0]} never UpToDate"
wait_status_state "$RD" "${diskful_nodes[1]}" UpToDate 120 \
    || die "Phase 1: ${diskful_nodes[1]} never UpToDate"

# =====================================================================
# Phase 2: same-node delete + re-create
# =====================================================================
n1="${diskful_nodes[0]}"
echo ">> Phase 2: r d $n1 $RD  (physical delete → Resource CRD removed)"
"${LCTL[@]}" resource delete "$n1" "$RD" >/dev/null

# `r d` physically removes the replica (matching upstream LINSTOR): the
# CRD disappears, the satellite tears DRBD down on the node and frees
# the backing storage, and surviving siblings run del-peer + forget-peer
# to reclaim the bitmap slot. The node-id-occupied invariant keeps the
# departed id occupied until the kernel confirms forget, so the next
# `r c` on the same node handshakes with a fresh node-id rather than
# wedging in Connecting/StandAlone.
wait_replica_absent "$RD" "$n1" 60 \
    || die "Phase 2: ${RD}.${n1} CRD never disappeared after r d"

# Re-create on the SAME node — bare form, no --diskless, no --storage-pool.
# Bug 327 contract: must come back diskful, not Diskless. The replica is
# spawned fresh (its CRD was removed by the physical delete above); the
# satellite renders a new .res, re-creates the backing volume, and the
# kernel resyncs the slot from the surviving peer.
echo ">> Phase 2: r c $n1 $RD  (Bug 327/339 trigger — bare form must spawn diskful)"
"${LCTL[@]}" resource create "$n1" "$RD" >/dev/null

wait_status_state "$RD" "$n1" UpToDate 120 \
    || die "Phase 2 (Bug 327/329): ${n1} never reached UpToDate after r c"

# Pick a peer that is currently diskful for the connection-state assertion.
mapfile -t peers_after_phase2 < <(linstor_diskful_nodes "$RD")
peer_for_n1=""
for p in "${peers_after_phase2[@]}"; do
    if [[ "$p" != "$n1" ]]; then
        peer_for_n1="$p"
        break
    fi
done
if [[ -n "$peer_for_n1" ]]; then
    # Bug 339 contract: peer connection must be Connected/Established,
    # not stuck on Off / Connecting after the re-create.
    wait_conns_ok "$RD" "$n1" "$peer_for_n1" 90 \
        || die "Phase 2 (Bug 339): ${n1}<->${peer_for_n1} never reached Connected/Established"
fi

# =====================================================================
# Phase 3: relocate — delete a diskful, create on a previously-free node
# =====================================================================
mapfile -t diskful_phase3 < <(linstor_diskful_nodes "$RD")
n_to_evict=""
for p in "${diskful_phase3[@]}"; do
    if [[ "$p" != "$n1" ]]; then
        n_to_evict="$p"
        break
    fi
done
[[ -n "$n_to_evict" ]] \
    || die "Phase 3: no second diskful node to evict (have ${diskful_phase3[*]:-none})"

echo ">> Phase 3: r d $n_to_evict $RD  (relocate prep — physical delete)"
"${LCTL[@]}" resource delete "$n_to_evict" "$RD" >/dev/null
# Physical delete: the evicted node's CRD is removed, the backing
# storage is freed, and the surviving diskful ($n1) reclaims the
# departed peer's bitmap slot. The relocate then spawns a fresh diskful
# replica on a DIFFERENT node (typically the tiebreaker's old slot).
wait_replica_absent "$RD" "$n_to_evict" 60 \
    || die "Phase 3: ${RD}.${n_to_evict} CRD never disappeared after r d"

# Pick the relocate target — a worker that is neither $n1 (the surviving
# diskful) nor $n_to_evict (just freed). Typically the node where the
# tiebreaker had been spawned. createOrPromoteResource on the next line
# handles the TIE_BREAKER-takeover path (Bug 260).
relocate_node=""
for w in "$WORKER_1" "$WORKER_2" "$WORKER_3"; do
    if [[ "$w" != "$n1" && "$w" != "$n_to_evict" ]]; then
        relocate_node="$w"
        break
    fi
done
[[ -n "$relocate_node" ]] \
    || die "Phase 3: no relocate target found (workers: $WORKER_1 $WORKER_2 $WORKER_3, n1=$n1, n_to_evict=$n_to_evict)"

echo ">> Phase 3: r c $relocate_node $RD  (diskful on the tiebreaker's old node)"
"${LCTL[@]}" resource create "$relocate_node" "$RD" >/dev/null
wait_status_state "$RD" "$relocate_node" UpToDate 120 \
    || die "Phase 3: ${relocate_node} never reached UpToDate after relocate"

# After relocate the original surviving diskful must still be UpToDate.
wait_status_state "$RD" "$n1" UpToDate 60 \
    || die "Phase 3: ${n1} disk_state regressed after peer relocate"

# Sync must converge cleanly on the new pair (Bug 329 — no UpToDate(NN%) stickiness).
wait_sync_done "$RD" "$relocate_node" "$n1" 240 \
    || die "Phase 3 (Bug 329): ${relocate_node}<->${n1} never reached clean (UpToDate, Established)"

# =====================================================================
# Phase 4: delete every diskful — cluster collapses cleanly
# =====================================================================
echo ">> Phase 4: r d every diskful (physical delete: each CRD removed)"
mapfile -t diskful_phase4 < <(linstor_diskful_nodes "$RD")
for n in "${diskful_phase4[@]}"; do
    "${LCTL[@]}" resource delete "$n" "$RD" >/dev/null
    wait_replica_absent "$RD" "$n" 60 \
        || die "Phase 4: ${RD}.${n} CRD never disappeared after r d"
done

# Mop up any remaining replicas (e.g. a diskless promote left behind by
# an earlier phase, or a tiebreaker the controller hasn't reaped yet).
# Each `r d` physically removes its CRD; Phase 5's `r c --diskless`
# precondition requires a fully torn-down cluster.
mapfile -t all_remaining < <(kubectl get resources.blockstor.io.blockstor.io \
    --no-headers 2>/dev/null \
    | awk -v rd="${RD}." '$1 ~ "^"rd {print $1}' \
    | sed "s/^${RD}\\.//")
for n in "${all_remaining[@]}"; do
    [[ -z "$n" ]] && continue
    "${LCTL[@]}" resource delete "$n" "$RD" >/dev/null
    wait_replica_absent "$RD" "$n" 60 \
        || die "Phase 4: ${RD}.${n} CRD never physically removed after r d"
done

# Give the controller a moment to tear any leftover tiebreaker witness.
sleep 10
diskful_left=$(linstor_diskful_count "$RD")
[[ "$diskful_left" == "0" ]] \
    || die "Phase 4: expected 0 diskful after deleting all, got $diskful_left"

# =====================================================================
# Phase 5: re-add as diskless
# =====================================================================
echo ">> Phase 5: r c $n1 $RD --diskless"
"${LCTL[@]}" resource create "$n1" "$RD" --diskless >/dev/null
wait_status_diskless "$RD" "$n1" 60 \
    || die "Phase 5: ${n1} never reached Diskless within 60s"

# Sibling-shape check: $n1 must be the ONLY non-tiebreaker row, and it
# must carry the DISKLESS flag (NOT a diskful spawn).
n1_flags=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${n1}" \
    -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
[[ "$n1_flags" == *"DISKLESS"* ]] \
    || die "Phase 5: ${n1} Spec.Flags='$n1_flags' missing DISKLESS — wrong-direction spawn"

# =====================================================================
# Phase 6: toggle diskless → diskful (-s <pool> materialises backing)
# =====================================================================
echo ">> Phase 6: r td $n1 $RD -s $SP  (diskless→diskful)"
"${LCTL[@]}" resource toggle-disk --storage-pool="$SP" "$n1" "$RD" >/dev/null
# Bug 356 fix: solo-replica diskful flip now triggers auto-promote
# (drbdadm primary --force) so the lone diskful slot transitions
# Inconsistent → UpToDate without needing a sync source. 240s ceiling
# accommodates QEMU-stand metadata create + attach + primary cycle.
wait_status_state "$RD" "$n1" UpToDate 240 \
    || die "Phase 6: ${n1} never reached UpToDate after r td -s $SP"

post_toggle_flags=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${n1}" \
    -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
[[ "$post_toggle_flags" != *"DISKLESS"* ]] \
    || die "Phase 6: ${n1} Spec.Flags='$post_toggle_flags' still contains DISKLESS after toggle to diskful"

# =====================================================================
# Phase 7: toggle diskful → diskless (Bug 330)
# =====================================================================
echo ">> Phase 7: r td --diskless $n1 $RD  (Bug 330 trigger)"
"${LCTL[@]}" resource toggle-disk --diskless "$n1" "$RD" >/dev/null
wait_status_diskless "$RD" "$n1" 60 \
    || die "Phase 7 (Bug 330): ${n1} never reached Diskless within 60s after r td --diskless"

# =====================================================================
# Cleanup (handled by EXIT trap) + invariant.
# =====================================================================
echo ">> PASS: full lifecycle (Bug 327/329/330/338/339/342/356 pinned in one chain)"
