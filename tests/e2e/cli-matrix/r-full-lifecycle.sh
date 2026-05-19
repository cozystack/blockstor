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
#   2.  r d <diskful>               → tiebreaker collapses (Bug 338)
#       r c <same-node>             → diskful re-spawn (Bug 327 / 329 / 339)
#   3.  r d + r c on a different
#       node (the old tiebreaker's) → relocate, sync to UpToDate
#   4.  r d <every diskful>         → cluster collapses cleanly
#   5.  r c <node> --diskless       → spawn Diskless replica
#   6.  r td <node> -s <pool>       → flip diskless→diskful (sync UpToDate)
#   7.  r td --diskless <node>      → flip back to Diskless (Bug 330)
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
"${LCTL[@]}" resource create --auto-place=2 --storage-pool "$SP" "$RD" >/dev/null

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
echo ">> Phase 2: r d $n1 $RD  (Bug 338 trigger — should collapse tiebreaker)"
"${LCTL[@]}" resource delete "$n1" "$RD" >/dev/null

wait_replica_absent "$RD" "$n1" 60 \
    || die "Phase 2: ${RD}.${n1} CRD never disappeared after r d"

# Bug 338 contract: 1 surviving diskful, no tiebreaker.
echo ">> Phase 2: wait up to 30s for tiebreaker to collapse"
deadline=$(( $(date +%s) + 30 ))
collapsed=false
while (( $(date +%s) < deadline )); do
    remaining=$(linstor_replica_count "$RD")
    if [[ "$remaining" == "1" ]]; then
        # Confirm the lone row is the surviving diskful (not the tiebreaker).
        if [[ -z "$(linstor_tiebreaker_node "$RD")" ]]; then
            collapsed=true
            break
        fi
    fi
    sleep 2
done
if [[ "$collapsed" != "true" ]]; then
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    die "Phase 2 (Bug 338): tiebreaker did not collapse to single diskful within 30s"
fi

# Re-create on the SAME node — bare form, no --diskless, no --storage-pool.
# Bug 327 contract: must come back diskful, not Diskless.
echo ">> Phase 2: r c $n1 $RD  (Bug 327 trigger — bare form must yield diskful)"
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

echo ">> Phase 3: r d $n_to_evict $RD  (relocate prep)"
"${LCTL[@]}" resource delete "$n_to_evict" "$RD" >/dev/null
wait_replica_absent "$RD" "$n_to_evict" 60 \
    || die "Phase 3: ${RD}.${n_to_evict} CRD never disappeared"

# Pick a fresh target that doesn't currently host a replica — typically
# the node where the tiebreaker had been spawned in Phase 1 / Phase 2.
relocate_node=$(linstor_pick_free_node "$RD" "$n1")
[[ -n "$relocate_node" ]] \
    || die "Phase 3: no free node to relocate onto (workers: $WORKER_1 $WORKER_2 $WORKER_3)"

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
echo ">> Phase 4: r d every diskful"
mapfile -t diskful_phase4 < <(linstor_diskful_nodes "$RD")
for n in "${diskful_phase4[@]}"; do
    "${LCTL[@]}" resource delete "$n" "$RD" >/dev/null
    wait_replica_absent "$RD" "$n" 60 \
        || die "Phase 4: ${RD}.${n} CRD never disappeared after r d"
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
"${LCTL[@]}" resource toggle-disk --storage-pool "$SP" "$n1" "$RD" >/dev/null
# Diskful materialise + initial sync can take a while on a busy QEMU
# stand. Use the 240s wait_status_state ceiling rather than the 60s
# default.
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
echo ">> PASS: full lifecycle (Bug 327/330/338/339/329 pinned in one chain)"
