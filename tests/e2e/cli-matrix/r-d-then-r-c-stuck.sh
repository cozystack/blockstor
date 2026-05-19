#!/usr/bin/env bash
#
# usage: r-d-then-r-c-stuck.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 339 (user-reported pending task #532).
#
# Reproduction from the e2e2 stand:
#
#   $ linstor r l -r test
#   test  worker-1  …  UpToDate
#   test  worker-2  …  UpToDate
#   test  worker-3  …  TieBreaker
#
#   $ linstor r d worker-2 test          # delete diskful replica
#   $ linstor r c worker-2 test          # re-create on the SAME node, bare form
#
#   $ linstor r l -r test
#   test  worker-1  …  UpToDate          Conns=Connecting(worker-2)
#   test  worker-2  …  Inconsistent      Conns=Connecting(worker-1)   ← STUCK
#   test  worker-3  …  TieBreaker
#
# Stuck-state surfaces:
#   - Survivor (worker-1) reports `Connecting(<recreated-node>)` past
#     the 90s convergence window; never reaches `Established`.
#   - Recreated replica (worker-2) sits on `Inconsistent` or
#     `Outdated`, never transitions through `SyncTarget` to `UpToDate`.
#   - Worst case: peer connection lands on `StandAlone` because the
#     surviving replica's on-disk metadata held a stale GI for the
#     deleted peer's old node-id slot, the recreated replica was
#     allocated a fresh node-id but its set-gi seed didn't match what
#     the survivor's forget-peer left behind, and DRBD-9's GI handshake
#     concludes "Unrelated data, aborting!".
#
# Why this cell is its own file (NOT a phase of r-full-lifecycle.sh):
#   - Bug 339 has been re-reported 5+ times despite the lifecycle test
#     covering it. Splitting the assertion into a focused cell makes
#     the failure signature obvious (no "Phase 2 of 7 failed" noise),
#     lets the nightly dispatcher run it without dragging in the 6
#     unrelated phases, and gives operators a 30-line repro script
#     they can re-run by hand.
#   - The lifecycle test's Phase 2 check is kept as belt-and-braces.
#     This cell is the catcher; the lifecycle pinning is the regression
#     wall.
#
# Contract (the actual user-visible PASS gate):
#
#   After `r d <node> <rd>` + `r c <node> <rd>` (bare, same node):
#     1. Recreated replica's CRD reaches Spec.Flags without DISKLESS
#        and Status.DiskState=UpToDate within 180s (Bug 327 invariant).
#     2. Survivor's peer connection to the recreated node reaches
#        Connected/Established within 90s of step 1 (Bug 339 invariant
#        — no stuck Connecting / StandAlone / Off).
#     3. Reverse direction: recreated node's connection to survivor
#        also reaches Connected/Established.
#     4. Replication state on both ends is `Established` (NOT
#        WFBitMapS/T, NOT VerifyS/T, NOT a stuck SyncTarget).
#     5. No orphan CRDs / kernel slots / .res files after delete_rd.
#
# Underlying fix chain (kept for cross-reference):
#   - Bug 67 / 263: REST bumps `blockstor.io/peer-changed` annotation
#     on every survivor after r d AND r c so the satellite watch fires
#     even though kube-apiserver dedupes resourceVersion bumps.
#   - Bug 284: seedPerPeerGi stamps the LOCAL node-id slot too, not
#     just peer slots — prevents Unrelated/StandAlone when the
#     survivor's forget-peer leaves an asymmetric GI tuple.
#   - Bug 289: APIReader fall-through in waitForControllerAllocation
#     so the recreated replica's reconcile doesn't pin on stale cache.
#   - Bug 290: orphan-sweeper clears kernel slots from the deleted
#     incarnation before the next r c on the same node.
#   - Bug 302: DRBD-ID allocation runs on every Reconcile (top-of-loop)
#     so the recreated Resource gets a fresh nodeID/port/minor
#     deterministically.
#   - Bug 327: bare `r c <node> <rd>` produces diskful, NOT Diskless.
#   - Bug 347: ensureMetadata seeds GI when metadata is freshly created
#     even on firstActivation=false paths (tiebreaker promote, replica
#     recreate after del-peer+forget-peer).
#
# Unit pins (legacy + ongoing):
#   - pkg/rest/peer_changed_bump_bug_263_test.go
#   - pkg/rest/r_create_bug_327_test.go
#   - pkg/satellite/reconciler_drbd_test.go (set-gi local-slot, Bug 284)
#   - internal/controller/bug_302_drbd_id_allocation_test.go
#
# This L6 cell is the stand-side ground truth: real CLI, real DRBD
# kernel, observer events2, peer connection message column.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-339
SP=${POOL:-stand}

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> [Bug 339] shape-2r-tb: 2-replica RD + auto-tiebreaker"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 256M >/dev/null
"${LCTL[@]}" resource create --auto-place=2 --storage-pool="$SP" "$RD" >/dev/null

echo ">> wait up to 180s for steady state: 2 diskful UpToDate + 1 TIE_BREAKER"
deadline=$(( $(date +%s) + 180 ))
diskful_nodes_arr=()
while (( $(date +%s) < deadline )); do
    mapfile -t diskful_nodes_arr < <(linstor_diskful_nodes "$RD")
    if (( ${#diskful_nodes_arr[@]} == 2 )); then
        d0=$(status_disk_state "$RD" "${diskful_nodes_arr[0]}" 0)
        d1=$(status_disk_state "$RD" "${diskful_nodes_arr[1]}" 0)
        tb=$(linstor_tiebreaker_node "$RD")
        if [[ "$d0" == "UpToDate" && "$d1" == "UpToDate" && -n "$tb" ]]; then
            break
        fi
    fi
    sleep 3
done
if (( ${#diskful_nodes_arr[@]} != 2 )); then
    echo "FAIL: never reached 2 diskful UpToDate within 180s" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi
N_REC="${diskful_nodes_arr[0]}"   # node we'll delete + recreate on
N_PEER="${diskful_nodes_arr[1]}"  # survivor diskful peer
echo "   recreate target: $N_REC   survivor peer: $N_PEER   tiebreaker: $(linstor_tiebreaker_node "$RD")"

# Sanity: the survivor must already be peering with N_REC (no point
# probing the stuck-after-recreate state if the pre-state was broken).
wait_conns_ok "$RD" "$N_PEER" "$N_REC" 60 \
    || die "Pre-state: survivor $N_PEER not yet Connected to $N_REC after initial place"

# ---------------------------------------------------------------------
# Phase A: r d <recreate-target> <rd>
# ---------------------------------------------------------------------
echo ">> linstor r d $N_REC $RD"
"${LCTL[@]}" resource delete "$N_REC" "$RD" >/dev/null

wait_replica_absent "$RD" "$N_REC" 60 \
    || die "Phase A: ${RD}.${N_REC} CRD never disappeared after r d"

# The survivor's peer connection to the deleted node must be torn
# down — observer should either remove the connection row or report
# it as Off/Disconnected. The next phase recreates the same node, and
# any leftover "Connecting" state from the deleted incarnation would
# make the post-recreate convergence check ambiguous.
deadline=$(( $(date +%s) + 30 ))
while (( $(date +%s) < deadline )); do
    msg=$(status_connection_state "$RD" "$N_PEER" "$N_REC" 2>/dev/null || echo "")
    # Empty/absent connection row OR explicit Off/Disconnected — both
    # signal del-peer landed. Connecting/Established here means the
    # tear-down didn't actually run.
    if [[ -z "$msg" || "$msg" =~ ^(Off|Disconnected|StandAlone|TearDown|Unknown)$ ]]; then
        break
    fi
    sleep 2
done

# ---------------------------------------------------------------------
# Phase B: r c <same-node> <rd>  (bare form — no flags, no -s)
# ---------------------------------------------------------------------
echo ">> linstor r c $N_REC $RD  (Bug 339 trigger: re-create on the SAME node, bare form)"
err_file=$(mktemp)
if ! "${LCTL[@]}" resource create "$N_REC" "$RD" 2>"$err_file"; then
    rc=$?
    echo "FAIL: bare r c on $N_REC exited $rc — Bug 327 regression on the REST handler" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

# Bug 327 invariant — the recreated replica must come back diskful.
# A diskless re-spawn here would mask the Bug 339 connection check
# below (a diskless replica's "connection" message is structurally
# different from a diskful one's).
echo ">> wait up to 180s for $N_REC to land DRBD+STORAGE, UpToDate (NOT Diskless)"
deadline=$(( $(date +%s) + 180 ))
ok_recreate=false
last_flags=""
last_disk=""
while (( $(date +%s) < deadline )); do
    last_flags=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N_REC}" \
        -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
    last_disk=$(status_disk_state "$RD" "$N_REC" 0)
    if [[ "$last_flags" != *"DISKLESS"* ]] && [[ "$last_disk" == "UpToDate" ]]; then
        ok_recreate=true
        break
    fi
    sleep 3
done
if [[ "$ok_recreate" != "true" ]]; then
    echo "FAIL (Bug 327/339): recreated ${RD}.${N_REC} stuck — flags='$last_flags' disk_state='$last_disk'" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    on_node "$N_REC" drbdsetup status "$RD" 2>&1 | sed 's/^/  N_REC drbd: /' >&2 || true
    on_node "$N_PEER" drbdsetup status "$RD" 2>&1 | sed 's/^/  N_PEER drbd: /' >&2 || true
    exit 1
fi

# ---------------------------------------------------------------------
# Phase C: Bug 339 core contract — peer connection MUST land
# Connected/Established on BOTH directions, replication MUST reach
# Established (NOT a stuck Connecting/StandAlone/WFBitMap*).
# ---------------------------------------------------------------------
echo ">> Bug 339 contract: $N_PEER <-> $N_REC must reach Connected/Established (both directions)"

wait_conns_ok "$RD" "$N_PEER" "$N_REC" 90 \
    || {
        echo "FAIL (Bug 339): survivor $N_PEER -> $N_REC connection stuck" >&2
        echo "  last message: $(status_connection_state "$RD" "$N_PEER" "$N_REC")" >&2
        echo "  last replicationState: $(status_replication_state "$RD" "$N_PEER" "$N_REC")" >&2
        on_node "$N_PEER" drbdsetup status "$RD" 2>&1 | sed 's/^/  drbd: /' >&2 || true
        exit 1
    }

wait_conns_ok "$RD" "$N_REC" "$N_PEER" 90 \
    || {
        echo "FAIL (Bug 339): recreated $N_REC -> $N_PEER connection stuck" >&2
        echo "  last message: $(status_connection_state "$RD" "$N_REC" "$N_PEER")" >&2
        echo "  last replicationState: $(status_replication_state "$RD" "$N_REC" "$N_PEER")" >&2
        on_node "$N_REC" drbdsetup status "$RD" 2>&1 | sed 's/^/  drbd: /' >&2 || true
        exit 1
    }

# Bug 329-style check: replication state must converge to Established
# on the survivor's view of the recreated node. A WFBitMapS/T or stuck
# SyncTarget surfaces the same "stuck after recreate" symptom from a
# slightly different angle (kernel handshake succeeded but bitmap
# exchange wedged).
wait_replication_state "$RD" "$N_PEER" "$N_REC" "Established|SyncSource|PausedSyncS" 90 \
    || die "Bug 339 (replication): survivor $N_PEER -> $N_REC stuck (rep=$(status_replication_state "$RD" "$N_PEER" "$N_REC"))"

# Final convergence: both ends UpToDate + Established, no (NN%) suffix.
wait_sync_done "$RD" "$N_REC" "$N_PEER" 240 \
    || die "Bug 339 (sync done): ${N_REC}<->${N_PEER} never reached clean UpToDate+Established"

echo ">> r-d-then-r-c-stuck OK (Bug 339 pinned: recreated replica converges, no stuck connection)"
