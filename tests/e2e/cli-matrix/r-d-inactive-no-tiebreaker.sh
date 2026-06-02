#!/usr/bin/env bash
#
# usage: r-d-inactive-no-tiebreaker.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 387 (P1, user-reported, stand-observable).
#
# Reproduction from the operator stand (resource `test1`):
#
#   $ linstor r l
#   test1  worker-1  DRBD,STORAGE  INACTIVE
#   test1  worker-2  DRBD,STORAGE  UpToDate
#   test1  worker-3  DRBD,STORAGE  UpToDate
#
#   $ linstor r d worker-2 test1
#   SUCCESS: resource deleted: test1 on worker-2
#
#   $ linstor r l
#   test1  worker-1  DRBD,STORAGE  INACTIVE
#   test1  worker-2  DRBD,STORAGE  Connecting(worker-3)  Created   <-- WRONG: became a TieBreaker witness
#   test1  worker-3  DRBD,STORAGE  Unknown
#
# Root cause: an INACTIVE replica is `drbdadm down` (operator
# deactivation) — its DRBD device is not up, so it does NOT vote in the
# `quorum: majority` decision the auto-tiebreaker invariant defends. The
# RD reconciler's witness-decision counted the INACTIVE replica as a
# full diskful, so after the `r d` of one active diskful the topology
# looked like "2 diskful, 0 user-diskless, even parity" → it spuriously
# grew a TIE_BREAKER witness (1 active diskful + 1 witness = a 2-voter
# quorum with no majority protection). Upstream LINSTOR simply deletes
# the replica with no witness conversion.
#
# Fix: drop INACTIVE replicas from the voting set before the
# diskful/diskless split in ensureTiebreaker — they influence neither
# the diskful count nor the diskless/witness count.
#
# Unit pin: internal/controller/ensure_tiebreaker_inactive_bug_387_test.go
# (TestBug387InactiveReplicaNotCountedAsVotingDiskful).
# This L6 cell is the stand-side companion: drives the real
# python-linstor CLI sequence on the 3-diskful → deactivate-one →
# r-d-one shape and asserts that NO tiebreaker is spawned, leaving
# exactly two Resource rows (1 INACTIVE + 1 active diskful) with no
# TIE_BREAKER residue.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-387
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

cleanup() {
    # If a replica is still INACTIVE, re-activate it first so delete_rd
    # can drive a clean cascade.
    "${LCTL[@]}" resource activate "$N1" "$RD" 2>/dev/null || true
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> [Bug 387] shape-3r: 3-replica diskful RD (no auto-tiebreaker — odd parity)"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 256M >/dev/null
"${LCTL[@]}" resource create --auto-place=3 --storage-pool=stand "$RD" >/dev/null

echo ">> wait for steady state: 3 diskful UpToDate"
deadline=$(( $(date +%s) + 180 ))
ready=false
while (( $(date +%s) < deadline )); do
    n_diskful=$(linstor_diskful_count "$RD")
    n_total=$(linstor_replica_count "$RD")
    if [[ "$n_diskful" == "3" ]] && [[ "$n_total" == "3" ]]; then
        ok_count=0
        for n in "$N1" "$N2" "$N3"; do
            d=$(status_disk_state "$RD" "$n" 0 2>/dev/null || echo "")
            [[ "$d" == "UpToDate" ]] && ok_count=$(( ok_count + 1 ))
        done
        if (( ok_count == 3 )); then
            ready=true
            break
        fi
    fi
    sleep 3
done
if [[ "$ready" != "true" ]]; then
    echo "FAIL: never reached 3 diskful UpToDate within 180s" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi
echo "   3 diskful UpToDate on $N1 $N2 $N3"

# Deactivate worker-1 → INACTIVE flag. This is the deactivated, non-
# voting replica the witness policy must learn to ignore.
echo ">> linstor r deactivate $N1 $RD  (→ INACTIVE)"
"${LCTL[@]}" resource deactivate "$N1" "$RD" >/dev/null 2>&1 || {
    echo "FAIL: r deactivate $N1 $RD exited non-zero" >&2
    exit 1
}

echo ">> wait up to 30s for INACTIVE flag visible on $N1"
deadline=$(( $(date +%s) + 30 ))
inactive=false
while (( $(date +%s) < deadline )); do
    flags=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${N1}" \
        -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
    if [[ "$flags" == *"INACTIVE"* ]]; then
        inactive=true
        break
    fi
    sleep 2
done
if [[ "$inactive" != "true" ]]; then
    echo "FAIL: INACTIVE flag never landed on $N1 within 30s" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi
echo "   $N1 is INACTIVE; voting set is now {$N2, $N3} (2 active diskful)"

# Delete one of the two ACTIVE diskful replicas. The remaining voting
# set is 1 active diskful + 1 INACTIVE — and an INACTIVE replica is NOT
# a voting diskful, so there must be NO tiebreaker conversion. Upstream
# LINSTOR simply deletes the replica.
echo ">> linstor r d $N2 $RD  (physical delete of one active diskful)"
"${LCTL[@]}" resource delete "$N2" "$RD" >/dev/null 2>&1 || {
    echo "FAIL: r d $N2 $RD exited non-zero" >&2
    exit 1
}

# Wait for the deleted replica's CRD to physically disappear before
# asserting "no tiebreaker", so the count check acts on a settled shape.
wait_replica_absent "$RD" "$N2" 60 \
    || die "Bug 387: ${RD}.${N2} CRD never disappeared after r d"

# The reconciler runs asynchronously. Give it a fixed window to (wrongly,
# pre-fix) spawn a witness; if after the window no TIE_BREAKER exists and
# exactly the 2 expected rows remain, the fix holds.
echo ">> watch 30s: NO tiebreaker must be spawned; exactly 2 rows must remain"
deadline=$(( $(date +%s) + 30 ))
while (( $(date +%s) < deadline )); do
    tb=$(linstor_tiebreaker_node "$RD")
    if [[ -n "$tb" ]]; then
        echo "FAIL (Bug 387 regression): spurious TIE_BREAKER witness spawned on $tb" >&2
        echo "  an INACTIVE replica must not be counted as a voting diskful" >&2
        "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
        exit 1
    fi
    sleep 2
done

# Final shape assertion: exactly 2 Resource rows — 1 INACTIVE ($N1) and
# 1 active diskful ($N3) — and neither carries TIE_BREAKER / DISKLESS.
n_total=$(linstor_replica_count "$RD")
if [[ "$n_total" != "2" ]]; then
    echo "FAIL (Bug 387): expected 2 rows after r d, got $n_total" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

# $N1 must still be INACTIVE (and definitely not a witness).
n1_flags=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${N1}" \
    -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
if [[ "$n1_flags" != *"INACTIVE"* ]] || [[ "$n1_flags" == *"TIE_BREAKER"* ]]; then
    echo "FAIL (Bug 387): $N1 flags=$n1_flags, want INACTIVE with no TIE_BREAKER" >&2
    exit 1
fi

# $N3 must be the lone surviving active diskful: no DISKLESS / TIE_BREAKER.
n3_flags=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${N3}" \
    -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
if [[ "$n3_flags" == *"TIE_BREAKER"* ]] || [[ "$n3_flags" == *"DISKLESS"* ]]; then
    echo "FAIL (Bug 387): $N3 carries unexpected flags=$n3_flags" >&2
    exit 1
fi

# $N2 (the deleted node) must NOT have come back as a witness — the exact
# operator-reported symptom (the deleted diskful re-appearing as a TB).
if kubectl get "resources.blockstor.cozystack.io/${RD}.${N2}" >/dev/null 2>&1; then
    echo "FAIL (Bug 387): deleted node $N2 re-appeared — it was converted into a tiebreaker" >&2
    exit 1
fi

echo ">> r-d-inactive-no-tiebreaker OK (Bug 387 pinned: r d of an active diskful on a 1-active+1-INACTIVE residual spawns no tiebreaker)"
