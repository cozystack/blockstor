#!/usr/bin/env bash
#
# usage: respawn-standalone-wedge.sh WORK_DIR
#
# Respawn-StandAlone wedge (P0) regression — the "relocate / respawn
# after r d + r c when a TieBreaker is present" family.
#
# Live-reproduced sequence (linstor CLI), starting from 3 diskful
# UpToDate replicas on workers 1/2/3:
#
#   1. linstor r d <w1> <rd>   -> settles to w1=TieBreaker, w2+w3 diskful
#   2. linstor r d <w2> <rd>   -> settles to 1 diskful + 1 TieBreaker
#   3. linstor r c <w2> <rd>   -> RE-CREATE a diskful replica on w2
#
# THE BUG: on step 3 the freshly-recreated w2 wins the lowest-node-id
# auto-primary election while the surviving UpToDate sibling is briefly
# invisible in the informer cache (its Status churns through the rapid
# delete: quorum loss, del-peer, a fresh new-current-uuid on the
# survivor). The per-peer DiskState gate (anyDiskfulPeerHasData) returns
# false in that window, w2 force-primaries, mints an UNRELATED Current
# UUID, and the survivor declines the handshake:
#
#   uuid_compare()=unrelated-data by rule=history-both
#   Unrelated data, aborting!
#   conn( ... -> StandAlone )
#
# -> permanent mutual StandAlone that never auto-recovers; the resource
# never returns to all-UpToDate.
#
# THE FIX (pkg/dispatcher): gate the auto-primary seed additionally on
# the controller-persisted, append-only RD.Spec.Initialized latch. An
# already-initialized RD can never legitimately need a force-primary
# seed, and the latch is immune to the cache trail. The recreated
# replica then comes up Inconsistent and SyncTargets the survivor.
#
# ASSERTION: within a bounded timeout after the r c, ALL live replicas
# reconverge to UpToDate with NO lingering StandAlone / Connecting /
# Unknown on any connection (kernel ground truth).
#
# FileSystem/Type is set on the RD on purpose: it is what makes the
# satellite take the `primary --force` mkfs path on a fresh replica —
# the exact path that minted the unrelated UUID on the live wedge.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

if ! command -v linstor >/dev/null 2>&1; then
    echo "FAIL: linstor CLI not in PATH (apt install linstor-client)" >&2
    exit 1
fi

RD=${RD:-e2e-respawn-wedge}
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3
POOL=${STORPOOL:-stand}
SIZE=${SIZE:-128M}
RECONVERGE_TIMEOUT=${RECONVERGE_TIMEOUT:-180}

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/respawn-wedge-pf.log 2>&1 &
PF_PID=$!

dump_diag() {
    echo "---- dump: linstor r l ----"
    "${LCTL[@]}" resource list-volumes -r "$RD" 2>/dev/null || true
    for n in "$N1" "$N2" "$N3"; do
        echo "---- dump: drbdsetup status on $n ----"
        on_node "$n" drbdsetup status "$RD" --verbose 2>/dev/null || true
    done
    echo "---- dump: controller logs ----"
    kubectl -n "$NS" logs deploy/blockstor-controller --tail=80 2>/dev/null | grep -iE "$RD|auto-primary|StandAlone|unrelated|ensureTiebreaker" || true
}

cleanup() {
    local rc=$?
    if (( rc != 0 )); then
        dump_diag
    fi
    delete_rd "$RD" 2>/dev/null || true
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for port-forward to bind.
for _ in $(seq 1 30); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:$PF_PORT")

# Clean slate so the auto-primary election + reconverge assertions
# aren't perturbed by a previous scenario's residue.
delete_rd "$RD" 2>/dev/null || true

# ---------------------------------------------------------------------
# SETUP: 3 diskful UpToDate replicas, RD carries FileSystem/Type=ext4.
# ---------------------------------------------------------------------
echo ">> rd create $RD"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null

echo ">> rd set-property $RD FileSystem/Type ext4 (drives the mkfs force-primary path)"
"${LCTL[@]}" resource-definition set-property "$RD" FileSystem/Type ext4 >/dev/null

echo ">> vd create $RD $SIZE"
"${LCTL[@]}" volume-definition create "$RD" "$SIZE" >/dev/null

echo ">> r create on $N1 $N2 $N3 (3 diskful), pool=$POOL"
"${LCTL[@]}" resource create "$N1" "$RD" --storage-pool "$POOL" >/dev/null
"${LCTL[@]}" resource create "$N2" "$RD" --storage-pool "$POOL" >/dev/null
"${LCTL[@]}" resource create "$N3" "$RD" --storage-pool "$POOL" >/dev/null

echo ">> wait all 3 replicas UpToDate"
wait_uptodate "$RD" "$N1" "$N2"
wait_uptodate "$RD" "$N1" "$N3"

# RD.Spec.Initialized must have latched true once real data exists.
# This is the persisted signal the fix keys on; assert it so a future
# regression in the controller latch is caught here too.
deadline=$(( $(date +%s) + 60 ))
init_ok=false
while (( $(date +%s) < deadline )); do
    init=$(kubectl get resourcedefinition "$RD" -o jsonpath='{.spec.initialized}' 2>/dev/null || echo "")
    if [[ "$init" == "true" ]]; then
        init_ok=true
        break
    fi
    sleep 2
done
if [[ "$init_ok" != "true" ]]; then
    echo "FAIL: RD.Spec.Initialized never latched true after 3 UpToDate replicas (got: '$init')"
    exit 1
fi
echo ">> RD.Spec.Initialized=true (fix keys on this latch)"

# ---------------------------------------------------------------------
# STEP 1: r d <w1> -> diskless TieBreaker auto-created on w1.
# ---------------------------------------------------------------------
echo ">> [1] linstor r d $N1 $RD"
"${LCTL[@]}" resource delete "$N1" "$RD" >/dev/null
wait_uptodate "$RD" "$N2" "$N3"

# ---------------------------------------------------------------------
# STEP 2: r d <w2> -> drops to 1 diskful + 1 TieBreaker.
# ---------------------------------------------------------------------
echo ">> [2] linstor r d $N2 $RD"
"${LCTL[@]}" resource delete "$N2" "$RD" >/dev/null

# Give the controller a beat to settle the witness/quorum churn — this
# is the window that, pre-fix, lets the cache trail hide the survivor.
sleep 5

# ---------------------------------------------------------------------
# STEP 3: r c <w2> -> RE-CREATE the diskful replica. This is the trigger.
# ---------------------------------------------------------------------
echo ">> [3] linstor r c $N2 $RD --storage-pool $POOL (the respawn trigger)"
"${LCTL[@]}" resource create "$N2" "$RD" --storage-pool "$POOL" >/dev/null

# reconverged_with_tiebreaker <rd> <node> [vol] — kernel ground truth for
# a 2-diskful + 1-tiebreaker RD: prints "ok" iff, read from <node>'s
# `drbdsetup status --json`, the local disk is UpToDate, EVERY connection
# is Connected/Established, and every DISKFUL peer (peer-disk != Diskless)
# is UpToDate. The diskless tiebreaker peer (peer-disk:Diskless) is
# expected and accepted. A StandAlone/Connecting connection or a
# non-UpToDate diskful peer keeps us waiting — so the respawn wedge is
# still a real failure. Empty/parse failure prints nothing.
reconverged_with_tiebreaker() {
    local rd=$1 node=$2 vol=${3:-0}
    on_node "$node" drbdsetup status "$rd" --json 2>/dev/null | jq -r \
        --argjson v "$vol" '
        ([.[0].devices[]? | select(.volume==$v) | ."disk-state"] | first) as $loc
        | [.[0].connections[]?] as $conns
        | ([$conns[] | ."connection-state"]) as $cstates
        | ([$conns[] | .peer_devices[]? | select(.volume==$v)
             | select(."peer-disk-state" != "Diskless")
             | ."peer-disk-state"]) as $diskful_peers
        | if ($loc=="UpToDate"
              and ($conns | length) > 0
              and ($cstates | all(. == "Connected" or . == "Established"))
              and ($diskful_peers | length) > 0
              and ($diskful_peers | all(. == "UpToDate")))
          then "ok" else "no" end' \
        2>/dev/null || true
}

# ---------------------------------------------------------------------
# ASSERTION: reconverge to all-UpToDate with NO StandAlone/Connecting.
# w3 is the surviving diskful replica and is always present; assert
# kernel ground truth from w3 (its status frame reports every peer).
# ---------------------------------------------------------------------
echo ">> waiting up to ${RECONVERGE_TIMEOUT}s for reconvergence (no StandAlone)"
deadline=$(( $(date +%s) + RECONVERGE_TIMEOUT ))
converged=false
while (( $(date +%s) < deadline )); do
    # Hard fail-fast: a StandAlone on any live connection is the wedge
    # signature. Surface it immediately rather than waiting the full
    # timeout — but only after giving the handshake a moment to start.
    if (( $(date +%s) > deadline - RECONVERGE_TIMEOUT + 20 )); then
        for n in "$N2" "$N3"; do
            sa=$(on_node "$n" drbdsetup status "$RD" --verbose 2>/dev/null \
                | grep -oE 'connection:[A-Za-z]+' | grep -c 'StandAlone' || true)
            if [[ "${sa:-0}" -gt 0 ]]; then
                echo "FAIL: StandAlone connection observed on $n after respawn — the wedge reproduced"
                dump_diag
                exit 1
            fi
        done
    fi

    if [[ "$(reconverged_with_tiebreaker "$RD" "$N3")" == "ok" ]]; then
        converged=true
        break
    fi
    sleep 3
done

if [[ "$converged" != "true" ]]; then
    echo "FAIL: $RD did not reconverge to all-UpToDate within ${RECONVERGE_TIMEOUT}s after respawn"
    dump_diag
    exit 1
fi

# Belt-and-braces: no lingering StandAlone / Connecting / Unknown on ANY
# live replica's connections (kernel ground truth from w2 and w3).
for n in "$N2" "$N3"; do
    bad=$(on_node "$n" drbdsetup status "$RD" --verbose 2>/dev/null \
        | grep -oE 'connection:[A-Za-z]+' \
        | grep -vE 'connection:(Connected|Established)' || true)
    if [[ -n "$bad" ]]; then
        echo "FAIL: $n still has non-Connected connection(s) after reconvergence:"
        echo "$bad"
        dump_diag
        exit 1
    fi
done

echo ">> RESPAWN-WEDGE OK — all replicas reconverged UpToDate, no StandAlone after r d x2 + r c"
