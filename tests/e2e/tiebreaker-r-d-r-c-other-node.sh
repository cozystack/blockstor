#!/usr/bin/env bash
#
# usage: tiebreaker-r-d-r-c-other-node.sh WORK_DIR
#
# Regression catcher for the "diskful relocate → StandAlone wedge"
# pattern. Topology: 3-node cluster with 2 diskful + 1 TIE_BREAKER.
# When the operator deletes a diskful on node A and re-creates a
# diskful on the previous TIE_BREAKER host C, the controller
# reshuffles DRBDNodeID allocations: node A returns as the new
# TIE_BREAKER but at a freshly-allocated peer-slot the on-disk
# metadata of the surviving diskful nodes has never used. The
# default-initialised v09 metadata for the new peer-slot has a
# non-zero bitmap-uuid that the kernel reads as
# `peer-disk:Outdated`. When `drbdadm adjust` then issues
# `drbdsetup peer-device-options --bitmap=no` (the declarative
# wire form for a `disk none` peer in the .res), the kernel
# refuses with:
#
#     test: Failure: (162) Invalid configuration request
#     additional info from kernel:
#     Can not drop the bitmap when both sides have a disk
#
# The first adjust fails leaving the slot StandAlone with peer-
# device entries registered (kernel allocated them via new-peer
# before peer-device-options fired). The next reconcile's
# shouldSkipNetOnAdjust gate (StandAlone + peer-devices-present)
# now permanently misfires, so subsequent adjusts skip the net
# section and the slot stays StandAlone forever. The user-visible
# symptom is the surviving diskful's `linstor r l` row showing
# `Conns: StandAlone(<departed-diskful>)` indefinitely while the
# returning TIE_BREAKER spins in `Connecting(<survivor>)`.
#
# This scenario lives in tests/e2e/ (not unit) because the failure
# mode is a four-way interaction:
#   - controller-side DRBDNodeID allocator (Bug 87 / Bug 302)
#   - satellite-side .res renderer (peer.<n>.diskless prop)
#   - drbdadm/drbdsetup peer-device-options semantics
#   - DRBD-9 kernel v09 default metadata state
# No mock can fake all four — only a real QEMU+Talos cluster can
# trigger the kernel-side bitmap-uuid behavior. See
# feedback_tiebreaker_e2e_must_exist.md in MEMORY.md.

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

RD=tb-relocate-wedge
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

# Per-step convergence budget. Worst observed re-converge after
# the wedge fix is ~25s on a healthy QEMU stand (down + up +
# adjust + handshake + bitmap-no acceptance); 90s gives 3x
# headroom for CI noise.
CONVERGE=${CONVERGE:-90}

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/tb-relocate-wedge-pf.log 2>&1 &
PF_PID=$!

dump_diag() {
    echo "---- dump: linstor r l -r $RD ----"
    "${LCTL[@]}" r l -r "$RD" 2>&1 || true
    echo "---- dump: per-node drbdsetup status ----"
    for n in "$N1" "$N2" "$N3"; do
        echo "  -- $n --"
        on_node "$n" drbdsetup status "$RD" 2>&1 || true
    done
    echo "---- dump: Resource.Status.connections ----"
    for n in "$N1" "$N2" "$N3"; do
        echo "  -- $n --"
        kubectl get "resources.blockstor.cozystack.io/${RD}.${n}" \
            -o jsonpath='{range .status.connections[*]}{.peerNodeName}{"="}{.message}{"; "}{end}{"\n"}' \
            2>/dev/null || true
    done
    echo "---- dump: kubectl logs -n $NS daemonset/blockstor-satellite --tail=80 ----"
    kubectl logs -n "$NS" daemonset/blockstor-satellite --tail=80 2>/dev/null \
        | grep -E "(bitmap|StandAlone|del-peer|adjust|test)" | tail -40 || true
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

for _ in $(seq 1 30); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:$PF_PORT")

# Clean any leftover state from a prior run.
delete_rd "$RD" 2>/dev/null || true

# assert_no_standalone <rd> — fail unless every (node,peer) pair
# in Resource.Status.connections has a non-StandAlone message
# across all three workers. Polls up to $CONVERGE seconds, exits
# 0 on first all-clear observation.
assert_no_standalone() {
    local rd=$1
    local deadline=$(( $(date +%s) + CONVERGE ))
    local saw_sa=""
    while (( $(date +%s) < deadline )); do
        saw_sa=""
        for n in "$N1" "$N2" "$N3"; do
            for p in "$N1" "$N2" "$N3"; do
                [[ "$n" == "$p" ]] && continue
                local m
                m=$(status_connection_state "$rd" "$n" "$p")
                if [[ "$m" == "StandAlone" || "$m" == StandAlone* ]]; then
                    saw_sa="${n}->${p}=${m}"
                fi
            done
        done
        if [[ -z "$saw_sa" ]]; then
            return 0
        fi
        sleep 3
    done
    echo "FAIL: a connection stayed in StandAlone after ${CONVERGE}s (last seen: $saw_sa)" >&2
    return 1
}

# ---- STEP 1: 2 diskful on N1+N2, TIE_BREAKER on N3 ----------------

echo ">> create RD $RD with 2 diskful on $N1, $N2 (auto-place=2 picks the TB host)"
"${LCTL[@]}" resource-definition create "$RD"
"${LCTL[@]}" volume-definition create "$RD" 64M
"${LCTL[@]}" resource create "$N1" "$RD" --storage-pool stand
"${LCTL[@]}" resource create "$N2" "$RD" --storage-pool stand

echo ">> wait both diskful replicas UpToDate (<=180s)"
wait_disk_state "$RD" "$N1" UpToDate 180 0
wait_disk_state "$RD" "$N2" UpToDate 180 0

# The auto-managed TIE_BREAKER landed on the only uninvolved node.
echo ">> wait for the TIE_BREAKER witness to settle on $N3"
wait_disk_state "$RD" "$N3" Diskless 60 0

# Baseline assert: no StandAlone in the initial steady state.
if ! assert_no_standalone "$RD"; then
    echo "FAIL: pre-step baseline already shows StandAlone — test cannot trust the wedge assert"
    exit 1
fi
echo "   baseline OK: 2 diskful + 1 TB, all peers connected"

# ---- STEP 2: r d worker-2 → 1 diskful left + (transient) TB --------

echo ">> linstor r d $N2 $RD (delete the second diskful)"
"${LCTL[@]}" resource delete "$N2" "$RD"

# Allow the controller to reach the post-r-d steady state. We
# don't pin a specific tiebreaker shape here — the post-r-d
# topology may be 1-diskful + 0-witness (Bug 338 carve-out) or
# 1-diskful + 1-witness depending on transient observations.
# What we need before triggering the wedge is just that the
# delete finished propagating to the kernel.
echo ">> wait for $N2 Resource to be gone from the controller"
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    if ! kubectl get "resources.blockstor.cozystack.io/${RD}.${N2}" >/dev/null 2>&1; then
        break
    fi
    sleep 2
done

# ---- STEP 3: r c worker-3 → diskful relocate, expose the wedge ----
#
# Pre-fix symptom: the controller re-allocates DRBDNodeID for
# both N3 (now becoming diskful, fresh slot) and N2 (returning as
# TIE_BREAKER on a peer-slot that the surviving diskful's v09
# metadata has not yet used). adjust on the surviving diskful's
# satellite fails with `Can not drop the bitmap when both sides
# have a disk`. The kernel slot for the returning TB stays
# StandAlone; shouldSkipNetOnAdjust then misfires forever.

echo ">> linstor r c $N3 $RD (relocate diskful onto the old TIE_BREAKER host)"
"${LCTL[@]}" resource create "$N3" "$RD" --storage-pool stand

echo ">> wait $N3 UpToDate after relocate (<=180s)"
wait_disk_state "$RD" "$N3" UpToDate 180 0

# This is the assertion the bug fails: the surviving diskful
# (N1) must NOT have a StandAlone slot toward the returning
# TIE_BREAKER (N2), and the returning TB (N2) must not be
# spinning in Connecting toward N1 either. We check the union
# across all 6 (node,peer) pairs because either side could be
# the one that misfires depending on race order.

echo ">> assert no StandAlone slots across the 3-node mesh (<=${CONVERGE}s)"
if ! assert_no_standalone "$RD"; then
    echo "ASSERT FAILED: r d $N2 + r c $N3 left a StandAlone slot in the kernel"
    echo "  This is the bitmap-bothdisks wedge — see scenario header for the chain."
    exit 1
fi

# Stability tail: hold for 15s and re-check. The pre-fix wedge
# is sticky (StandAlone slots don't self-heal), but a flaky
# fix that resolves on the first reconcile then bounces back on
# a subsequent adjust would slip past a single-shot probe.
echo ">> stability tail: hold 15s, no StandAlone may reappear"
deadline=$(( $(date +%s) + 15 ))
while (( $(date +%s) < deadline )); do
    for n in "$N1" "$N2" "$N3"; do
        for p in "$N1" "$N2" "$N3"; do
            [[ "$n" == "$p" ]] && continue
            m=$(status_connection_state "$RD" "$n" "$p")
            if [[ "$m" == "StandAlone" || "$m" == StandAlone* ]]; then
                echo "ASSERT FAILED (oscillation): ${n}->${p} flipped to ${m} during stability tail"
                exit 1
            fi
        done
    done
    sleep 2
done

echo ">> PASS: r d + r c relocate converged without StandAlone wedge"
