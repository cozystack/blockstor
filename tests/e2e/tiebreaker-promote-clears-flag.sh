#!/usr/bin/env bash
#
# usage: tiebreaker-promote-clears-flag.sh WORK_DIR
#
# Bug-hunt v2 B.4 regression catcher: `linstor r td --storage-pool
# <pool> <node> <rd>` (promote an auto-managed TIE_BREAKER witness
# back to a diskful peer) MUST clear BOTH the DISKLESS and the
# TIE_BREAKER flags on the Resource CRD. Before the fix only
# DISKLESS was stripped, leaving the promoted diskful peer marked
# `TIE_BREAKER` on Spec.Flags. The latent failure mode:
# `filterTieBreaker` and `splitByDiskless` key on the flag (not the
# observed disk state), so the next `ensureTiebreaker` pass double-
# counts the slot as both a diskful and a witness and the witness
# invariant is silently violated.
#
# Lives in tests/e2e/ (not unit) because the regression's blast
# radius is the controller's witness math: a CRD-side unit test
# pins the REST handler, but only a real reconcile pass that walks
# `filterTieBreaker` proves the flag drift doesn't reach
# ensureTiebreaker.

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

RD=tb-promote-clears-flag
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

CONVERGE=${CONVERGE:-60}

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/tb-promote-clears-flag-pf.log 2>&1 &
PF_PID=$!

dump_diag() {
    echo "---- dump: linstor r l -r $RD ----"
    "${LCTL[@]}" r l -r "$RD" 2>&1 || true
    echo "---- dump: Resource CRDs (flags) ----"
    for n in "$N1" "$N2" "$N3"; do
        kubectl get "resources.blockstor.cozystack.io/${RD}.${n}" \
            -o jsonpath='{.metadata.name}{" flags="}{.spec.flags}{"\n"}' \
            2>/dev/null || true
    done
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

delete_rd "$RD" 2>/dev/null || true

# ---- STEP 1: 2 diskful on N1+N2 → auto-TB lands on N3 -------------

echo ">> create RD $RD with 2 diskful on $N1, $N2 (auto-TB lands on $N3)"
"${LCTL[@]}" resource-definition create "$RD"
"${LCTL[@]}" volume-definition create "$RD" 64M
"${LCTL[@]}" resource create "$N1" "$RD" --storage-pool stand
"${LCTL[@]}" resource create "$N2" "$RD" --storage-pool stand

echo ">> wait both diskful UpToDate (<=180s)"
wait_disk_state "$RD" "$N1" UpToDate 180 0
wait_disk_state "$RD" "$N2" UpToDate 180 0

echo ">> wait auto-TB to appear on $N3 (<=${CONVERGE}s)"
wait_disk_state "$RD" "$N3" Diskless "$CONVERGE" 0

# Pin the pre-promote shape: the witness CRD must carry BOTH
# DISKLESS and TIE_BREAKER (this is the only state the bug
# applies to — a non-witness diskless would have only DISKLESS).
tb_flags=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${N3}" \
    -o jsonpath='{.spec.flags}' 2>/dev/null)

if [[ "$tb_flags" != *"DISKLESS"* || "$tb_flags" != *"TIE_BREAKER"* ]]; then
    echo "FAIL: pre-promote shape — auto-TB on $N3 must carry both DISKLESS + TIE_BREAKER, got: $tb_flags"
    dump_diag
    exit 1
fi

echo "   pre-promote OK: ${N3} flags=${tb_flags}"

# ---- STEP 2: promote the auto-TB to diskful ------------------------

echo ">> linstor r td --storage-pool zfs-thin $N3 $RD (promote auto-TB to diskful)"
"${LCTL[@]}" resource toggle-disk "$N3" "$RD" --storage-pool zfs-thin

echo ">> wait $N3 UpToDate after promote (<=180s)"
wait_disk_state "$RD" "$N3" UpToDate 180 0

# ---- STEP 3: assert flags on $N3 ----------------------------------
#
# Pre-fix observed state: spec.flags = [TIE_BREAKER] (DISKLESS was
# cleared, TIE_BREAKER was not). Post-fix the toggle-to-diskful
# handler strips both, so the promoted Resource CRD must end with
# a flags-free Spec — matching the shape of the other two diskful
# replicas on $N1 and $N2.
echo ">> assert promoted Resource CRD has no DISKLESS and no TIE_BREAKER flag"

deadline=$(( $(date +%s) + CONVERGE ))
final_flags=""

while (( $(date +%s) < deadline )); do
    final_flags=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${N3}" \
        -o jsonpath='{.spec.flags}' 2>/dev/null)

    if [[ "$final_flags" != *"DISKLESS"* && "$final_flags" != *"TIE_BREAKER"* ]]; then
        echo ">> PASS: $N3 flags=${final_flags} (DISKLESS and TIE_BREAKER both cleared)"
        exit 0
    fi

    sleep 2
done

echo "ASSERT FAILED: promoted Resource CRD on $N3 still carries a witness flag"
echo "  got flags=${final_flags}"
echo "  expected: no DISKLESS, no TIE_BREAKER (diskful peer should not be marked a witness)"
echo "  see comment in handleResourceToggleDiskToDiskful for the contract"
exit 1
