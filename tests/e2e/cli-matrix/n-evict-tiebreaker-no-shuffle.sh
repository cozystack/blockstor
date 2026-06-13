#!/usr/bin/env bash
#
# usage: n-evict-tiebreaker-no-shuffle.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 385 (P1, user-reported, stand-observable).
#
# Reproduction from the dev stand:
#
#   $ linstor r l
#   test  worker-1  …  UpToDate
#   test  worker-2  …  UpToDate
#   test  worker-3  …  TieBreaker
#
#   $ linstor n e dev-worker-3
#   SUCCESS
#
#   $ linstor n l
#   dev-worker-1  SATELLITE  Online
#   dev-worker-2  SATELLITE  Online
#   dev-worker-3  SATELLITE  EVICTED
#   $ linstor r l
#   test  worker-1  …  UpToDate
#   test  worker-2  …  TieBreaker   ← WRONG: was UpToDate, demoted to TB
#   test  worker-3  …  UpToDate     ← EVICTED node still holds a diskful
#
# Operator summary: "Если евиктить ноду, при свободной TieBreaker
# реплике, то евикт не произойдёт" — the evict does not actually take
# effect.
#
# Root cause: ensureTiebreaker counted the TIE_BREAKER witness sitting on
# the just-EVICTED node as a live witness, so the witness invariant read
# as satisfied (diskful=2 + witness=1) and the reconciler never relocated
# the witness off the drained node. A healthy diskful must NEVER be
# demoted to TIE_BREAKER.
#
# Fix: replicas on EVICTED / LOST nodes are draining placements, not live
# ones — the witness / quorum decision ignores them and a TIE_BREAKER
# stranded on a disabled node is reaped so a fresh one lands on a healthy
# spare (or quorum falls to off when none remains).
#
# Unit pin: internal/controller/ensure_tiebreaker_evict_bug_385_test.go
# (TestBug385EvictTiebreakerNodeReapsStrandedWitness +
#  TestBug385EvictTiebreakerNodeRelocatesWitnessToSpare).
#
# This L6 cell is the stand-side companion: it builds the 2r+tb shape on
# 3 workers, drains the tiebreaker node into EVICTED via the real
# `linstor node evacuate` CLI (linstor-client has no `node evict` verb —
# the repro's `n e` reached EVICTED through the evacuate/auto-evict
# machinery), and asserts that within 30s (a) the EVICTED node no longer
# hosts a witness, and (b) BOTH original diskful replicas are still
# diskful — no healthy UpToDate replica was demoted to TIE_BREAKER.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-385
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

EVICTED_NODE=""
cleanup() {
    # Restore any node we evicted so the shared stand is left healthy
    # for the next cell, BEFORE deleting the RD.
    if [[ -n "$EVICTED_NODE" ]]; then
        "${LCTL[@]}" node restore "$EVICTED_NODE" >/dev/null 2>&1 || true
    fi
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> [Bug 385] shape-2r-tb: 2-replica RD + auto-tiebreaker"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 256M >/dev/null
"${LCTL[@]}" resource create --auto-place=2 --storage-pool=stand "$RD" >/dev/null

echo ">> wait for steady state: 2 diskful UpToDate + 1 TIE_BREAKER witness"
deadline=$(( $(date +%s) + 180 ))
uptodate_pair=""
tb_node=""
while (( $(date +%s) < deadline )); do
    pair=()
    tb=""
    for n in "$N1" "$N2" "$N3"; do
        flags=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${n}" \
            -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
        if [[ "$flags" == *"TIE_BREAKER"* ]]; then
            tb=$n
            continue
        fi
        d=$(status_disk_state "$RD" "$n" 0)
        if [[ "$d" == "UpToDate" ]]; then
            pair+=("$n")
        fi
    done
    if (( ${#pair[@]} >= 2 )) && [[ -n "$tb" ]]; then
        uptodate_pair="${pair[0]} ${pair[1]}"
        tb_node=$tb
        break
    fi
    sleep 3
done
if [[ -z "$uptodate_pair" ]] || [[ -z "$tb_node" ]]; then
    echo "FAIL: never reached steady state (2 diskful UpToDate + TIE_BREAKER) within 180s" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi
echo "   diskful pair: $uptodate_pair  tiebreaker: $tb_node"

# Capture the two diskful nodes so we can assert neither is demoted.
DISKFUL_A=$(echo "$uptodate_pair" | awk '{print $1}')
DISKFUL_B=$(echo "$uptodate_pair" | awk '{print $2}')

# Drain the tiebreaker node into the EVICTED state. NOTE: linstor-client
# (1.27.1) has NO `node evict` verb — eviction is either automatic
# (auto-evict) or operator-driven via `node evacuate`, which stamps the
# same EVICTED flag this cell asserts on. The original `node evict`
# invocation died in the CLI's argparse (exit 2, no REST call) and the
# cell could never pass (BUG-040 triage).
echo ">> linstor n evacuate $tb_node  (drain the tiebreaker node → EVICTED)"
EVICTED_NODE=$tb_node
err_file=$(mktemp)
if ! "${LCTL[@]}" node evacuate "$tb_node" >/dev/null 2>"$err_file"; then
    echo "FAIL: n evacuate exited non-zero" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

# The node must show EVICTED in `linstor n l` (evict took effect at the
# node level).
echo ">> confirm $tb_node is flagged EVICTED"
deadline=$(( $(date +%s) + 30 ))
node_evicted=false
while (( $(date +%s) < deadline )); do
    nflags=$(kubectl get "nodes.blockstor.cozystack.io/${tb_node}" \
        -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
    if [[ "$nflags" == *"EVICTED"* ]]; then
        node_evicted=true
        break
    fi
    sleep 2
done
if [[ "$node_evicted" != "true" ]]; then
    echo "FAIL: node $tb_node never reached EVICTED flag within 30s" >&2
    exit 1
fi

echo ">> wait up to 30s for the tiebreaker on EVICTED $tb_node to be relocated/reaped"
deadline=$(( $(date +%s) + 30 ))
reaped=false
while (( $(date +%s) < deadline )); do
    # The witness must no longer sit on the evicted node.
    flags=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${tb_node}" \
        -o jsonpath='{.spec.flags}' 2>/dev/null || echo "__absent__")
    if [[ "$flags" == "__absent__" ]] || [[ "$flags" != *"TIE_BREAKER"* ]]; then
        reaped=true
        break
    fi
    sleep 2
done

if [[ "$reaped" != "true" ]]; then
    echo "FAIL (Bug 385 regression): TIE_BREAKER still pinned to EVICTED $tb_node within 30s" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

# The load-bearing invariant: NEITHER healthy diskful was demoted to
# TIE_BREAKER (the exact wrong behaviour from the repro where worker-2
# went UpToDate → TieBreaker).
echo ">> assert neither healthy diskful ($DISKFUL_A, $DISKFUL_B) was demoted to TIE_BREAKER"
for n in "$DISKFUL_A" "$DISKFUL_B"; do
    flags=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${n}" \
        -o jsonpath='{.spec.flags}' 2>/dev/null || echo "__absent__")
    if [[ "$flags" == "__absent__" ]]; then
        echo "FAIL (Bug 385): healthy diskful on $n disappeared after evict; flags=$flags" >&2
        exit 1
    fi
    if [[ "$flags" == *"TIE_BREAKER"* ]]; then
        echo "FAIL (Bug 385 regression): healthy diskful on $n was demoted to TIE_BREAKER; flags=$flags" >&2
        "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
        exit 1
    fi
    if [[ "$flags" == *"DISKLESS"* ]]; then
        echo "FAIL (Bug 385): healthy diskful on $n gained DISKLESS after evict; flags=$flags" >&2
        exit 1
    fi
done

echo ">> n-evict-tiebreaker-no-shuffle OK (Bug 385 pinned: evict relocates the witness, no healthy diskful demoted)"
