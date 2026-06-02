#!/usr/bin/env bash
#
# usage: n-rst-recreates-tiebreaker.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 386 (user-reported, stand-observable).
#
# Reproduction from the operator stand:
#
#   $ linstor r l
#   test  dev-worker-1  …  UpToDate
#   test  dev-worker-2  …  UpToDate
#   test  dev-worker-3  …  TieBreaker
#
#   $ linstor n evacuate dev-worker-3     # drain the witness node
#   $ linstor r l
#   test  dev-worker-1  …  UpToDate
#   test  dev-worker-2  …  UpToDate       ← witness collapsed (EVICTED)
#
#   $ linstor n rst dev-worker-3          # node recovered
#   $ linstor r l
#   test  dev-worker-1  …  UpToDate
#   test  dev-worker-2  …  UpToDate
#   # TieBreaker was NOT recreated        ← WRONG: quorum/split-brain risk
#
# Root cause: the RD reconciler watched only RDs and Resources, never
# Nodes. `pickTiebreakerNode` / `isDisabledNode` exclude an EVICTED node
# as a witness candidate, so while dev-worker-3 was drained the witness
# could not be (re)placed. After `n rst` cleared the EVICTED flag,
# NOTHING enqueued the RD — so ensureTiebreaker never re-ran and the
# resource sat at two diskful UpToDate replicas with no witness until
# the next periodic re-sync (a quorum=majority split-brain hazard in
# between).
#
# Fix: a Node watch on the RD reconciler (nodeDrainFlagChanged →
# enqueueRDsForNode) re-enqueues every RD when a node's EVICTED/LOST
# flag set transitions, so `n rst` re-runs the tiebreaker invariant and
# the witness is re-placed.
#
# Unit pin: internal/controller/ensure_tiebreaker_test.go
# (TestBug386NodeRestoreRecreatesTiebreaker). This L6 cell is the
# stand-side companion: drives the real python-linstor CLI sequence on
# the 2r-tb shape, evacuates the witness node to collapse the witness,
# restores it with `n rst`, and asserts the TIE_BREAKER reappears in
# `linstor r l` within 60s — leaving the diskful+diskful+TB shape.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-386
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

cleanup() {
    # Best-effort restore in case the test bailed mid-evacuate, so the
    # shared stand isn't left with an EVICTED node.
    "${LCTL[@]}" node restore "$N3" >/dev/null 2>&1 || true
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> [Bug 386] shape-2r-tb: 2-replica RD + auto-tiebreaker"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 256M >/dev/null
"${LCTL[@]}" resource create --auto-place=2 --storage-pool=stand "$RD" >/dev/null

echo ">> wait for steady state: 2 diskful UpToDate + 1 TIE_BREAKER witness"
deadline=$(( $(date +%s) + 180 ))
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
        tb_node=$tb
        break
    fi
    sleep 3
done
if [[ -z "$tb_node" ]]; then
    echo "FAIL: never reached steady state (2 diskful UpToDate + TIE_BREAKER) within 180s" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi
echo "   tiebreaker landed on: $tb_node"

# Evacuate the witness node. The TIE_BREAKER witness lives on a node
# the autoplacer/tiebreaker treats as a candidate; EVICTED removes it
# from the candidate set, so the witness collapses (no other spare node
# in a 3-worker cluster with 2 diskful already placed).
echo ">> linstor n evacuate $tb_node  (drain the witness node → EVICTED)"
"${LCTL[@]}" node evacuate "$tb_node" >/dev/null 2>&1 || {
    echo "FAIL: n evacuate exited non-zero" >&2
    exit 1
}

echo ">> wait up to 60s for the witness on $tb_node to collapse"
deadline=$(( $(date +%s) + 60 ))
collapsed=false
while (( $(date +%s) < deadline )); do
    flags=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${tb_node}" \
        -o jsonpath='{.spec.flags}' 2>/dev/null || echo "ABSENT")
    if [[ "$flags" == "ABSENT" ]] || { [[ "$flags" != *"TIE_BREAKER"* ]]; }; then
        collapsed=true
        break
    fi
    sleep 2
done
if [[ "$collapsed" != "true" ]]; then
    echo "FAIL: witness on $tb_node never collapsed after n evacuate within 60s" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi
echo "   witness collapsed (node EVICTED); RD now 2 diskful, no witness"

# Restore the node. This is the load-bearing Bug 386 step: clearing
# EVICTED must re-run the tiebreaker invariant so the witness reappears.
echo ">> linstor n rst $tb_node  (node recovered → EVICTED cleared)"
"${LCTL[@]}" node restore "$tb_node" >/dev/null 2>&1 || {
    echo "FAIL: n rst exited non-zero" >&2
    exit 1
}

echo ">> wait up to 60s for the tiebreaker to be RE-created after n rst"
deadline=$(( $(date +%s) + 60 ))
recreated=false
last_rows=""
while (( $(date +%s) < deadline )); do
    tb=""
    for n in "$N1" "$N2" "$N3"; do
        flags=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${n}" \
            -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
        if [[ "$flags" == *"TIE_BREAKER"* ]]; then
            tb=$n
        fi
    done
    last_rows=$(kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="${RD}." '$1 ~ "^"rd {print $1}' || true)
    if [[ -n "$tb" ]]; then
        recreated=true
        break
    fi
    sleep 3
done

if [[ "$recreated" != "true" ]]; then
    echo "FAIL (Bug 386 regression): tiebreaker NOT recreated within 60s of n rst" >&2
    echo "  CRD rows for ${RD}:" >&2
    printf '    %s\n' $last_rows >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

# Belt-and-suspenders: the operator-visible `linstor r l` surface the
# bug report cited must show exactly one TIE_BREAKER row plus the two
# diskful replicas.
wire=$(linstor_r_l_json "$RD")
n_tb=$(printf '%s' "$wire" \
    | jq -r '.[][] | select((.rsc_flags // []) | index("TIE_BREAKER")) | .name' 2>/dev/null \
    | wc -l | tr -d ' ' || echo 0)
if [[ "$n_tb" != "1" ]]; then
    echo "FAIL (Bug 386): linstor r l shows $n_tb TIE_BREAKER rows for $RD, want 1" >&2
    printf '%s\n' "$wire" | jq '.[][]| {name, node_name, flags: .rsc_flags}' 2>/dev/null >&2 || true
    exit 1
fi

echo ">> n-rst-recreates-tiebreaker OK (Bug 386 pinned: tiebreaker recreated after n rst)"
