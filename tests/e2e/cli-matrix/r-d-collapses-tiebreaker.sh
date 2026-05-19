#!/usr/bin/env bash
#
# usage: r-d-collapses-tiebreaker.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 338 (user-reported, stand-observable).
#
# Reproduction from the e2e2 stand:
#
#   $ linstor r l
#   test  worker-1  …  UpToDate
#   test  worker-2  …  UpToDate
#   test  worker-3  …  TieBreaker
#
#   $ linstor r d worker-1 test
#   SUCCESS
#
#   $ linstor r l
#   test  worker-2  …  UpToDate
#   test  worker-3  …  TieBreaker         ← WRONG: should be gone
#
# Root cause: the keep-branch of shouldTieBreakerExist preserved the
# witness whenever diskful ∈ [1, 3), without checking whether a
# non-witness diskless was present. With diskful dropped to 1 (via
# `r d` — not a toggle), the lone diskful + lone witness left a
# 2-voter cluster with no real majority on failure. Per upstream
# LINSTOR CtrlAutoQuorumTask, the witness should be torn down once
# diskful < 2 and no user-diskless needs it as a tie-breaker.
#
# Unit pin: internal/controller/ensure_tiebreaker_test.go
# (TestBug338TiebreakerCollapsesWhenDiskfulDropsToOne).
# This L6 cell is the stand-side companion: drives the real
# python-linstor CLI sequence on the 2r-tb shape and asserts the
# tiebreaker actually disappears from `linstor r l` within 30s of
# the `r d`, leaving exactly one Resource row (the surviving
# diskful) with no DISKLESS / TIE_BREAKER residue.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-338
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> [Bug 338] shape-2r-tb: 2-replica RD + auto-tiebreaker"
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
        flags=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${n}" \
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

# Pick one of the diskful nodes to delete. Either is fine — the
# invariant is "the tiebreaker collapses regardless of which diskful
# leaves".
DELETE_NODE=$(echo "$uptodate_pair" | awk '{print $1}')
echo ">> linstor r d $DELETE_NODE $RD  (Bug 338 trigger: real delete, not toggle)"
err_file=$(mktemp)
if ! "${LCTL[@]}" resource delete "$DELETE_NODE" "$RD" 2>"$err_file"; then
    rc=$?
    echo "FAIL: r d exited $rc" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

echo ">> wait up to 30s for tiebreaker on $tb_node to be collapsed"
deadline=$(( $(date +%s) + 30 ))
collapsed=false
last_rows=""
while (( $(date +%s) < deadline )); do
    # Count CRDs for this RD — exactly 1 row should remain (the
    # surviving diskful).
    rows=$(kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
        | awk -v rd="${RD}." '$1 ~ "^"rd {print $1}' || true)
    n_rows=$(printf '%s\n' "$rows" | grep -cv '^$' || true)
    last_rows="$rows"

    if (( n_rows == 1 )); then
        # Verify the lone survivor is NOT the tiebreaker.
        survivor=$(printf '%s\n' "$rows" | head -1)
        flags=$(kubectl get "resources.blockstor.io.blockstor.io/${survivor}" \
            -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
        if [[ "$flags" != *"TIE_BREAKER"* ]] && [[ "$flags" != *"DISKLESS"* ]]; then
            collapsed=true
            break
        fi
    fi
    sleep 2
done

if [[ "$collapsed" != "true" ]]; then
    echo "FAIL (Bug 338 regression): tiebreaker on $tb_node was not collapsed within 30s" >&2
    echo "  last CRD rows for ${RD}:" >&2
    printf '    %s\n' $last_rows >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

# Belt-and-suspenders: walk `linstor r l -r $RD` machine-readable
# JSON and confirm exactly 1 resource row remains with no DISKLESS
# flag. The CRD layer was the load-bearing check above; this is the
# operator-visible surface the bug report cited (`linstor r l`).
wire=$(linstor_r_l_json "$RD")
n_wire=$(printf '%s' "$wire" | jq -r '.[][] | .name' 2>/dev/null | wc -l | tr -d ' ' || echo 0)
if [[ "$n_wire" != "1" ]]; then
    echo "FAIL (Bug 338): linstor r l shows $n_wire rows for $RD, want 1" >&2
    printf '%s\n' "$wire" | jq '.[][]| {name, node_name, flags: .rsc_flags}' 2>/dev/null >&2 || true
    exit 1
fi

wire_flags=$(printf '%s' "$wire" | jq -r '.[][] | .rsc_flags // [] | join(",")' 2>/dev/null || echo "")
if [[ "$wire_flags" == *"TIE_BREAKER"* ]] || [[ "$wire_flags" == *"DISKLESS"* ]]; then
    echo "FAIL (Bug 338): surviving row carries unexpected flags=$wire_flags" >&2
    exit 1
fi

echo ">> r-d-collapses-tiebreaker OK (Bug 338 pinned: tiebreaker collapses when diskful drops to 1)"
