#!/usr/bin/env bash
#
# usage: r-td-diskless-reaps-tiebreaker.sh WORK_DIR
#
# L6 cli-matrix cell — auto-tiebreaker reaped below 2 diskful on the
# `r td --diskless` (toggle) path. Sibling of r-d-collapses-tiebreaker
# (Bug 338, the physical-delete path); this cell covers the toggle path
# that the former Bug 104 keep-branch + Bug 108 race-repair branch
# wrongly protected.
#
# Reproduction from the stand:
#
#   $ linstor r l
#   test  worker-1  …  UpToDate
#   test  worker-2  …  UpToDate
#   test  worker-3  …  TieBreaker
#
#   $ linstor r td --diskless worker-2 test
#   SUCCESS
#
#   $ linstor r l
#   test  worker-1  …  UpToDate
#   test  worker-2  …  Diskless
#   test  worker-3  …  TieBreaker         ← WRONG: should be gone
#
# Root cause: at 1 diskful + 1 user-diskless, quorumPolicy returns
# quorum=off (it arms majority only at diskful == 2 or >= 3), so there
# is no majority to freeze and upstream LINSTOR's shouldTieBreakerExist
# never manages a witness below 2 diskful. blockstor kept (Bug 104) or
# re-created (Bug 108) the witness here on the false premise that
# "1 diskful + 1 diskless freezes quorum:majority".
#
# `r td --diskless` is a TOGGLE (matching upstream LINSTOR): the
# diskful replica keeps its Resource CRD but flips to DISKLESS, the
# satellite tears DRBD storage down and detaches the backing device.
# That drops diskful from 2 to 1 while leaving 1 user-diskless behind —
# the controller must reap the now-pointless witness, settling on
# exactly 2 rows (1 diskful UpToDate + 1 user-diskless), 0 TIE_BREAKER.
#
# Unit pin: internal/controller/ensure_tiebreaker_test.go
# (TestEnsureTiebreakerReapedAfterToggleDiskful2Diskless,
#  TestEnsureTiebreakerNoWitnessCreatedAtSingleDiskful,
#  TestEnsureTiebreakerReapedFullSequenceAfterToggle).
# This L6 cell is the stand-side companion: drives the real
# python-linstor CLI sequence on the 2r-tb shape and asserts the
# tiebreaker actually disappears from `linstor r l` within 30s of
# the `r td --diskless`, leaving exactly two Resource rows (the
# surviving diskful + the toggled user-diskless) with no TIE_BREAKER
# residue.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-td-tb-reap
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> [td-reap] shape-2r-tb: 2-replica RD + auto-tiebreaker"
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

# Toggle one of the diskful replicas to diskless. Either is fine — the
# invariant is "the witness is reaped once diskful drops to 1,
# regardless of which diskful is toggled".
TOGGLE_NODE=$(echo "$uptodate_pair" | awk '{print $1}')
KEEP_NODE=$(echo "$uptodate_pair" | awk '{print $2}')

echo ">> linstor r td --diskless $TOGGLE_NODE $RD  (toggle → DISKLESS, CRD kept)"
"${LCTL[@]}" resource toggle-disk --diskless "$TOGGLE_NODE" "$RD" >/dev/null 2>&1 || {
    echo "FAIL: r td --diskless exited non-zero" >&2
    exit 1
}

# Wait for the toggled replica to actually converge to Diskless before
# asserting the witness reap, so the count check below acts on a
# known-settled shape rather than racing the toggle.
wait_status_diskless "$RD" "$TOGGLE_NODE" 60 \
    || die "Phase 1: ${RD}.${TOGGLE_NODE} never converged to Diskless after r td --diskless"

echo ">> wait up to 30s for tiebreaker on $tb_node to be reaped"
deadline=$(( $(date +%s) + 30 ))
reaped=false
last_rows=""
while (( $(date +%s) < deadline )); do
    rows=$(kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="${RD}." '$1 ~ "^"rd {print $1}' || true)
    n_rows=$(printf '%s\n' "$rows" | grep -cv '^$' || true)
    last_rows="$rows"

    # Expect exactly 2 rows (surviving diskful + toggled user-diskless)
    # and NO row carrying the TIE_BREAKER flag.
    if (( n_rows == 2 )) && [[ -z "$(linstor_tiebreaker_node "$RD")" ]]; then
        reaped=true
        break
    fi
    sleep 2
done

if [[ "$reaped" != "true" ]]; then
    echo "FAIL (tiebreaker-reap regression): witness on $tb_node not reaped within 30s" >&2
    echo "  last CRD rows for ${RD}:" >&2
    printf '    %s\n' $last_rows >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

# Confirm the post-reap composition: exactly 1 diskful + 1 user-diskless.
diskful_n=$(linstor_diskful_count "$RD")
if [[ "$diskful_n" != "1" ]]; then
    echo "FAIL: post-reap diskful count = $diskful_n, want 1 (only $KEEP_NODE)" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

# Belt-and-suspenders: walk `linstor r l -r $RD` machine-readable JSON
# and confirm exactly 2 rows with no TIE_BREAKER flag — the
# operator-visible surface the contract cites (`linstor r l`).
wire=$(linstor_r_l_json "$RD")
n_wire=$(printf '%s' "$wire" | jq -r '.[][] | .name' 2>/dev/null | wc -l | tr -d ' ' || echo 0)
if [[ "$n_wire" != "2" ]]; then
    echo "FAIL: linstor r l shows $n_wire rows for $RD, want 2" >&2
    printf '%s\n' "$wire" | jq '.[][]| {name, node_name, flags: .flags}' 2>/dev/null >&2 || true
    exit 1
fi

wire_flags=$(printf '%s' "$wire" | jq -r '.[][] | .flags // [] | join(",")' 2>/dev/null || echo "")
if [[ "$wire_flags" == *"TIE_BREAKER"* ]]; then
    echo "FAIL: a surviving row still carries TIE_BREAKER (flags=$wire_flags)" >&2
    exit 1
fi

echo ">> r-td-diskless-reaps-tiebreaker OK (witness reaped when toggle drops diskful to 1)"
