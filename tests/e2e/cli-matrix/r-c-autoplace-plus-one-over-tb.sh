#!/usr/bin/env bash
#
# usage: r-c-autoplace-plus-one-over-tb.sh WORK_DIR
#
# L6 cli-matrix cell — corner-case campaign D2b.
#
# Bug (live-stand verified on final main): with 2 diskful + 1 auto-
# tiebreaker witness (the standard place-count-2 shape on a 3-worker
# cluster), `linstor resource create --auto-place +1 <rd>` failed
# rc=10 "Not enough nodes fulfilling the following auto-place criteria
# ... Replica count: 3". The relative arithmetic was correct (+1 ->
# total 3) but the autoplacer EXCLUDED the witness-holding node from
# the diskful candidate set, so it could never reach the target.
#
# Upstream LINSTOR succeeds on this shape: it UPGRADES the witness on
# the third node into a diskful replica in place (the witness row's
# DISKLESS + TIE_BREAKER flags are dropped and a backing disk is
# stamped) — the same transition `r c <node> --storage-pool` performs.
#
# This cell pins the fix end-to-end:
#   1. rg place-count 2 + spawn on 3 workers -> 2 diskful + 1 witness.
#   2. assert the auto-tiebreaker witness exists (precondition).
#   3. `r c --auto-place +1` MUST exit 0 and grow to exactly 3 diskful.
#   4. assert the witness was upgraded IN PLACE: the node that held the
#      TIE_BREAKER now hosts a diskful replica, the tiebreaker row is
#      gone, and the total replica count is still 3 (no 4th node, the
#      cluster only has 3 workers anyway).
#   5. all 3 diskful replicas reach UpToDate.
#
# Unit pin: pkg/placer/corner_d2b_autoplace_over_tiebreaker_test.go.
# L7 replay: tests/operator-harness/replay/r-c-autoplace-plus-one.yaml.
#
# Distinct from r-c-autoplace-plus-one.sh: that cell pins the +1
# arithmetic (2 -> 3); THIS cell pins WHICH node fills slot 3 — the
# witness node, upgraded in place — which is the corner-D2b regression.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RG=ccd2b-rg
RD=ccd2b-ap
POOL=${POOL:-stand}

cleanup() {
    "${LCTL[@]}" resource-definition delete "$RD" >/dev/null 2>&1 || true
    "${LCTL[@]}" resource-group delete "$RG" >/dev/null 2>&1 || true
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> [D2b] rg create --place-count 2 + spawn -> 2 diskful + 1 witness"
"${LCTL[@]}" resource-group create "$RG" --place-count 2 --storage-pool="$POOL" >/dev/null
"${LCTL[@]}" resource-group spawn-resources "$RG" "$RD" 32M >/dev/null

# Wait for the steady state: 3 Resource rows total (2 diskful + 1 TB).
wait_replica_count "$RD" 3 120 || {
    echo "FAIL (D2b setup): RD did not reach 3 rows (2 diskful + 1 witness)" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
}

diskful2=$(linstor_diskful_count "$RD")
if [[ "$diskful2" != "2" ]]; then
    echo "FAIL (D2b setup): expected 2 diskful replicas, got $diskful2" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

TB_NODE=$(linstor_tiebreaker_node "$RD")
if [[ -z "$TB_NODE" ]]; then
    echo "FAIL (D2b setup): no auto-tiebreaker witness present — cannot exercise the upgrade-over-TB path" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi
echo ">> baseline: 2 diskful + 1 witness on node '$TB_NODE' — OK"

echo ">> [D2b] r c --auto-place +1 $RD MUST upgrade the witness on '$TB_NODE' to diskful (-> 3 diskful)"
if ! "${LCTL[@]}" resource create --auto-place +1 "$RD"; then
    echo "FAIL (D2b): --auto-place +1 exited non-zero (the corner-D2b regression: witness node excluded from candidates)" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

# Expect exactly 3 diskful replicas now and ZERO tiebreaker rows.
deadline=$(( $(date +%s) + 120 ))
diskful=0
tb_after="?"
while (( $(date +%s) < deadline )); do
    diskful=$(linstor_diskful_count "$RD")
    tb_after=$(linstor_tiebreaker_node "$RD")
    if (( diskful >= 3 )) && [[ -z "$tb_after" ]]; then
        break
    fi
    sleep 3
done

if [[ "$diskful" != "3" ]]; then
    echo "FAIL (D2b): after --auto-place +1 expected 3 diskful replicas, got $diskful" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

if [[ -n "$tb_after" ]]; then
    echo "FAIL (D2b): tiebreaker witness still present on '$tb_after' after +1 (witness was NOT upgraded in place)" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

# The upgrade must have happened on the SAME node that held the witness
# (the cluster has only 3 workers, so slot 3 can only be the TB node).
if ! kubectl get "resources.blockstor.cozystack.io/${RD}.${TB_NODE}" >/dev/null 2>&1; then
    echo "FAIL (D2b): witness node '$TB_NODE' has no Resource CRD after upgrade" >&2
    exit 1
fi
upgraded_flags=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${TB_NODE}" \
    -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
if [[ "$upgraded_flags" == *"DISKLESS"* ]] || [[ "$upgraded_flags" == *"TIE_BREAKER"* ]]; then
    echo "FAIL (D2b): replica on ex-witness node '$TB_NODE' still carries diskless/TB flags: $upgraded_flags" >&2
    exit 1
fi
echo ">> witness on '$TB_NODE' upgraded in place to diskful, 3 diskful total, 0 tiebreakers — OK"

echo ">> wait all 3 diskful replicas UpToDate"
deadline=$(( $(date +%s) + 300 ))
all_up=false
while (( $(date +%s) < deadline )); do
    up=$("${LCTL[@]}" --machine-readable resource list --resources "$RD" 2>/dev/null \
        | jq -r '[.[][]? | .volumes[]? | select(.provider_kind != "DISKLESS") | .state.disk_state]
                 | map(select(. == "UpToDate")) | length' 2>/dev/null || echo 0)
    if (( up >= 3 )); then
        all_up=true
        break
    fi
    sleep 3
done

if [[ "$all_up" != "true" ]]; then
    echo "FAIL (D2b): not all 3 diskful replicas reached UpToDate after the witness upgrade" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

echo ">> r-c-autoplace-plus-one-over-tb OK (D2b pinned: +1 upgraded the auto-tiebreaker witness in place; 3 diskful, all UpToDate)"
