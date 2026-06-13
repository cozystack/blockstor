#!/usr/bin/env bash
#
# usage: auto-diskful-evicted-node.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 390 (auto-diskful must ignore disabled-node
# replicas) / BUG-040 cell repair.
#
# The auto-diskful controller partitions an RD's replicas to decide
# whether the parent RG's place_count is satisfied. Pre-Bug-390 the
# partition was blind to node state: a diskful replica on an
# EVICTED/LOST node was still counted as a live diskful, so the deficit
# created by draining a node was masked — the timer never armed and the
# lost diskful was never replaced. Worse, promoteOne could promote a
# diskless replica sitting on a node the operator was draining.
#
# BUG-040 triage note: the original revision of this cell asserted
# "refill to >= 3 diskful on healthy nodes" after evacuating one of 3
# workers — topologically impossible (only 2 healthy workers remain) —
# and created the RD in DfltRscGrp, whose empty SelectFilter
# (place_count 0) makes the auto-diskful controller skip the RD
# entirely (`placeCountForRD` finds no target). The cell could never
# pass; the sweep exposed it. This revision pins the same product
# contract on an achievable 3-worker shape:
#
#   1. rg c --place-count 2 + rd c --resource-group + vd c.
#   2. r c <spare> --diskless        # user-diskless promotion candidate
#      (created FIRST so no auto-tiebreaker witness ever spawns:
#       a non-witness diskless already breaks the tie).
#   3. r c <d1> / r c <d2> diskful   # place_count satisfied.
#   4. controller set-property DrbdOptions/auto-diskful 1 (minutes).
#   5. node evacuate <d2>            # -> EVICTED, deficit of 1.
#
# Expected (Bug 390 contract, outcome-level): the EVICTED node's
# diskful no longer counts toward place_count, so the cluster repairs
# the deficit by promoting the healthy spare's diskless replica to
# diskful (the auto-diskful timer and the evacuate add-before-drop
# migration both converge on the same outcome — the spare is the only
# eligible candidate). End state: 2 healthy diskful (d1 + spare), and
# crucially NO diskful replica on the evacuated node.
#
# Pre-fix: the cluster stayed "satisfied" at 1 live + 1 evicted diskful
# (the evicted one masked the deficit), or the replacement was stamped
# back onto the evicted node.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RG=apg-390
RD=cli-matrix-390
POOL=${POOL:-stand}

D1=$WORKER_1
D2=$WORKER_2
SPARE=$WORKER_3

cleanup() {
    # Best-effort restore of the evacuated node so the stand is
    # reusable by later cells; disarm the controller-scope timer.
    "${LCTL[@]}" node restore "$D2" >/dev/null 2>&1 || true
    "${LCTL[@]}" controller set-property DrbdOptions/auto-diskful "" >/dev/null 2>&1 || true
    delete_rd "$RD"
    assert_no_orphans "$RD"
    "${LCTL[@]}" resource-group delete "$RG" >/dev/null 2>&1 || true
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> [Bug 390] rg c $RG --place-count 2 + rd c $RD"
"${LCTL[@]}" resource-group create "$RG" --place-count 2 --storage-pool="$POOL" >/dev/null
"${LCTL[@]}" resource-definition create "$RD" --resource-group "$RG" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 128M >/dev/null

# The promotion candidate FIRST: a user diskless on the spare. Created
# before any diskful so the auto-tiebreaker never wants a witness
# (non-witness diskless count >= 1 breaks the tie by itself) and the
# candidate carries no TIE_BREAKER flag — promoteOne refuses witnesses.
echo ">> [Bug 390] r c $SPARE $RD --diskless (promotion candidate)"
"${LCTL[@]}" resource create "$SPARE" "$RD" --diskless >/dev/null

echo ">> [Bug 390] diskful pair on $D1 + $D2"
"${LCTL[@]}" resource create "$D1" "$RD" --storage-pool="$POOL" >/dev/null
"${LCTL[@]}" resource create "$D2" "$RD" --storage-pool="$POOL" >/dev/null

echo ">> wait for both diskful UpToDate"
wait_uptodate "$RD" "$D1" "$D2"

echo ">> arm auto-diskful (1 minute) at controller scope"
"${LCTL[@]}" controller set-property DrbdOptions/auto-diskful 1 >/dev/null

echo ">> linstor node evacuate $D2 (Bug 390 trigger — drains/evicts the node)"
err_file=$(mktemp)
if ! "${LCTL[@]}" node evacuate "$D2" 2>"$err_file"; then
    rc=$?
    echo "FAIL (Bug 390): node evacuate $D2 exited $rc" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

# Budget: the 1-minute auto-diskful deadline + promotion + sync. The
# evacuate add-before-drop migration may repair the deficit earlier;
# either path must converge on the same end state.
echo ">> wait up to 240s for the deficit repair: $SPARE promoted diskful, no diskful on $D2"
deadline=$(( $(date +%s) + 240 ))
ok=0
while (( $(date +%s) < deadline )); do
    mapfile -t diskful < <(linstor_diskful_nodes "$RD")
    spare_diskful=0
    evicted_diskful=0
    healthy_diskful=0
    for n in "${diskful[@]}"; do
        [[ -z "$n" ]] && continue
        if [[ "$n" == "$D2" ]]; then
            evicted_diskful=1
        else
            healthy_diskful=$(( healthy_diskful + 1 ))
        fi
        if [[ "$n" == "$SPARE" ]]; then
            spare_diskful=1
        fi
    done

    if (( spare_diskful == 1 && healthy_diskful >= 2 && evicted_diskful == 0 )); then
        ok=1
        break
    fi
    sleep 5
done

if (( ok != 1 )); then
    echo "FAIL (Bug 390 regression): deficit behind the EVICTED node was not repaired" >&2
    echo "  evacuated node: $D2" >&2
    echo "  diskful nodes:  $(linstor_diskful_nodes "$RD" | tr '\n' ' ')" >&2
    kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="${RD}." '$1 ~ "^"rd' >&2 || true
    exit 1
fi

# Defensive double-check (Bug 390 #4): the surviving diskful pair is
# d1 + spare, and the original healthy diskful was never demoted.
flags=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${D1}" \
    -o jsonpath='{.spec.flags}' 2>/dev/null || echo "__absent__")
if [[ "$flags" == "__absent__" || "$flags" == *"DISKLESS"* ]]; then
    echo "FAIL (Bug 390): healthy diskful on $D1 was lost/demoted during the repair; flags=$flags" >&2
    exit 1
fi

echo ">> auto-diskful-evicted-node OK (Bug 390 pinned: evicted diskful does not count; repair promotes the healthy spare)"
