#!/usr/bin/env bash
#
# usage: auto-diskful-evicted-node.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 390.
#
# The auto-diskful controller partitions an RD's replicas to decide
# whether place_count is satisfied. Pre-fix the partition was blind to
# node state: a diskful replica on an EVICTED/LOST node was still
# counted as a live diskful, so the deficit created by draining a node
# was masked — the timer never armed and the lost diskful was never
# replaced. Worse, promoteOne could re-stamp StorPoolName onto a
# replica whose node was being drained, re-creating diskful storage on
# a node the operator is trying to evict.
#
# Reproduction (operator-CLI level):
#
#   $ linstor rd c <rd>; linstor vd c <rd> 128M
#   $ linstor r c <n1> <rd> -s stand
#   $ linstor r c <n2> <rd> -s stand
#   $ linstor r c <n3> <rd> -s stand          # 3 diskful, UpToDate
#   $ linstor controller set-property DrbdOptions/auto-diskful 1
#   $ linstor node evacuate <n3>              # n3 → EVICTED
#
# Expected (post-fix): the evicted node's diskful no longer counts
# toward place_count, so the controller refills a diskful replica on a
# HEALTHY node and the cluster reconverges to 3 diskful — and crucially
# NONE of the resulting diskful replicas sits on the evicted node.
#
# Pre-fix: the cluster stayed "satisfied" at 2 live + 1 evicted diskful
# (the evicted one masked the deficit), or a replacement was stamped
# back onto the evicted node.
#
# Contract: after evacuate + convergence, assert there are >= 3 diskful
# replicas AND the evicted node hosts no diskful replica.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-390
POOL=${POOL:-stand}

N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

cleanup() {
    # Best-effort restore of the evicted node so the stand is reusable
    # by later cells. `node evacuate --restore` (or re-create) is
    # idempotent; ignore failures.
    "${LCTL[@]}" node evacuate --restore "$N3" >/dev/null 2>&1 || true
    "${LCTL[@]}" controller set-property DrbdOptions/auto-diskful "" >/dev/null 2>&1 || true
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> [Bug 390] 3-replica diskful RD on $N1+$N2+$N3"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 128M >/dev/null
"${LCTL[@]}" resource create "$N1" "$RD" --storage-pool="$POOL" >/dev/null
"${LCTL[@]}" resource create "$N2" "$RD" --storage-pool="$POOL" >/dev/null
"${LCTL[@]}" resource create "$N3" "$RD" --storage-pool="$POOL" >/dev/null

echo ">> wait for all three diskful UpToDate"
# wait_uptodate asserts a pair at a time; chain the two pairs that
# cover all three replicas (n1↔n2 and n2↔n3).
wait_uptodate "$RD" "$N1" "$N2"
wait_uptodate "$RD" "$N2" "$N3"

echo ">> arm auto-diskful (1 minute) at controller scope"
"${LCTL[@]}" controller set-property DrbdOptions/auto-diskful 1 >/dev/null

echo ">> linstor node evacuate $N3 (Bug 390 trigger — drains/evicts the node)"
err_file=$(mktemp)
if ! "${LCTL[@]}" node evacuate "$N3" 2>"$err_file"; then
    rc=$?
    echo "FAIL (Bug 390): node evacuate $N3 exited $rc" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

# Give the auto-diskful timer (1m) + the placer refill time to fire.
# 180s budget covers the 1-minute deadline plus replacement create +
# initial sync to UpToDate.
echo ">> wait up to 180s for the cluster to refill to 3 diskful on healthy nodes"
deadline=$(( $(date +%s) + 180 ))
ok=0
while (( $(date +%s) < deadline )); do
    # Diskful nodes excluding the evicted one.
    mapfile -t diskful < <(linstor_diskful_nodes "$RD")
    healthy_diskful=0
    evicted_diskful=0
    for n in "${diskful[@]}"; do
        [[ -z "$n" ]] && continue
        if [[ "$n" == "$N3" ]]; then
            evicted_diskful=1
        else
            healthy_diskful=$(( healthy_diskful + 1 ))
        fi
    done

    if (( healthy_diskful >= 3 && evicted_diskful == 0 )); then
        ok=1
        break
    fi
    sleep 5
done

if (( ok != 1 )); then
    echo "FAIL (Bug 390 regression): cluster did not refill to 3 diskful on healthy nodes" >&2
    echo "  evicted node:  $N3" >&2
    echo "  diskful nodes: $(linstor_diskful_nodes "$RD" | tr '\n' ' ')" >&2
    kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="${RD}." '$1 ~ "^"rd' >&2 || true
    exit 1
fi

# Defensive double-check: the evicted node must host NO diskful replica
# (promoteOne must never select / re-stamp a disabled node — Bug 390 #4).
if linstor_diskful_nodes "$RD" | grep -qx "$N3"; then
    echo "FAIL (Bug 390 #4): a diskful replica was placed/kept on the EVICTED node $N3" >&2
    exit 1
fi

echo ">> auto-diskful-evicted-node OK (Bug 390 pinned: evicted diskful does not count; refill lands on a healthy node)"
