#!/usr/bin/env bash
#
# usage: n-evacuate-prunes-source.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 389 (user-reported, P1).
#
# `linstor node evacuate <node>` is an online drain: it must place a
# replacement diskful replica on a healthy peer, wait for it to reach
# UpToDate, then DELETE the source replica on the evacuated node — so
# the node is left empty and `linstor node delete` completes cleanly.
#
# Pre-fix blockstor behaviour: the NodeReconciler gap-filled the
# replacement but NEVER deleted the source on the evacuated node
# (`if !lost { return nil }`). Result: the RD ran at place_count+1
# diskful forever with one copy pinned to an EVICTED node, storage was
# never reclaimed, and the drain never completed.
#
# This cell drives the real CLI against a 3-node stand:
#   1. rd c + vd c + r c --auto-place=2  -> 2 diskful replicas.
#   2. pick one diskful node as the evacuation target.
#   3. linstor node evacuate <target>.
#   4. assert: a replacement diskful replica appears on the third node
#      and reaches UpToDate, AND the source replica on the evacuated
#      node is GONE (the node is drained empty).
#
# The add-before-drop ordering is also implicitly checked: the
# replacement must be UpToDate before the source disappears (we wait
# for the replacement UpToDate first, then assert the source is gone).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-389
POOL=${POOL:-lvm-thin}

EVAC_NODE=""

cleanup() {
    # Un-evacuate the node so the stand is left in a usable state for
    # the next cell (restore-flag is best-effort; a fresh EVICTED flag
    # would otherwise leak across cells).
    if [[ -n "$EVAC_NODE" ]]; then
        "${LCTL[@]}" node restore "$EVAC_NODE" >/dev/null 2>&1 || true
    fi
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

# Pre-flight: need the named pool on at least 3 nodes so autoplace=2
# leaves a third node free for the evacuation replacement to land on.
echo ">> pre-flight: 3 healthy $POOL SPs"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 3 )); then
    echo "SKIP: $POOL SP not on 3 nodes (got $ok_nodes) — Bug 389 fixture not available"
    exit 0
fi

echo ">> [Bug 389] rd c + vd c + r c --auto-place=2 -s $POOL"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 1G >/dev/null
"${LCTL[@]}" resource create --auto-place=2 --storage-pool="$POOL" "$RD" >/dev/null

echo ">> wait for 2 diskful replicas, both UpToDate"
wait_replica_count "$RD" 2 90
mapfile -t diskful < <(linstor_diskful_nodes "$RD")
if (( ${#diskful[@]} != 2 )); then
    die "[Bug 389] expected 2 diskful replicas after autoplace=2, got ${#diskful[@]}: ${diskful[*]}"
fi

for n in "${diskful[@]}"; do
    wait_status_state "$RD" "$n" "UpToDate|UpToDate\\(100%\\)" 240
done

EVAC_NODE="${diskful[0]}"
KEEP_NODE="${diskful[1]}"
echo ">> [Bug 389] evacuating $EVAC_NODE (keeping $KEEP_NODE)"

# The replacement must land on the one node that has the pool but no
# replica yet.
SPARE_NODE=$(linstor_pick_free_node "$RD" "$EVAC_NODE" "$KEEP_NODE")
if [[ -z "$SPARE_NODE" ]]; then
    echo "SKIP: no spare $POOL node free for the evacuation replacement"
    exit 0
fi
echo "   replacement expected on $SPARE_NODE"

"${LCTL[@]}" node evacuate "$EVAC_NODE" >/dev/null

echo ">> [Bug 389] wait for replacement on $SPARE_NODE to reach UpToDate"
# The replacement Resource CRD must appear and converge before the
# source is allowed to drop (strict add-before-drop).
deadline=$(( $(date +%s) + 120 ))
while (( $(date +%s) < deadline )); do
    if kubectl get "resources.blockstor.cozystack.io/${RD}.${SPARE_NODE}" >/dev/null 2>&1; then
        break
    fi
    sleep 3
done
if ! kubectl get "resources.blockstor.cozystack.io/${RD}.${SPARE_NODE}" >/dev/null 2>&1; then
    die "[Bug 389] evacuate never placed a replacement replica on $SPARE_NODE"
fi
wait_status_state "$RD" "$SPARE_NODE" "UpToDate|UpToDate\\(100%\\)" 240

echo ">> [Bug 389] assert source replica on $EVAC_NODE is pruned"
# THE BUG: pre-fix, this replica lived forever. Post-fix, once the
# replacement is UpToDate the NodeReconciler deletes it.
if ! wait_replica_absent "$RD" "$EVAC_NODE" 120; then
    die "[Bug 389 REGRESSION] source replica ${RD}.${EVAC_NODE} still present after evacuate completed — node never drained"
fi

echo ">> [Bug 389] assert redundancy preserved: $KEEP_NODE + $SPARE_NODE both UpToDate"
wait_status_state "$RD" "$KEEP_NODE" "UpToDate|UpToDate\\(100%\\)" 60
wait_status_state "$RD" "$SPARE_NODE" "UpToDate|UpToDate\\(100%\\)" 60

# Final shape: exactly 2 diskful replicas, neither on the evacuated node.
mapfile -t final < <(linstor_diskful_nodes "$RD")
for n in "${final[@]}"; do
    if [[ "$n" == "$EVAC_NODE" ]]; then
        die "[Bug 389] diskful replica still on evacuated node $EVAC_NODE: ${final[*]}"
    fi
done
if (( ${#final[@]} != 2 )); then
    die "[Bug 389] expected 2 diskful replicas after drain, got ${#final[@]}: ${final[*]}"
fi

echo ">> n-evacuate-prunes-source OK (Bug 389 pinned: evacuate placed replacement, drained source on $EVAC_NODE)"
