#!/usr/bin/env bash
#
# usage: n-evacuate-single-replica.sh WORK_DIR
#
# L6 cli-matrix cell — corner case U305 (upstream-mined, P0).
#
# `linstor node evacuate <node>` of the node holding the ONLY diskful
# replica of an RD must MIGRATE that copy (place-then-prune) — it must
# NEVER drop the lone source while no replacement is durable, which would
# leave the resource Outdated / sourceless (data loss).
#
# This cell drives the real CLI against a 3-node stand:
#   1. rd c + vd c + r c <node1>      -> exactly ONE diskful replica.
#   2. linstor node evacuate <node1>.
#   3. assert: a replacement diskful replica appears on a spare node and
#      reaches UpToDate (add-before-drop), the source on <node1> is
#      pruned, and at the end >= 1 diskful replica exists on a
#      NON-evacuated node (never sourceless, never zero).
#
# The data-loss guard (NodeReconciler.evacuationReplacementReady >= 1
# floor) is what keeps the source pinned until the migrated copy is
# UpToDate.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-u305
POOL=${POOL:-lvm-thin}

EVAC_NODE=""

cleanup() {
    if [[ -n "$EVAC_NODE" ]]; then
        "${LCTL[@]}" node restore "$EVAC_NODE" >/dev/null 2>&1 || true
    fi
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

# Pre-flight: need the named pool on at least 3 nodes so the single
# replica has two spare nodes to migrate onto.
echo ">> pre-flight: 3 healthy $POOL SPs"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 3 )); then
    echo "SKIP: $POOL SP not on 3 nodes (got $ok_nodes) — U305 fixture not available"
    exit 0
fi

# Pick a worker to host the single diskful replica.
START_NODE="$WORKER_1"

echo ">> [U305] rd c + vd c + r c $START_NODE -s $POOL (single diskful)"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 1G >/dev/null
"${LCTL[@]}" resource create "$START_NODE" "$RD" --storage-pool="$POOL" >/dev/null

echo ">> wait for the single diskful replica to reach UpToDate"
wait_status_state "$RD" "$START_NODE" "UpToDate|UpToDate\\(100%\\)" 240

mapfile -t diskful < <(linstor_diskful_nodes "$RD")
if (( ${#diskful[@]} != 1 )); then
    die "[U305] expected exactly 1 diskful replica, got ${#diskful[@]}: ${diskful[*]}"
fi

EVAC_NODE="$START_NODE"
echo ">> [U305] evacuating the node holding the ONLY replica: $EVAC_NODE"

SPARE_NODE=$(linstor_pick_free_node "$RD" "$EVAC_NODE")
if [[ -z "$SPARE_NODE" ]]; then
    echo "SKIP: no spare $POOL node free for the migration target"
    exit 0
fi
echo "   replacement expected on $SPARE_NODE (or another spare)"

"${LCTL[@]}" node evacuate "$EVAC_NODE" >/dev/null

echo ">> [U305] wait for a replacement diskful replica to appear on a spare node and reach UpToDate"
deadline=$(( $(date +%s) + 180 ))
repl_node=""
while (( $(date +%s) < deadline )); do
    mapfile -t cur < <(linstor_diskful_nodes "$RD")
    for n in "${cur[@]}"; do
        if [[ "$n" != "$EVAC_NODE" ]]; then
            repl_node="$n"
            break
        fi
    done
    [[ -n "$repl_node" ]] && break
    sleep 3
done

if [[ -z "$repl_node" ]]; then
    die "[U305] evacuate never placed a replacement diskful replica on a non-evacuated node — the lone copy was not migrated"
fi
echo "   replacement landed on $repl_node"
wait_status_state "$RD" "$repl_node" "UpToDate|UpToDate\\(100%\\)" 240

echo ">> [U305] assert source replica on $EVAC_NODE is pruned (only after the migrated copy is durable)"
if ! wait_replica_absent "$RD" "$EVAC_NODE" 120; then
    die "[U305 REGRESSION] source replica ${RD}.${EVAC_NODE} still present after evacuate — drain did not complete"
fi

echo ">> [U305] assert the resource is NOT sourceless: >= 1 diskful on a non-evacuated node"
mapfile -t final < <(linstor_diskful_nodes "$RD")
final_off_evac=0
for n in "${final[@]}"; do
    if [[ "$n" == "$EVAC_NODE" ]]; then
        die "[U305] diskful replica still on evacuated node $EVAC_NODE: ${final[*]}"
    fi
    final_off_evac=$(( final_off_evac + 1 ))
done

if (( final_off_evac < 1 )); then
    die "[U305 DATA LOSS] single-replica evacuate left the resource sourceless (0 diskful): ${final[*]}"
fi

# Every surviving diskful replica must be UpToDate — redundancy never dipped.
for n in "${final[@]}"; do
    wait_status_state "$RD" "$n" "UpToDate|UpToDate\\(100%\\)" 60
done

echo ">> n-evacuate-single-replica OK (U305 pinned: lone replica migrated, never sourceless; ${final_off_evac} diskful on ${final[*]})"
