#!/usr/bin/env bash
#
# usage: n-autoplace-target-excludes.sh WORK_DIR
#
# L6 cli-matrix cell — corner-case F4 (UG9 §"AutoplaceTarget";
# kb.linbit.com/linstor/preventing-linstor-resource-placement-on-a-node).
#
# `linstor node set-property <node> AutoplaceTarget false` excludes the
# node from autoplace TARGETING: the autoplacer must never pick it for a
# NEW replica, but existing replicas stay put (no migration). This is the
# maintenance-drain workflow — stop new placements without evicting.
#
# Pre-fix blockstor behaviour: the placer's only node-exclusion was the
# EVICTED / LOST flag set; the per-node AutoplaceTarget prop was never
# consulted, so a drained maintenance node kept receiving new replicas.
#
# This cell drives the real CLI against a 3-node stand:
#   1. node set-property <drain> AutoplaceTarget false.
#   2. spawn several rd c + vd c + rd ap --place-count 2.
#   3. assert: zero replicas land on the drained node.
#   4. clear the prop, spawn one more, assert it CAN land there again.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

POOL=${POOL:-lvm-thin}
PREFIX=ccf-aptgt
DRAIN_NODE=""
RDS=()

cleanup() {
    # Clear the prop first so a leaked AutoplaceTarget=false can't strand
    # the next cell's placements on this node.
    if [[ -n "$DRAIN_NODE" ]]; then
        "${LCTL[@]}" node set-property "$DRAIN_NODE" AutoplaceTarget true >/dev/null 2>&1 || true
    fi
    for rd in "${RDS[@]}"; do
        delete_rd "$rd"
        assert_no_orphans "$rd"
    done
    linstor_cli_teardown
}
trap cleanup EXIT

# Pre-flight: need the named pool on at least 3 nodes so autoplace=2
# still has a free non-drain node to choose between.
echo ">> pre-flight: 3 healthy $POOL SPs"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
mapfile -t pool_nodes < <(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | .[]' <<<"$sp_json" 2>/dev/null)
if (( ${#pool_nodes[@]} < 3 )); then
    echo "SKIP: $POOL SP not on 3 nodes (got ${#pool_nodes[@]}) — F4 fixture not available"
    exit 0
fi

DRAIN_NODE="${pool_nodes[0]}"
echo ">> [F4] draining target: $DRAIN_NODE (AutoplaceTarget=false)"
"${LCTL[@]}" node set-property "$DRAIN_NODE" AutoplaceTarget false >/dev/null

echo ">> [F4] spawn 4 RDs autoplace=2, none must land on $DRAIN_NODE"
for i in 1 2 3 4; do
    rd="${PREFIX}-${i}"
    RDS+=("$rd")
    "${LCTL[@]}" resource-definition create "$rd" >/dev/null
    "${LCTL[@]}" volume-definition create "$rd" 256M >/dev/null
    "${LCTL[@]}" resource create --auto-place=2 --storage-pool="$POOL" "$rd" >/dev/null
    wait_replica_count "$rd" 2 90
done

echo ">> [F4] assert zero replicas on $DRAIN_NODE"
for rd in "${RDS[@]}"; do
    mapfile -t nodes < <(linstor_diskful_nodes "$rd")
    for n in "${nodes[@]}"; do
        if [[ "$n" == "$DRAIN_NODE" ]]; then
            die "[F4 REGRESSION] $rd placed a replica on AutoplaceTarget=false node $DRAIN_NODE: ${nodes[*]}"
        fi
    done
done
echo "   OK: all ${#RDS[@]} RDs avoided $DRAIN_NODE"

echo ">> [F4] clear the prop; a fresh autoplace=3 must now use $DRAIN_NODE"
"${LCTL[@]}" node set-property "$DRAIN_NODE" AutoplaceTarget true >/dev/null
rd="${PREFIX}-restore"
RDS+=("$rd")
"${LCTL[@]}" resource-definition create "$rd" >/dev/null
"${LCTL[@]}" volume-definition create "$rd" 256M >/dev/null
"${LCTL[@]}" resource create --auto-place=3 --storage-pool="$POOL" "$rd" >/dev/null
wait_replica_count "$rd" 3 90

mapfile -t nodes < <(linstor_diskful_nodes "$rd")
on_drain=false
for n in "${nodes[@]}"; do
    if [[ "$n" == "$DRAIN_NODE" ]]; then
        on_drain=true
    fi
done
if ! $on_drain; then
    die "[F4] after clearing AutoplaceTarget, place-count=3 still skipped $DRAIN_NODE: ${nodes[*]}"
fi

echo ">> n-autoplace-target-excludes OK (F4 pinned: AutoplaceTarget=false excludes node, true re-includes it)"
