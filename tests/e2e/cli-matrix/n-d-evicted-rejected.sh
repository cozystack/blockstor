#!/usr/bin/env bash
#
# usage: n-d-evicted-rejected.sh WORK_DIR
#
# L6 cli-matrix cell — corner-case F6 (UG9 §"Auto-evict" / EVICTED
# latched state).
#
# EVICTED is a one-way drain mark. A plain `linstor node delete` on an
# EVICTED node must be REJECTED: the operator has to choose explicitly
# between `node restore` (cancel the drain, keep SP / props / resources)
# and `node lost` (the node is gone for good). A bare `n d` would race
# the in-flight eviction migration and discard that decision.
#
# Pre-fix blockstor: `n d` only refused on outstanding Resource / SP
# references, so once the eviction reconciler had drained the node it
# would silently succeed — bypassing the restore-or-lost decision.
#
# This cell drives the real CLI against a 3-node stand:
#   1. rd c + vd c + r c --auto-place=2.
#   2. node evacuate <one diskful node>  -> node enters EVICTED.
#   3. node delete <evicted node>        -> MUST fail (non-zero exit).
#   4. node restore <node>               -> clears EVICTED, node usable.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=ccf-nd-evicted
POOL=${POOL:-lvm-thin}
EVAC_NODE=""

cleanup() {
    # Always clear the EVICTED flag so the node is left usable for the
    # next cell, whatever happened above.
    if [[ -n "$EVAC_NODE" ]]; then
        "${LCTL[@]}" node restore "$EVAC_NODE" >/dev/null 2>&1 || true
    fi
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> pre-flight: 3 healthy $POOL SPs"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 3 )); then
    echo "SKIP: $POOL SP not on 3 nodes (got $ok_nodes) — F6 fixture not available"
    exit 0
fi

echo ">> [F6] rd c + vd c + r c --auto-place=2 -s $POOL"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 256M >/dev/null
"${LCTL[@]}" resource create --auto-place=2 --storage-pool="$POOL" "$RD" >/dev/null
wait_replica_count "$RD" 2 90

mapfile -t diskful < <(linstor_diskful_nodes "$RD")
if (( ${#diskful[@]} < 2 )); then
    die "[F6] expected 2 diskful replicas after autoplace=2, got ${#diskful[@]}: ${diskful[*]}"
fi

EVAC_NODE="${diskful[0]}"
echo ">> [F6] node evacuate $EVAC_NODE (enters EVICTED)"
"${LCTL[@]}" node evacuate "$EVAC_NODE" >/dev/null

# Give the controller a moment to stamp the EVICTED flag on the Node CRD.
deadline=$(( $(date +%s) + 30 ))
while (( $(date +%s) < deadline )); do
    flags=$(kubectl get node.blockstor.cozystack.io "$EVAC_NODE" -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
    if [[ "$flags" == *EVICTED* ]]; then
        break
    fi
    sleep 2
done
if [[ "$flags" != *EVICTED* ]]; then
    die "[F6] node $EVAC_NODE never reached EVICTED state (flags=$flags)"
fi

echo ">> [F6] node delete $EVAC_NODE — MUST be rejected"
set +e
out=$("${LCTL[@]}" node delete "$EVAC_NODE" 2>&1)
rc=$?
set -e
printf '   %s\n' "$out"
if (( rc == 0 )); then
    die "[F6 REGRESSION] node delete of EVICTED node $EVAC_NODE SUCCEEDED — latched-state contract violated"
fi
if ! grep -qi "evicted" <<<"$out"; then
    die "[F6] node delete rejection did not mention EVICTED state; got: $out"
fi
echo "   OK: rejected (rc=$rc), message names the EVICTED state"

echo ">> [F6] node restore $EVAC_NODE clears EVICTED"
"${LCTL[@]}" node restore "$EVAC_NODE" >/dev/null
deadline=$(( $(date +%s) + 30 ))
while (( $(date +%s) < deadline )); do
    flags=$(kubectl get node.blockstor.cozystack.io "$EVAC_NODE" -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
    if [[ "$flags" != *EVICTED* ]]; then
        break
    fi
    sleep 2
done
if [[ "$flags" == *EVICTED* ]]; then
    die "[F6] node $EVAC_NODE still EVICTED after restore (flags=$flags)"
fi
EVAC_NODE=""  # cleared — cleanup needn't restore again

echo ">> n-d-evicted-rejected OK (F6 pinned: n d on EVICTED node rejected, restore clears it)"
