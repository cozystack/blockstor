#!/usr/bin/env bash
#
# usage: rg-c-overcommit-spawn-defers.sh WORK_DIR
#
# L6 cli-matrix cell — corner-case campaign D1
# (UG9 linstor-administration.adoc ~916-931).
#
# Over-commit contract on blockstor (DELIBERATE divergence from upstream
# LINSTOR — see docs/cli-parity-known-deltas.md row 83):
#
#   - `resource-group create --place-count 7` on a 3-node cluster is
#     ACCEPTED (parity with upstream — the shortfall is never an early
#     create-time error; size the RG for a cluster that will grow).
#
#   - `resource-group spawn-resources` on that over-committed RG does
#     NOT fail short the way upstream LINSTOR does (upstream returns
#     FAIL_NOT_ENOUGH_NODES / ret_code 996 and places nothing). BS
#     instead takes a DEFERRED, best-effort autoplace path: it spawns
#     the RD + VDs, places as many diskful replicas as the topology
#     allows (3 of 7 here), and surfaces the shortfall as an INFO in the
#     success envelope ("autoplace deferred: <rd>: not enough candidate
#     storage pools: placed 3 of 7"), exit 0. The RGRebalanceReconciler
#     then tops the replica count back up additively once more nodes
#     appear (internal/controller/rg_rebalance_controller.go).
#
# Reproduction (3-node cluster, --place-count 7):
#
#   $ linstor resource-group create rg7 --place-count 7 -s stand
#   SUCCESS                                       <-- accepted, no early fail
#   $ linstor resource-group spawn-resources rg7 rg7r 32M
#   SUCCESS                                       <-- exit 0, deferred
#     resource definition spawned, autoplace deferred: rg7r:
#     not enough candidate storage pools: placed 3 of 7
#
# Why the contract flipped here (this cell used to be
# rg-c-overcommit-spawn-fails and asserted the upstream 996 short-fail):
# the 996 short-fail path is upstream-faithful but BS deliberately chose
# the deferred best-effort path so the CSI external-provisioner retry
# loop sees a created RD it can converge on, not a hard failure on a
# cluster that is one node away from satisfying the request. The
# behaviour is pinned at the unit tier by
# pkg/rest/spawn_test.go::TestSpawnImpossiblePlacementReturnsActionableError
# (asserts the "autoplace deferred" + "placed N of M" envelope and that
# the RD survives) — switching spawn back to writeAutoplaceShortfall
# would be a deliberate API change, NOT a silent regression, so this
# cell pins the deferred contract end-to-end through the real CLI.
#
# What a REAL regression would look like (and this cell would catch):
#   - the over-committed create being REJECTED early (breaks "size for
#     growth"), or
#   - spawn placing MORE diskful than the topology allows (over-place),
#     or zero diskful when at least 3 nodes can host (under-place), or
#   - spawn returning a non-zero exit (the old upstream short-fail
#     creeping back in without the deliberate-API-change ceremony).
#
# Unit pin: pkg/rest/spawn_test.go::TestSpawnImpossiblePlacementReturnsActionableError
# + pkg/rest/bug_367_rg_place_count_validation_test.go (place_count
# input validation).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RG=cli-matrix-d1-rg
RD=cli-matrix-d1-rd
POOL=${POOL:-stand}

cleanup() {
    "${LCTL[@]}" resource-definition delete "$RD" >/dev/null 2>&1 || true
    "${LCTL[@]}" resource-group delete "$RG" >/dev/null 2>&1 || true
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> [D1] over-commit: rg create --place-count 7 on a 3-node cluster MUST be accepted"
if ! "${LCTL[@]}" resource-group create "$RG" --place-count 7 --storage-pool="$POOL"; then
    echo "FAIL (D1 regression): rg create --place-count 7 was REJECTED — BS accepts it" >&2
    exit 1
fi

# Confirm the RG persisted with place_count=7.
pc=$("${LCTL[@]}" --machine-readable resource-group list --resource-groups "$RG" 2>/dev/null \
    | jq -r '[.[][]? | .select_filter.place_count] | .[0] // empty' 2>/dev/null || echo "")
if [[ "$pc" != "7" ]]; then
    echo "FAIL (D1): RG persisted place_count=$pc, want 7" >&2
    "${LCTL[@]}" resource-group list --resource-groups "$RG" 2>&1 | tail -20 >&2
    exit 1
fi
echo ">> RG created with place_count=7 (over-committed, accepted) — OK"

echo ">> [D1] spawn MUST succeed with a DEFERRED best-effort autoplace (BS delta, not the upstream 996 short-fail)"
out_file=$(mktemp)
if ! "${LCTL[@]}" resource-group spawn-resources "$RG" "$RD" 32M >"$out_file" 2>&1; then
    echo "FAIL (D1 regression): spawn of an over-committed RG returned NON-ZERO" >&2
    echo "  BS contract is deferred best-effort autoplace (exit 0), not the upstream 'Not enough nodes' short-fail." >&2
    cat "$out_file" >&2
    rm -f "$out_file"
    exit 1
fi

# The success envelope must carry the deferred-autoplace shortfall INFO.
if ! grep -qiE 'autoplace deferred|not enough candidate storage pools|placed [0-9]+ of [0-9]+' "$out_file"; then
    echo "FAIL (D1): spawn succeeded but without the deferred-autoplace shortfall envelope" >&2
    echo "  expected an 'autoplace deferred: ... placed N of M' INFO surfacing the partial placement." >&2
    cat "$out_file" >&2
    rm -f "$out_file"
    exit 1
fi
echo ">> spawn succeeded with the deferred-autoplace shortfall envelope — OK"
cat "$out_file"
rm -f "$out_file"

# The RD must survive (the CSI retry loop converges on it).
if ! kubectl get "resourcedefinitions.blockstor.cozystack.io/${RD}" >/dev/null 2>&1; then
    echo "FAIL (D1): deferred spawn did not leave the RD behind — CSI retry loop has nothing to converge on" >&2
    exit 1
fi

# Best-effort placement: at least one diskful, never more than the
# topology allows (3 nodes). Count diskful (non-DISKLESS / non-TIE_BREAKER)
# Resource CRDs for the RD, polling briefly so the placer has landed.
nodes=3
diskful=0
for _ in $(seq 1 15); do
    diskful=$(kubectl get resources.blockstor.cozystack.io -o json 2>/dev/null \
        | RD="$RD" python3 -c "import json,sys,os
d=json.load(sys.stdin)
rd=os.environ['RD']
n=0
for it in d.get('items',[]):
    if it.get('spec',{}).get('resourceDefinitionName')!=rd: continue
    flags=it.get('spec',{}).get('flags',[]) or []
    if 'DISKLESS' in flags or 'TIE_BREAKER' in flags: continue
    n+=1
print(n)" 2>/dev/null || echo 0)
    [[ "$diskful" -ge 1 ]] && break
    sleep 2
done

if [[ "$diskful" -lt 1 ]]; then
    echo "FAIL (D1): deferred spawn placed ZERO diskful replicas on a cluster that can host $nodes" >&2
    exit 1
fi
if [[ "$diskful" -gt "$nodes" ]]; then
    echo "FAIL (D1): deferred spawn OVER-placed $diskful diskful on a $nodes-node cluster" >&2
    exit 1
fi
echo ">> deferred spawn placed $diskful diskful (best-effort, <= $nodes nodes) — OK"

echo ">> rg-c-overcommit-spawn-defers OK (D1 pinned: create accepts pc=7, spawn defers best-effort with exit 0)"
