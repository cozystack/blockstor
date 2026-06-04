#!/usr/bin/env bash
#
# usage: rg-c-overcommit-spawn-fails.sh WORK_DIR
#
# L6 cli-matrix cell — corner-case campaign D1
# (UG9 linstor-administration.adoc ~916-931).
#
# Upstream LINSTOR contract: an unsatisfiable place-count is NOT
# rejected at `resource-group create` time. The RG is created happily;
# the shortfall only surfaces at `rg spawn-resources`, which fails with
# "Not enough available nodes" (FAIL_NOT_ENOUGH_NODES, ret_code 996).
#
# Reproduction (3-node cluster, --place-count 7):
#
#   $ linstor resource-group create rg7 --place-count 7 -s stand
#   SUCCESS                                  <-- accepted, no early fail
#   $ linstor resource-group spawn-resources rg7 rg7r 32M
#   ERROR: Not enough available nodes        <-- fails only here
#
# This cell pins BOTH phases against the real CLI: the over-committed
# create must exit 0, and the spawn must exit non-zero carrying the
# upstream "Not enough" envelope. A regression that fails early on
# create (rejecting place_count > node_count) would diverge from
# upstream and break the legitimate "size the RG for a cluster that
# will grow" workflow.
#
# Unit pin: pkg/rest/bug_367_rg_place_count_validation_test.go (accepts
# positive place_count <= sanity ceiling) + pkg/rest/autoplace.go
# writeAutoplaceShortfall (the 996 envelope).

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
    echo "FAIL (D1 regression): rg create --place-count 7 was REJECTED — upstream accepts it" >&2
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

echo ">> [D1] spawn MUST fail with 'Not enough available nodes'"
err_file=$(mktemp)
if "${LCTL[@]}" resource-group spawn-resources "$RG" "$RD" 32M >"$err_file" 2>&1; then
    echo "FAIL (D1 regression): spawn of an over-committed RG SUCCEEDED — should fail short" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi

if ! grep -qiE 'Not enough (available nodes|nodes)' "$err_file"; then
    echo "FAIL (D1): spawn failed but without the upstream 'Not enough nodes' envelope" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
echo ">> spawn failed with the upstream shortfall envelope — OK"
cat "$err_file"
rm -f "$err_file"

# The spawned RD may or may not be created (definitions_only); either
# way it must NOT carry placed diskful replicas. Tolerate the RD
# existing with zero resources.
placed=$(kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
    | awk -v rd="$RD." '$1 ~ "^"rd' | wc -l | tr -d ' ')
if [[ "$placed" != "0" ]]; then
    echo "FAIL (D1): over-committed spawn left $placed Resource CRDs behind" >&2
    exit 1
fi

echo ">> rg-c-overcommit-spawn-fails OK (D1 pinned: create accepts pc=7, spawn fails short with 996)"
