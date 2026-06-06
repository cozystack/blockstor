#!/usr/bin/env bash
#
# usage: r-c-autoplace-pool-pinned.sh WORK_DIR
#
# L6 cli-matrix cell — upstream-issues campaign-2, items U83/U21 + U139/U94
# (placement family, agent U6).
#
# Two pinned behaviours against the real CLI on a 3-node stand:
#
#   [U83] Pool-pinned autoplace — `r c --auto-place N --storage-pool <pool>`
#         must land replicas ONLY on nodes that actually host <pool>. The
#         pool name is a HARD filter (matchesPoolFilter), not a soft
#         preference: a node without the pool is never selected even if it
#         has more free space. We pin this by autoplacing onto the named
#         pool and asserting every diskful replica's node hosts that pool.
#
#   [U139] Contradictory / unsatisfiable constraints must NOT
#          "successfully autoplace on 0 nodes". `--replicas-on-same X` AND
#          `--replicas-on-different X` on the same Aux key is
#          self-contradictory beyond the first replica, so a place-count=2
#          request can never be satisfied. The CLI MUST exit non-zero with
#          the upstream "Not enough nodes" (FAIL_NOT_ENOUGH_NODES, 996)
#          envelope — never report SUCCESS — and leave no 2-replica RD.
#
# Unit pins:
#   pkg/placer/issues_u6_placement_family_test.go
#       ::TestU83PoolPinnedAutoplaceLandsOnlyOnPoolNodes
#       ::TestU139ContradictoryConstraintsPlaceZeroNeverSilentSuccess
#   pkg/rest/autoplace_issues_u6_test.go
#       ::TestU139ContradictoryConstraintsReturns409

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

POOL=${POOL:-lvm-thin}
PREFIX=ccu6-place
RDS=()
SET_AUX_NODES=()

cleanup() {
    for rd in "${RDS[@]}"; do
        delete_rd "$rd"
        assert_no_orphans "$rd"
    done
    # Clear any Aux/site labels we stamped so the next cell starts clean.
    for n in "${SET_AUX_NODES[@]}"; do
        "${LCTL[@]}" node set-property "$n" Aux/site "" >/dev/null 2>&1 || true
    done
    linstor_cli_teardown
}
trap cleanup EXIT

# Discover the nodes that host $POOL — the U83 invariant is asserted
# against exactly this set.
echo ">> pre-flight: discover nodes hosting $POOL"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
mapfile -t pool_nodes < <(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | .[]' <<<"$sp_json" 2>/dev/null)
if (( ${#pool_nodes[@]} < 2 )); then
    echo "SKIP: $POOL SP not on >=2 nodes (got ${#pool_nodes[@]}) — placement fixture unavailable"
    exit 0
fi
echo "   $POOL nodes: ${pool_nodes[*]}"

# ---------------------------------------------------------------------------
# [U83] pool-pinned autoplace lands only on pool nodes
# ---------------------------------------------------------------------------
echo ">> [U83] autoplace=2 -s $POOL — replicas must land only on $POOL nodes"
rd="${PREFIX}-u83"
RDS+=("$rd")
"${LCTL[@]}" resource-definition create "$rd" >/dev/null
"${LCTL[@]}" volume-definition create "$rd" 256M >/dev/null
"${LCTL[@]}" resource create --auto-place=2 --storage-pool="$POOL" "$rd" >/dev/null
# auto-quorum may add a DISKLESS TIE_BREAKER witness on a 3rd node, so the
# TOTAL resource count can be 3. We assert on the DISKFUL count, not the
# total — wait until exactly 2 diskful replicas exist.
for _ in $(seq 1 90); do
    [[ "$(linstor_diskful_count "$rd")" == "2" ]] && break
    sleep 1
done

mapfile -t placed_nodes < <(linstor_diskful_nodes "$rd")
if (( ${#placed_nodes[@]} != 2 )); then
    die "[U83] expected 2 diskful replicas, got ${#placed_nodes[@]}: ${placed_nodes[*]}"
fi
for n in "${placed_nodes[@]}"; do
    found=false
    for pn in "${pool_nodes[@]}"; do
        [[ "$n" == "$pn" ]] && found=true
    done
    if ! $found; then
        die "[U83 REGRESSION] $rd placed a replica on $n which does not host $POOL (pool nodes: ${pool_nodes[*]})"
    fi
done
echo "   OK: both replicas on $POOL nodes (${placed_nodes[*]})"

# ---------------------------------------------------------------------------
# [U139] contradictory constraints must fail short, never succeed-on-zero
# ---------------------------------------------------------------------------
echo ">> [U139] stamp Aux/site so the topology keys resolve"
# Give every pool node a distinct Aux/site value. With both
# --replicas-on-same site AND --replicas-on-different site, the 2nd
# replica is unsatisfiable (must share AND differ on the same key).
i=0
for n in "${pool_nodes[@]}"; do
    "${LCTL[@]}" node set-property "$n" Aux/site "site-$i" >/dev/null
    SET_AUX_NODES+=("$n")
    i=$((i + 1))
done

rd="${PREFIX}-u139"
RDS+=("$rd")
"${LCTL[@]}" resource-definition create "$rd" >/dev/null
"${LCTL[@]}" volume-definition create "$rd" 256M >/dev/null

echo ">> [U139] autoplace=2 with contradictory same+different site MUST fail short"
# NOTE (CLI arg-order quirk): python-linstor's --replicas-on-same /
# --replicas-on-different take nargs='*' and greedily eat the trailing
# positional RD name. Keep a self-contained flag (--storage-pool=...) as
# the LAST token before "$rd" so argparse stops consuming AUX values at
# the next "-" flag and the RD name lands as the positional.
err_file=$(mktemp)
if "${LCTL[@]}" resource create --auto-place=2 \
        --replicas-on-same site --replicas-on-different site \
        --storage-pool="$POOL" "$rd" >"$err_file" 2>&1; then
    echo "FAIL (U139 regression): contradictory-constraint autoplace SUCCEEDED — must fail short" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi

if ! grep -qiE 'Not enough (available nodes|nodes)' "$err_file"; then
    echo "FAIL (U139): autoplace failed but without the upstream 'Not enough nodes' envelope" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
echo "   OK: failed with the upstream FAIL_NOT_ENOUGH_NODES shortfall envelope"
cat "$err_file"
rm -f "$err_file"

# The RD must NOT carry 2 placed diskful replicas — at most the single
# zone-pinning first replica may exist.
dc=$(linstor_diskful_count "$rd")
if (( dc >= 2 )); then
    die "[U139] contradictory-constraint autoplace left $dc diskful replicas (success-on-zero/partial bug)"
fi
echo "   OK: $rd has $dc diskful replicas (< 2, no false success)"

echo ">> r-c-autoplace-pool-pinned OK (U83 pool-pin honored; U139 contradictory constraints fail short with 996)"
