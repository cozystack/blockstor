#!/usr/bin/env bash
#
# usage: u268-bulk-create-converges.sh WORK_DIR
#
# L6 cli-matrix cell — U268 (bulk-create back-pressure / convergence).
#
# Upstream LINSTOR user report: firing many `r c --auto-place` in a tight
# loop (bulk provisioning, e.g. a StatefulSet scale-up) left a subset of
# resources wedged Inconsistent — initial syncs that never made progress
# because the controller/satellite reconcile pipeline fell behind under
# the create burst and dropped or serialised events badly.
#
# Blockstor pin: create 30 RDs (64M, --auto-place 2 -s <pool>) as fast as
# the CLI will accept them, then assert that ALL 30 reach all-replicas-
# UpToDate within 300s, with NONE left Inconsistent-without-progress.
# TieBreaker / Diskless rows are excluded from the "must be UpToDate"
# set (a diskless witness never carries data, so UpToDate is N/A there).
#
# The assertion is intentionally able to FAIL for the right reason:
#   - count != 30 RDs converged  -> some never reached UpToDate (the bug)
#   - any diskful replica Inconsistent with no done% advance for >STUCK_S
#     while the deadline still has room -> wedged initial sync (the bug)
#
# Teardown deletes all 30 RDs and asserts no orphans for each. SKIPs
# cleanly when the pool is not present on >=2 nodes.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

# --auto-place 2 needs at least 2 diskful-capable workers.
require_workers 2

linstor_cli_setup

PREFIX=cli-matrix-u268
POOL=${POOL:-stand}
COUNT=${COUNT:-30}
CONVERGE_TIMEOUT=${CONVERGE_TIMEOUT:-300}
STUCK_S=${STUCK_S:-90}

# Build the RD name list once so create + teardown agree exactly.
RDS=()
for i in $(seq 1 "$COUNT"); do
    RDS+=("${PREFIX}-$(printf '%03d' "$i")")
done

cleanup() {
    local rd
    for rd in "${RDS[@]}"; do
        delete_rd "$rd"
    done
    for rd in "${RDS[@]}"; do
        assert_no_orphans "$rd"
    done
    linstor_cli_teardown
}
trap cleanup EXIT

# Pre-flight: the named pool on at least 2 nodes (auto-place 2 needs them).
echo ">> pre-flight: $POOL SP on >=2 nodes"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
pool_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( pool_nodes < 2 )); then
    echo "SKIP: $POOL SP not on >=2 nodes (got $pool_nodes) — U268 fixture unavailable"
    exit 0
fi

# ---- BULK CREATE: fire all RDs in a tight loop (the back-pressure) ----
echo ">> bulk-creating $COUNT RDs (64M, --auto-place 2 -s $POOL) in a tight loop"
created=0
for rd in "${RDS[@]}"; do
    # Each RD is rd c + vd c + r c --auto-place 2. We don't wait between
    # iterations — the burst is the whole point of U268.
    if ! "${LCTL[@]}" resource-definition create "$rd" >/dev/null 2>&1; then
        echo "FAIL (U268): rd c $rd rejected during bulk burst" >&2
        exit 1
    fi
    if ! "${LCTL[@]}" volume-definition create "$rd" 64M >/dev/null 2>&1; then
        echo "FAIL (U268): vd c $rd rejected during bulk burst" >&2
        exit 1
    fi
    if ! "${LCTL[@]}" resource create --auto-place 2 --storage-pool="$POOL" "$rd" >/dev/null 2>&1; then
        echo "FAIL (U268): r c --auto-place 2 $rd rejected during bulk burst" >&2
        exit 1
    fi
    created=$((created+1))
done
echo "   accepted $created/$COUNT RD creates"
if (( created != COUNT )); then
    echo "FAIL (U268): only $created/$COUNT RD creates accepted" >&2
    exit 1
fi

# ---- CONVERGENCE: every RD's diskful replicas reach UpToDate ----
#
# converged_rd <rd> -> prints "yes" iff EVERY diskful (non-DISKLESS,
# non-TIE_BREAKER) replica of <rd> reports diskState UpToDate, AND there
# is at least 2 diskful replicas (auto-place 2 must have landed both).
converged_rd() {
    local rd=$1
    local name flags ds diskful=0 up=0
    while read -r name; do
        [[ -z "$name" ]] && continue
        flags=$(kubectl get "resources.blockstor.cozystack.io/${name}" \
            -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
        # Skip diskless witnesses / tiebreakers.
        if [[ "$flags" == *"DISKLESS"* || "$flags" == *"TIE_BREAKER"* ]]; then
            continue
        fi
        diskful=$((diskful+1))
        ds=$(kubectl get "resources.blockstor.cozystack.io/${name}" \
            -o jsonpath='{.status.volumes[0].diskState}' 2>/dev/null || echo "")
        [[ "$ds" == "UpToDate" ]] && up=$((up+1))
    done < <(kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="${rd}." '$1 ~ "^"rd {print $1}')
    if (( diskful >= 2 && up == diskful )); then
        echo "yes"
    else
        echo "no"
    fi
}

# inconsistent_no_progress <rd> -> prints "stuck" if any diskful replica
# of <rd> is Inconsistent AND its kernel resync done% has not advanced
# since last sample (tracked by the caller via the assoc arrays). Here we
# just report the current min done% across replicas + whether any is
# Inconsistent so the caller can decide.
rd_min_done_and_inconsistent() {
    local rd=$1 node pct any_incons="no" min=100
    while read -r node; do
        [[ -z "$node" ]] && continue
        local ds
        ds=$(status_disk_state "$rd" "$node" 0)
        [[ "$ds" == "Inconsistent" ]] && any_incons="yes"
        pct=$(on_node "$node" drbdsetup status "$rd" --json 2>/dev/null | jq -r '
            [.[0].connections[]? | .peer_devices[]? | (.done // empty)]
            | map(select(. != null)) | (min // 100)' 2>/dev/null || echo 100)
        [[ -z "$pct" ]] && pct=100
        (( pct < min )) && min=$pct
    done < <(linstor_diskful_nodes "$rd")
    echo "${any_incons}:${min}"
}

echo ">> waiting up to ${CONVERGE_TIMEOUT}s for all $COUNT RDs to reach all-replicas-UpToDate"
deadline=$(( $(date +%s) + CONVERGE_TIMEOUT ))

# Per-RD stuck tracking for Inconsistent-without-progress.
declare -A last_min=()
declare -A last_min_ts=()

all_done=false
while (( $(date +%s) < deadline )); do
    pending=()
    now=$(date +%s)
    for rd in "${RDS[@]}"; do
        if [[ "$(converged_rd "$rd")" == "yes" ]]; then
            continue
        fi
        pending+=("$rd")

        # Stuck check: if a diskful replica is Inconsistent and the min
        # resync done% has not advanced for >STUCK_S, it's wedged.
        info=$(rd_min_done_and_inconsistent "$rd")
        incons=${info%%:*}
        minpct=${info##*:}
        if [[ "$incons" == "yes" ]]; then
            if [[ "${last_min[$rd]:-}" == "$minpct" ]]; then
                elapsed=$(( now - ${last_min_ts[$rd]:-now} ))
                if (( elapsed > STUCK_S )); then
                    echo "FAIL (U268): $rd has an Inconsistent diskful replica stuck at done=${minpct}% for ${elapsed}s (>${STUCK_S}s) — wedged initial sync under bulk burst" >&2
                    kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
                        | awk -v rd="${rd}." '$1 ~ "^"rd' | sed 's/^/    /' >&2 || true
                    for node in $(linstor_diskful_nodes "$rd"); do
                        on_node "$node" drbdsetup status "$rd" 2>/dev/null | sed "s/^/    ${node}: /" >&2 || true
                    done
                    exit 1
                fi
            else
                last_min["$rd"]="$minpct"
                last_min_ts["$rd"]=$now
            fi
        else
            # Progressing or not-yet-Inconsistent — reset the stuck clock.
            unset 'last_min[$rd]'
            unset 'last_min_ts[$rd]'
        fi
    done

    if (( ${#pending[@]} == 0 )); then
        all_done=true
        break
    fi
    echo "   ${#pending[@]}/$COUNT RDs still converging..."
    sleep 10
done

if [[ "$all_done" != "true" ]]; then
    echo "FAIL (U268): not all $COUNT RDs reached all-replicas-UpToDate within ${CONVERGE_TIMEOUT}s" >&2
    echo "   still-pending RDs:" >&2
    for rd in "${RDS[@]}"; do
        if [[ "$(converged_rd "$rd")" != "yes" ]]; then
            echo "    $rd:" >&2
            kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
                | awk -v rd="${rd}." '$1 ~ "^"rd' | sed 's/^/      /' >&2 || true
        fi
    done
    exit 1
fi

# Final independent recount — converged_rd already proved per-RD UpToDate,
# but recompute the converged total so the success line is honest and a
# logic slip in the loop above can't claim a vacuous pass.
converged_total=0
for rd in "${RDS[@]}"; do
    [[ "$(converged_rd "$rd")" == "yes" ]] && converged_total=$((converged_total+1))
done
if (( converged_total != COUNT )); then
    echo "FAIL (U268): final recount $converged_total/$COUNT converged — convergence loop disagreed with recount" >&2
    exit 1
fi

echo ">> u268-bulk-create-converges OK (all $COUNT bulk-created RDs reached all-replicas-UpToDate, none wedged Inconsistent)"
