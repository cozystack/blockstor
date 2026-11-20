#!/usr/bin/env bash
#
# usage: r-c-no-drbd-no-tiebreaker.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 334 (user-reported).
#
# Reproduction from the user's stand (with Bug 335 fix in place):
#
#   $ linstor r c test3 --auto-place=1 -l STORAGE -s stand
#   SUCCESS
#   $ linstor r l -r test3
#   test3  worker-1  ...
#   test3  worker-3  ...  TieBreaker
#
# Pre-fix behaviour: a TIE_BREAKER DISKLESS witness landed on a third
# node despite no DRBD in the layer list. TIE_BREAKER is a DRBD-9
# quorum primitive (1 diskless arbiter peer for `quorum: majority`
# decisions in 2-replica setups). Without DRBD there is no quorum
# machinery — the witness is meaningless extra state that surprises
# operators reading `linstor r l`.
#
# Post-fix contract: `--auto-place=1` with `-l STORAGE` MUST yield
# exactly 1 Resource (no TIE_BREAKER witness on a third node).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-334
POOL=${POOL:-lvm-thin}

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> pre-flight: at least one healthy $POOL SP"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 1 )); then
    echo "SKIP: $POOL SP not present — Bug 334 fixture not available"
    exit 0
fi

echo ">> [Bug 334] linstor rd c + vd c"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 1G >/dev/null

echo ">> [Bug 334] linstor r c $RD --auto-place=1 -l STORAGE -s $POOL"
err_file=$(mktemp)
if ! "${LCTL[@]}" resource create --auto-place=1 --layer-list storage --storage-pool="$POOL" "$RD" 2>"$err_file"; then
    rc=$?
    echo "FAIL: auto-place=1 with -l STORAGE exited $rc (should succeed)" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

echo ">> wait up to 60s for replicas to stabilise"
deadline=$(( $(date +%s) + 60 ))
stable=false
last_count=0
while (( $(date +%s) < deadline )); do
    last_count=$(kubectl get resources.blockstor.org --no-headers 2>/dev/null \
        | awk -v rd="$RD." '$1 ~ "^"rd' | wc -l)
    if (( last_count == 1 )); then
        # Give the RD reconciler a beat in case the (pre-fix) witness
        # is about to land on a stale enqueue. We poll one extra
        # period before declaring success.
        sleep 5
        last_count=$(kubectl get resources.blockstor.org --no-headers 2>/dev/null \
            | awk -v rd="$RD." '$1 ~ "^"rd' | wc -l)
        if (( last_count == 1 )); then
            stable=true
            break
        fi
    fi
    sleep 2
done

if [[ "$stable" != "true" ]]; then
    echo "FAIL (Bug 334): expected exactly 1 Resource CRD for $RD; got $last_count" >&2
    kubectl get resources.blockstor.org --no-headers 2>/dev/null \
        | awk -v rd="$RD." '$1 ~ "^"rd' >&2
    exit 1
fi

# Assert no TIE_BREAKER witness on any node. The pre-fix bug stamped
# the witness on a third node — `kubectl get` exposes it via the
# Resource.spec.flags array which carries "TIE_BREAKER".
tb_count=$(kubectl get resources.blockstor.org -o json 2>/dev/null \
    | jq --arg rd "$RD" '[.items[] | select(.spec.resourceDefinitionName == $rd)
                                  | select(.spec.flags // [] | index("TIE_BREAKER"))] | length' \
    2>/dev/null || echo 0)
if (( tb_count > 0 )); then
    echo "FAIL (Bug 334): $tb_count TIE_BREAKER witness Resource(s) found for $RD (-l STORAGE => no witness)" >&2
    kubectl get resources.blockstor.org -o json 2>/dev/null \
        | jq --arg rd "$RD" '.items[] | select(.spec.resourceDefinitionName == $rd) | {name: .metadata.name, flags: .spec.flags}' >&2
    exit 1
fi

# Cross-check via the CLI's `linstor r l -r <rd>` view too — the python
# CLI renders the TieBreaker state in its own column, and a regression
# that bypassed the spec.flags path but still rendered the state would
# surface here.
rows=$("${LCTL[@]}" --machine-readable resource list --resources "$RD" 2>/dev/null \
    | jq -r '[.[][]? | .name] | length' 2>/dev/null || echo 0)
if (( rows != 1 )); then
    echo "FAIL (Bug 334): linstor r l shows $rows rows for $RD, want 1 (no TieBreaker)" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

# --- U110: State / Usage columns for a no-DRBD row ---------------------
# Upstream-issue U110: a storage-layer-only (no-DRBD) resource still has
# to report a State in `linstor r l`. There is no DRBD device, so neither
# BS nor upstream populates a per-volume `state.disk_state` in the `-m`
# JSON (oracle 1.33.2 confirmed: vol.state is `None` for STORAGE-only on
# BOTH). The operator-visible signal is the DERIVED State COLUMN the
# python CLI renders: exactly `Created` for a healthy storage-only
# volume, on both. This pins that surface so a regression that blanked
# the State column, or read the volume as Unknown, would fail here.
echo ">> [U110] assert derived State column reads Created for the no-DRBD row"
deadline=$(( $(date +%s) + 60 ))
state_col=""
while (( $(date +%s) < deadline )); do
    # The python CLI's `r l` State column is the 7th `|`-delimited field
    # for our single-resource row. Match the RD row and pull the State
    # cell, stripping ANSI colour + surrounding whitespace.
    state_col=$("${LCTL[@]}" resource list --resources "$RD" 2>/dev/null \
        | sed 's/\x1b\[[0-9;]*m//g' \
        | awk -F'|' -v rd="$RD" '$0 ~ rd { gsub(/^[ \t]+|[ \t]+$/,"",$7); print $7; exit }')
    [[ "$state_col" == "Created" ]] && break
    sleep 3
done
if [[ "$state_col" != "Created" ]]; then
    echo "FAIL (U110): no-DRBD resource $RD renders State '$state_col', want 'Created'" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -10 >&2
    exit 1
fi
echo ">> [U110] State column: '$state_col' (upstream renders 'Created' here)"

# Usage: nothing observes whether a bare storage volume is open. The
# oracle leaves `rsc.state` EMPTY ({}) for a STORAGE-only resource, so
# `in_use` is absent and the python CLI renders a BLANK Usage cell; it
# prints `Unused` only for an observed `in_use: false`. BS used to send
# that unobserved `false`, which read as an idle volume a pod may in
# fact be writing to. Pin the blank cell.
usage_col=$("${LCTL[@]}" resource list --resources "$RD" 2>/dev/null \
    | sed 's/\x1b\[[0-9;]*m//g' \
    | awk -F'|' -v rd="$RD" '$0 ~ rd { gsub(/^[ \t]+|[ \t]+$/,"",$5); print $5; exit }')
if [[ -n "$usage_col" ]]; then
    echo "FAIL (U110): no-DRBD resource $RD renders Usage '$usage_col', want blank (in_use unknown)" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -10 >&2
    exit 1
fi
echo ">> [U110] Usage column blank (upstream renders it blank here)"

echo ">> r-c-no-drbd-no-tiebreaker OK (Bug 334 pinned: -l STORAGE auto-place=1 yields 1 replica, no TIE_BREAKER witness; U110: State='$state_col', Usage blank)"
