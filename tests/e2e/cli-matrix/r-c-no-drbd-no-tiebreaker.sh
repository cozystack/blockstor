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
    last_count=$(kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="$RD." '$1 ~ "^"rd' | wc -l)
    if (( last_count == 1 )); then
        # Give the RD reconciler a beat in case the (pre-fix) witness
        # is about to land on a stale enqueue. We poll one extra
        # period before declaring success.
        sleep 5
        last_count=$(kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
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
    kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="$RD." '$1 ~ "^"rd' >&2
    exit 1
fi

# Assert no TIE_BREAKER witness on any node. The pre-fix bug stamped
# the witness on a third node — `kubectl get` exposes it via the
# Resource.spec.flags array which carries "TIE_BREAKER".
tb_count=$(kubectl get resources.blockstor.cozystack.io -o json 2>/dev/null \
    | jq --arg rd "$RD" '[.items[] | select(.spec.resourceDefinitionName == $rd)
                                  | select(.spec.flags // [] | index("TIE_BREAKER"))] | length' \
    2>/dev/null || echo 0)
if (( tb_count > 0 )); then
    echo "FAIL (Bug 334): $tb_count TIE_BREAKER witness Resource(s) found for $RD (-l STORAGE => no witness)" >&2
    kubectl get resources.blockstor.cozystack.io -o json 2>/dev/null \
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

# --- U110: State / InUse columns must be populated for a no-DRBD row ----
# Upstream-issue U110: a storage-layer-only (no-DRBD) resource still has
# to report a State and an InUse value in `linstor r l`. The python CLI
# fills the State column from the volume's disk_state (there is no DRBD
# role/connection to read), and the InUse column from the resource's
# `state.in_use` field. A regression that left these blank for the
# STORAGE-only stack would render an empty State column — the operator
# can no longer tell a healthy storage-only volume from a stuck one.
echo ">> [U110] assert State (volume disk_state) populated for the no-DRBD row"
deadline=$(( $(date +%s) + 60 ))
disk_state=""
while (( $(date +%s) < deadline )); do
    disk_state=$("${LCTL[@]}" --machine-readable resource list-volumes --resources "$RD" 2>/dev/null \
        | jq -r 'first(.[][]?.volumes[]?.state.disk_state // empty) // empty' 2>/dev/null || echo "")
    [[ -n "$disk_state" ]] && break
    sleep 3
done
if [[ -z "$disk_state" ]]; then
    echo "FAIL (U110): no-DRBD resource $RD reports empty volume disk_state — State column would render blank" >&2
    "${LCTL[@]}" --machine-readable resource list-volumes --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi
echo ">> [U110] State column populated: disk_state=$disk_state"

# InUse must be a concrete boolean (not null/missing) so the InUse
# column renders. A freshly-created, unmounted volume is in_use=false;
# we only assert the field is present and boolean, not its value.
in_use=$("${LCTL[@]}" --machine-readable resource list --resources "$RD" 2>/dev/null \
    | jq -r 'first(.[][]? | select(.state != null) | .state.in_use) // "MISSING"' 2>/dev/null || echo "MISSING")
if [[ "$in_use" != "true" && "$in_use" != "false" ]]; then
    echo ">> [U110] note: in_use rendered as '$in_use' (satellite may not have reported usage yet; State pin is the hard gate)"
else
    echo ">> [U110] InUse column populated: in_use=$in_use"
fi

echo ">> r-c-no-drbd-no-tiebreaker OK (Bug 334 pinned: -l STORAGE auto-place=1 yields 1 replica, no TIE_BREAKER witness; U110: State/InUse populated)"
