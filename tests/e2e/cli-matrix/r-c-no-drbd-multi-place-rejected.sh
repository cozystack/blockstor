#!/usr/bin/env bash
#
# usage: r-c-no-drbd-multi-place-rejected.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 335 (user-reported, data-loss class).
#
# Reproduction from the user's stand:
#
#   $ linstor r c test3 --auto-place=2 -l STORAGE -s stand
#   SUCCESS
#
# Pre-fix behaviour: 2 INDEPENDENT local volumes on 2 nodes, no
# inter-node replication (DRBD absent from the layer list). The
# first write to either replica diverges silently from the other.
# The CLI surfaced "SUCCESS" and the operator only discovered the
# divergence much later.
#
# Post-fix contract: `--auto-place=N` (N>1) with `-l STORAGE` (no
# replication layer) MUST exit non-zero with an actionable error
# envelope. No Resource CRDs may be left behind by the rejected
# call.
#
# This cell drives the real linstor CLI against a freshly-provisioned
# 3-node stand (any provider — the gate is layer-stack-driven, not
# pool-kind-driven) and asserts both the exit-code and the
# stderr-envelope-shape contract.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-335
POOL=${POOL:-lvm-thin}

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

# Pre-flight: at least one healthy SP exists across the cluster. The
# bug is layer-stack-driven (no DRBD => reject multi-place); the
# provider kind doesn't matter, we just need SOME pool to point the
# request at so the placer would have a chance to run absent the
# gate.
echo ">> pre-flight: healthy $POOL SPs"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 2 )); then
    echo "SKIP: $POOL SP not on >=2 nodes (got $ok_nodes) — Bug 335 multi-place fixture not available"
    exit 0
fi

echo ">> [Bug 335] linstor rd c + vd c"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 1G >/dev/null

echo ">> [Bug 335] linstor r c $RD --auto-place=2 -l STORAGE -s $POOL (must be rejected)"
err_file=$(mktemp)
out_file=$(mktemp)
set +e
"${LCTL[@]}" resource create --auto-place=2 --layer-list storage --storage-pool="$POOL" "$RD" >"$out_file" 2>"$err_file"
rc=$?
set -e

if (( rc == 0 )); then
    echo "FAIL (Bug 335): --auto-place=2 -l STORAGE exited 0; must be rejected (data-divergence hazard)" >&2
    echo "----- stdout -----" >&2
    cat "$out_file" >&2
    echo "----- stderr -----" >&2
    cat "$err_file" >&2
    rm -f "$err_file" "$out_file"
    exit 1
fi

# Stderr (or stdout — linstor sometimes routes API errors through stdout) MUST
# explain the no-replication-layer hazard so an operator can grep their
# CI log for the actionable cause.
combined=$(cat "$err_file" "$out_file")
if ! grep -qiE 'replication layer|diverge|data-divergence' <<<"$combined"; then
    echo "FAIL (Bug 335): error message must mention 'replication layer' or 'diverge'; got:" >&2
    echo "$combined" >&2
    rm -f "$err_file" "$out_file"
    exit 1
fi

rm -f "$err_file" "$out_file"

# The gate must fire BEFORE the placer — no Resource CRDs may have
# been written even partially. We assert via the CRD layer because
# the linstor CLI's `r l` view can be ambiguous on transient state
# during a rejected request.
echo ">> [Bug 335] assert no Resource CRDs leaked for rejected $RD"
leaked=$(kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
    | awk -v rd="$RD." '$1 ~ "^"rd' | wc -l)
if (( leaked > 0 )); then
    echo "FAIL (Bug 335): gate fired but $leaked Resource CRDs leaked:" >&2
    kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
        | awk -v rd="$RD." '$1 ~ "^"rd' >&2
    exit 1
fi

echo ">> r-c-no-drbd-multi-place-rejected OK (Bug 335 pinned: --auto-place=2 -l STORAGE refused with no leaked Resources)"
