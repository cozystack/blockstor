#!/usr/bin/env bash
#
# usage: r-l-created-on.sh WORK_DIR
#
# L6 cli-matrix cell — `linstor r list` CreatedOn parity.
#
# Upstream LINSTOR populates the `create_timestamp` wire field for every
# resource; the Python CLI renders it as the `CreatedOn` column (unix
# milliseconds / 1000). blockstor previously left it unset, so `r l`
# showed a blank CreatedOn for every replica. The fix sources it,
# persistence-free, from the backing Resource CRD's
# metadata.creationTimestamp (per replica) in crdToWireResource.
#
# This cell pins the operator-visible result: after `r c`, both the
# machine-readable `create_timestamp` and the human `CreatedOn` column
# are non-empty and carry a plausibly-recent timestamp.
#
# Verified live against the upstream LINSTOR oracle (1.33.2), which
# fills CreatedOn the same way (2026-06-08 A/B on the dev stand).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

linstor_cli_setup

RD=cli-matrix-createdon-rd
POOL=${POOL:-stand}

cleanup() {
    "${LCTL[@]}" resource-definition delete "$RD" >/dev/null 2>&1 || true
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

# Lower bound captured just before the create, with 5s of slack for
# clock skew between this host and the apiserver node.
before_ms=$(( $(date +%s) * 1000 - 5000 ))

echo ">> rd c + vd c + r c $WORKER_1 (diskful) for $RD"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 64M >/dev/null
"${LCTL[@]}" resource create "$WORKER_1" "$RD" --storage-pool="$POOL" >/dev/null

if ! wait_replica_count "$RD" 1 60; then
    echo "FAIL: RD did not reach 1 replica" >&2
    "${LCTL[@]}" resource list -r "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

echo ">> create_timestamp MUST be non-empty in machine-readable r l"
ts=$(linstor_r_l_json "$RD" \
    | jq -r '[.[][]? | .create_timestamp] | map(select(. != null and . > 0)) | (.[0] // 0)')
if [[ -z "$ts" || "$ts" == "0" || "$ts" == "null" ]]; then
    echo "FAIL: r l create_timestamp empty/zero — upstream populates it (CreatedOn column)" >&2
    linstor_r_l_json "$RD" | jq -c '.[][]? | {node: .node_name, create_timestamp}' >&2 || true
    exit 1
fi

now_ms=$(( $(date +%s) * 1000 + 5000 ))
if (( ts < before_ms || ts > now_ms )); then
    echo "FAIL: create_timestamp=$ts not in plausible window [$before_ms, $now_ms] (unix ms)" >&2
    exit 1
fi
echo ">> create_timestamp=$ts (plausible, unix ms) — OK"

echo ">> human r l CreatedOn column MUST render a date"
human=$("${LCTL[@]}" resource list -r "$RD" 2>/dev/null \
    | grep -E "$RD" \
    | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}' | head -1)
if [[ -z "$human" ]]; then
    echo "FAIL: human r l CreatedOn column is blank" >&2
    "${LCTL[@]}" resource list -r "$RD" >&2
    exit 1
fi

echo ">> r-l-created-on OK (create_timestamp=$ts, CreatedOn=$human)"
