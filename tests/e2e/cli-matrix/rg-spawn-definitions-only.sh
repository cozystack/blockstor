#!/usr/bin/env bash
#
# usage: rg-spawn-definitions-only.sh WORK_DIR
#
# L6 cli-matrix cell: `linstor rg spawn-resources --definition-only`
# creates the definition and its volumes and places nothing. The spawn
# handler ignored `definitions_only` and placed replicas from the group's
# place-count anyway (cozystack/blockstor#216, issue #210).
#
#   $ linstor resource-group create rg --place-count 1 -s stand
#   $ linstor resource-group spawn-resources --definition-only rg rd 32M
#   $ linstor resource list -r rd          # MUST be empty
#
# Unit pin: pkg/rest/create_doors_props_modify_test.go.
# L7 replay: tests/operator-harness/replay/rg-spawn-definitions-only.yaml.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

linstor_cli_setup

RG=cli-matrix-defs-only-rg
RD=cli-matrix-defs-only-rd
POOL=${POOL:-stand}

cleanup() {
    "${LCTL[@]}" resource-definition delete "$RD" >/dev/null 2>&1 || true
    "${LCTL[@]}" resource-group delete "$RG" >/dev/null 2>&1 || true
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> rg create --place-count 1 + spawn-resources --definition-only $RD 32M"
"${LCTL[@]}" resource-group create "$RG" --place-count 1 --storage-pool="$POOL" >/dev/null
"${LCTL[@]}" resource-group spawn-resources --definition-only "$RG" "$RD" 32M >/dev/null

if ! wait_vd_size "$RD" 0 32768 60; then
    echo "FAIL: the spawned definition has no 32 MiB volume" >&2
    "${LCTL[@]}" volume-definition list --resource-definitions "$RD" 2>&1 | tail -20 >&2
    exit 1
fi

echo ">> no replica may appear over 15s"
deadline=$(( $(date +%s) + 15 ))
while (( $(date +%s) < deadline )); do
    count=$(linstor_replica_count "$RD")
    if (( count > 0 )); then
        echo "FAIL: --definition-only spawn placed ${count} replica(s)" >&2
        "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
        exit 1
    fi
    sleep 3
done

echo ">> rg-spawn-definitions-only OK"
