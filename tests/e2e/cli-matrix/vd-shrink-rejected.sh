#!/usr/bin/env bash
#
# usage: vd-shrink-rejected.sh WORK_DIR
#
# L6 cli-matrix cell — P0 shrink-rejection contract.
#
# DRBD physically cannot shrink past the metadata position — once
# meta sits at offset N on the backing disk, shrinking below N
# destroys the meta and the resource is unrecoverable. Upstream
# LINSTOR rejects `vd s` with a strictly-smaller value via a
# STORAGE_POOL_CAPACITY_REDUCTION_FAILED-style envelope. Our REST
# must do the same — silently accepting a shrink would corrupt the
# resource on the next satellite reconcile.
#
# Steps:
#   1. rd c + vd c 2G + r c --auto-place=2 -s lvm-thin
#   2. Wait UpToDate on both placed nodes.
#   3. linstor vd s <rd> 0 500M  → MUST exit non-zero
#   4. stderr must mention "shrink" / "cannot reduce" / "smaller"
#   5. linstor vd l SizeKib still reports 2 GiB (no partial mutation)

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

RD=cli-matrix-shrink-rejected
POOL=${POOL:-lvm-thin}
SIZE_2G_KIB=2097152

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> pre-flight: 2 healthy $POOL SPs"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 2 )); then
    echo "SKIP: $POOL SP not on >=2 nodes (got $ok_nodes)"
    exit 0
fi

echo ">> rd c + vd c 2G + r c --auto-place=2 -s $POOL"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 2G >/dev/null
"${LCTL[@]}" resource create --auto-place=2 --storage-pool="$POOL" "$RD" >/dev/null

# Wait for both replicas to be placed before we attempt the shrink.
# A shrink attempted while one side is still Inconsistent could
# spuriously succeed on the in-memory CRD before the satellite
# observes it — we want the steady-state rejection, not a race.
deadline=$(( $(date +%s) + 90 ))
placed_nodes=()
while (( $(date +%s) < deadline )); do
    mapfile -t placed_nodes < <(
        kubectl get resources.blockstor.io.blockstor.io --no-headers 2>/dev/null \
            | awk -v rd="$RD." '$1 ~ "^"rd {sub(rd, "", $1); print $1}'
    )
    if (( ${#placed_nodes[@]} == 2 )); then break; fi
    sleep 2
done
if (( ${#placed_nodes[@]} != 2 )); then
    echo "FAIL: autoplace did not stage 2 replicas" >&2
    exit 1
fi
wait_uptodate "$RD" "${placed_nodes[0]}" "${placed_nodes[1]}"

echo ">> linstor vd s $RD 0 500M (MUST exit non-zero)"
err_file=$(mktemp)
if "${LCTL[@]}" volume-definition set-size "$RD" 0 500M >/dev/null 2>"$err_file"; then
    echo "FAIL: vd s 2G→500M unexpectedly succeeded" >&2
    echo "   DRBD protocol forbids shrink past meta — REST must reject." >&2
    cat "$err_file" >&2
    exit 1
fi

echo ">> stderr must mention shrink / reduce / smaller"
if ! grep -qiE 'shrink|cannot.*(reduce|shrink)|smaller|reduction|STORAGE_POOL_CAPACITY_REDUCTION_FAILED' "$err_file"; then
    echo "FAIL: shrink rejected but error text is unhelpful:" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi
rm -f "$err_file"

echo ">> SizeKib still 2 GiB after rejected shrink"
cur_kib=$(linstor_vd_size_kib "$RD" 0)
if (( cur_kib != SIZE_2G_KIB )); then
    echo "FAIL: post-reject SizeKib=$cur_kib != $SIZE_2G_KIB" >&2
    echo "   REST mutated state despite rejection — partial-write bug." >&2
    exit 1
fi

echo ">> vd-shrink-rejected OK (shrink properly rejected, size unchanged)"
