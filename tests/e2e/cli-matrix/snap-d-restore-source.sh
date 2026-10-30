#!/usr/bin/env bash
#
# usage: snap-d-restore-source.sh WORK_DIR
#
# L6 cli-matrix cell: a snapshot a restore still reads from is kept until
# the restore is done, and a delete of a snapshot that is gone stays
# idempotent (cozystack/blockstor#197).
#
# A restored definition keeps its restore marker, and while the marker is
# there every new replica is brought up from the snapshot. Deleting the
# snapshot under an unfinished restore left the next replica blank and
# never UpToDate.
#
# Contract:
#   1. source RD with one UpToDate diskful replica, snapshot created.
#   2. bare s rsc restore: the target has its volumes and no replica.
#   3. s d: non-zero, the snapshot is still listed.
#   4. r c the target on node 1: UpToDate.
#   5. s d: exit 0, and the target no longer carries the restore marker.
#   6. s d again: exit 0 over a snapshot that is gone.
#   7. r c the target on node 2: UpToDate, synced from node 1.
#
# Unit pins: pkg/rest/restore_source_lifetime_test.go,
# pkg/rest/snapshot_delete_restore_race_test.go (REST door),
# internal/cli/snapshot_delete_restore_source_test.go (CLI door). L7
# companion: tests/operator-harness/replay/snap-d-restore-source.yaml.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

SRC=cli-matrix-sdsrc-src
TGT=cli-matrix-sdsrc-tgt
SNAP=sdsrc
POOL=${POOL:-stand}

N1=$WORKER_1
N2=$WORKER_2

# The target goes first, then the snapshot, then the source.
cleanup() {
    local rc=$? clean=0 rd
    set +e
    "${LCTL[@]}" resource-definition delete "$TGT" >/dev/null 2>&1
    "${LCTL[@]}" snapshot delete "$SRC" "$SNAP" >/dev/null 2>&1
    strict_cleanup_on_exit "${_STRICT_CLEANUP_RDS[@]}" || clean=1
    for rd in "${_STRICT_CLEANUP_RDS[@]}"; do
        assert_no_orphans "$rd" || clean=1
    done
    linstor_cli_teardown
    if [[ $rc -eq 0 && $clean -ne 0 ]]; then
        echo "FAIL: the cluster could not be cleaned after the cell; PASS demoted to FAIL" >&2
        rc=1
    fi
    exit "$rc"
}
register_strict_cleanup "$TGT" "$SRC"
trap cleanup EXIT

echo ">> source RD: 1 diskful replica on $POOL, then a snapshot"
"${LCTL[@]}" resource-definition create "$SRC" >/dev/null
"${LCTL[@]}" volume-definition create "$SRC" 64M >/dev/null
"${LCTL[@]}" resource create "$N1" "$SRC" --storage-pool="$POOL" >/dev/null
wait_uptodate "$SRC" "$N1"
"${LCTL[@]}" snapshot create "$SRC" "$SNAP" >/dev/null

echo ">> bare restore: the target has its volumes and no replica yet"
if ! out=$("${LCTL[@]}" snapshot resource restore \
        --from-resource "$SRC" --from-snapshot "$SNAP" --to-resource "$TGT" 2>&1); then
    echo "FAIL: bare s rsc restore: $out" >&2
    exit 1
fi

echo ">> s d while the restore is unfinished must be refused"
if out=$("${LCTL[@]}" snapshot delete "$SRC" "$SNAP" 2>&1); then
    echo "FAIL: s d took the source of an unfinished restore: $out" >&2
    exit 1
fi
if ! "${LCTL[@]}" snapshot list 2>/dev/null | grep -q "$SNAP"; then
    echo "FAIL: the refused s d took the snapshot anyway" >&2
    exit 1
fi

echo ">> place the restored replica, then s d succeeds"
"${LCTL[@]}" resource create "$N1" "$TGT" --storage-pool="$POOL" >/dev/null
wait_uptodate "$TGT" "$N1"
if ! out=$("${LCTL[@]}" snapshot delete "$SRC" "$SNAP" 2>&1); then
    echo "FAIL: s d after the restore finished: $out" >&2
    exit 1
fi
if "${LCTL[@]}" resource-definition list-properties "$TGT" 2>/dev/null \
        | grep -q BlockstorRestoreFromSnapshot; then
    echo "FAIL: $TGT still names the deleted snapshot as its restore source" >&2
    exit 1
fi

echo ">> s d again: the snapshot is gone, the delete is still a success"
if ! out=$("${LCTL[@]}" snapshot delete "$SRC" "$SNAP" 2>&1); then
    echo "FAIL: s d of a snapshot that is gone exited non-zero: $out" >&2
    exit 1
fi

echo ">> a later replica syncs from its peer"
"${LCTL[@]}" resource create "$N2" "$TGT" --storage-pool="$POOL" >/dev/null
wait_uptodate "$TGT" "$N1" "$N2"

echo ">> snap-d-restore-source OK (source kept while needed, released after, delete idempotent)"
