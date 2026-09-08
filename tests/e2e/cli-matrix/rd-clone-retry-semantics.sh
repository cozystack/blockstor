#!/usr/bin/env bash
#
# usage: rd-clone-retry-semantics.sh WORK_DIR
#
# L6 cli-matrix cell — the clone endpoint's retry contract, and the
# props-modify triple it accepts.
#
# Three behaviours that are new on this surface and that unit tests
# cannot reach, because each of them is about what the OPERATOR sees
# on a second invocation of the same verb:
#
#   A. Replaying a finished clone succeeds. linstor-csi re-issues
#      CreateVolume whenever a response is lost or external-provisioner
#      restarts, so `rd clone <src> <dst>` run twice must answer the
#      same way twice. A finished clone is a copy of a point-in-time.
#
#   B. Replaying it AFTER the source grows still succeeds. This is the
#      regression the guard introduced: comparing the internal snapshot
#      against the live source made an ordinary `vd set-size` poison
#      every later retry, permanently — the snapshot is deterministic
#      and outlives the clone by design (delta row 82), so no repeat
#      could ever get past it, and its own correction ("delete the
#      snapshot") cannot be followed, because that snapshot is the
#      origin the existing clone depends on.
#
#   C. A clone body carrying `delete_namespaces` is accepted. The verb
#      has no flag for it, so this is driven over raw REST: the field
#      reaches the endpoint from golinstor's GenericPropsModify. The
#      body is decoded with DisallowUnknownFields, so before the triple
#      was declared an undeclared field was a 400 before any of the
#      handler ran.
#
# Contract:
#   1. first clone: exit 0, target materialises replicas, UpToDate.
#   2. second clone, same argv: exit 0, and the target still holds
#      exactly the volume the first one produced (a replay must not
#      re-shape what it replays).
#   3. `vd set-size` on the source, then a third clone: exit 0.
#   4. clone POST carrying delete_namespaces: HTTP 201.
#
# Pool: `stand` (FILE_THIN), the pool the sibling clone cell uses.
#
# Unit pins: pkg/rest/rd_clone_review3_test.go (replay after resize,
# leftover shapes) and pkg/rest/rd_clone_idempotency_test.go
# (delete_namespaces). This cell is the stand-side companion.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

SRC=cli-matrix-retry-src
DST=cli-matrix-retry-dst
DST_NS=cli-matrix-retry-ns
POOL=${POOL:-stand}

N1=$WORKER_1
N2=$WORKER_2

cleanup() {
    delete_rd "$DST"
    delete_rd "$DST_NS"
    delete_rd "$SRC"
    assert_no_orphans "$DST"
    assert_no_orphans "$DST_NS"
    assert_no_orphans "$SRC"
    linstor_cli_teardown
}
trap cleanup EXIT

# clone_volume_size <rd> — the target's volume 0 size in KiB, as the
# controller reports it.
clone_volume_size() {
    local rd=$1
    "${LCTL[@]}" --machine-readable volume-definition list --resource-definitions "$rd" \
        2>/dev/null | jq -r '..|.volume_definitions? // empty | .[0].size_kib // empty' | head -1
}

echo ">> source RD: 2 diskful replicas on $POOL"
"${LCTL[@]}" resource-definition create "$SRC" >/dev/null
"${LCTL[@]}" volume-definition create "$SRC" 64M >/dev/null
"${LCTL[@]}" resource create "$N1" "$SRC" --storage-pool="$POOL" >/dev/null
"${LCTL[@]}" resource create "$N2" "$SRC" --storage-pool="$POOL" >/dev/null
wait_uptodate "$SRC" "$N1" "$N2"

echo ">> [A] first clone"
if ! "${LCTL[@]}" resource-definition clone "$SRC" "$DST" >/dev/null 2>&1; then
    echo "FAIL: first clone exited non-zero" >&2
    exit 1
fi

deadline=$(( $(date +%s) + 120 ))
dst_nodes=()
while (( $(date +%s) < deadline )); do
    mapfile -t dst_nodes < <(linstor_diskful_nodes "$DST")
    (( ${#dst_nodes[@]} == 2 )) && break
    sleep 2
done
if (( ${#dst_nodes[@]} != 2 )); then
    echo "FAIL: clone $DST never materialised 2 diskful replicas" >&2
    exit 1
fi
wait_uptodate "$DST" "${dst_nodes[0]}" "${dst_nodes[1]}"

size_after_first=$(clone_volume_size "$DST")

echo ">> [A] replay of the finished clone answers the same way"
if ! "${LCTL[@]}" resource-definition clone "$SRC" "$DST" >/dev/null 2>&1; then
    echo "FAIL: replaying a finished clone exited non-zero" >&2
    echo "  CSI re-issues CreateVolume whenever a response is lost" >&2
    exit 1
fi

size_after_replay=$(clone_volume_size "$DST")
if [[ "$size_after_first" != "$size_after_replay" ]]; then
    echo "FAIL: the replay re-shaped the clone ($size_after_first -> $size_after_replay KiB)" >&2
    exit 1
fi

echo ">> [B] grow the source, then replay again"
"${LCTL[@]}" volume-definition set-size "$SRC" 0 128M >/dev/null
if ! "${LCTL[@]}" resource-definition clone "$SRC" "$DST" >/dev/null 2>&1; then
    echo "FAIL: replay after a source resize exited non-zero" >&2
    echo "  a finished clone is a copy of a point-in-time and owes the source nothing;" >&2
    echo "  the internal snapshot outlives it by design, so this refusal was permanent" >&2
    exit 1
fi

size_after_resize=$(clone_volume_size "$DST")
if [[ "$size_after_first" != "$size_after_resize" ]]; then
    echo "FAIL: the replay resized the finished clone ($size_after_first -> $size_after_resize KiB)" >&2
    exit 1
fi

echo ">> [C] a clone body carrying delete_namespaces is accepted"
# Driven over raw REST, not through the verb: the clone CLI has no flag for
# this (1.31.0 offers --external-name, --use-zfs-clone, --volume-passphrase,
# --layer-list, --resource-group). delete_namespaces reaches the endpoint from
# golinstor's GenericPropsModify, which is the shape under test — and the body
# is decoded with DisallowUnknownFields, so before the triple was declared an
# undeclared field was a 400 before any of the handler ran.
"${LCTL[@]}" resource-definition set-property "$SRC" DrbdOptions/Net/protocol C >/dev/null 2>&1 || true
http_code=$(curl -sS -m 30 -o /tmp/cli-matrix-retry-ns.json -w '%{http_code}' \
    -X POST -H 'Content-Type: application/json' \
    -d "{\"name\":\"${DST_NS}\",\"delete_namespaces\":[\"DrbdOptions\"],\"use_zfs_clone\":true}" \
    "http://127.0.0.1:${LCTL_PORT}/v1/resource-definitions/${SRC}/clone" \
    2>/dev/null || echo "000")
if [[ "$http_code" != "201" ]]; then
    echo "FAIL: clone with delete_namespaces answered HTTP $http_code, want 201" >&2
    cat /tmp/cli-matrix-retry-ns.json >&2 2>/dev/null || true
    exit 1
fi

echo ">> rd-clone-retry-semantics OK (replay is idempotent across a source resize; delete-namespace accepted)"
