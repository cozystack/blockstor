#!/usr/bin/env bash
#
# usage: snap-restore-retry-semantics.sh WORK_DIR
#
# L6 cli-matrix cell — the resource restore's retry contract
# (cozystack/blockstor#186).
#
# linstor-csi restores a VolumeSnapshot into a PVC the way LINSTOR
# documents it: `rd c <target>`, `s vd restore` into it, then
# `s rsc restore`. Two behaviours a unit test cannot show, because each is
# about what the OPERATOR sees on a real stand:
#
#   A. The resource restore takes the definition the first two steps made
#      as its own target. It carries no restore marker yet; its volumes
#      are the snapshot's and it holds no live replica, which is exactly
#      the state the documented sequence leaves.
#
#   B. Run again with the same argv, the retry CSI issues when a response
#      is lost, it answers success and places nothing more: the target
#      keeps one diskful replica and the size it was restored at.
#
# Contract:
#   1. source RD with one UpToDate diskful replica, snapshot created.
#   2. rd c / s vd restore / s rsc restore onto node 1: exit 0 each, the
#      restored replica reaches UpToDate.
#   3. the same s rsc restore again: exit 0, still exactly one diskful
#      replica, volume size unchanged.
#
# Unit pins: pkg/rest/snapshot_restore_csi_flow_test.go and
# pkg/rest/snapshot_restore_idempotency_test.go (REST door),
# internal/cli/snapshot_restore_prepared_test.go (CLI door). L7 companion:
# tests/operator-harness/replay/snap-restore-retry-semantics.yaml.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

linstor_cli_setup

SRC=cli-matrix-rstretry-src
TGT=cli-matrix-rstretry-tgt
SNAP=rstretry
POOL=${POOL:-stand}

N1=$WORKER_1

# The snapshot has to go before the source can: the trap deletes it first.
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

target_volume_sizes() {
    "${LCTL[@]}" --machine-readable volume-definition list --resource-definitions "$TGT" \
        2>/dev/null | jq -r '..|.volume_definitions? // empty | .[] | "\(.volume_number)=\(.size_kib)"' \
        | sort | paste -sd, -
}

echo ">> source RD: 1 diskful replica on $POOL, then a snapshot"
"${LCTL[@]}" resource-definition create "$SRC" >/dev/null
"${LCTL[@]}" volume-definition create "$SRC" 64M >/dev/null
"${LCTL[@]}" resource create "$N1" "$SRC" --storage-pool="$POOL" >/dev/null
wait_uptodate "$SRC" "$N1"
"${LCTL[@]}" snapshot create "$SRC" "$SNAP" >/dev/null

echo ">> [A] the sequence linstor-csi issues"
"${LCTL[@]}" resource-definition create "$TGT" >/dev/null
if ! out=$("${LCTL[@]}" snapshot volume-definition restore \
        --from-resource "$SRC" --from-snapshot "$SNAP" --to-resource "$TGT" 2>&1); then
    echo "FAIL: s vd restore into the prepared target: $out" >&2
    exit 1
fi
if ! out=$("${LCTL[@]}" snapshot resource restore \
        --from-resource "$SRC" --from-snapshot "$SNAP" --to-resource "$TGT" "$N1" 2>&1); then
    echo "FAIL: s rsc restore refused the target its own sequence prepared: $out" >&2
    exit 1
fi
wait_uptodate "$TGT" "$N1"

size_after_restore=$(target_volume_sizes)
# An empty read compares equal to another empty read; refuse to go on with one.
if ! [[ "$size_after_restore" =~ ^[0-9]+=[0-9]+ ]]; then
    echo "FAIL: could not read the volumes of $TGT (got '$size_after_restore')" >&2
    exit 1
fi

echo ">> [B] the retry answers the same way and places nothing more"
if ! out=$("${LCTL[@]}" snapshot resource restore \
        --from-resource "$SRC" --from-snapshot "$SNAP" --to-resource "$TGT" "$N1" 2>&1); then
    echo "FAIL: retrying the resource restore exited non-zero: $out" >&2
    echo "  linstor-csi re-issues CreateVolume whenever a response is lost" >&2
    exit 1
fi

sleep 10
mapfile -t diskful < <(linstor_diskful_nodes "$TGT")
if (( ${#diskful[@]} != 1 )); then
    echo "FAIL: the retry left ${#diskful[@]} diskful replicas on $TGT, want 1: ${diskful[*]}" >&2
    exit 1
fi

size_after_retry=$(target_volume_sizes)
if [[ "$size_after_restore" != "$size_after_retry" ]]; then
    echo "FAIL: the retry re-shaped the target ($size_after_restore -> $size_after_retry)" >&2
    exit 1
fi

echo ">> snap-restore-retry-semantics OK (prepared target taken; the retry is idempotent)"
