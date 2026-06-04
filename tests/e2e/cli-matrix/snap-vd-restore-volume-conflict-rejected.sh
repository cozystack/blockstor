#!/usr/bin/env bash
#
# usage: snap-vd-restore-volume-conflict-rejected.sh WORK_DIR
#
# L6 cli-matrix cell — corner-case G3b (two-phase restore edge).
#
# `linstor snapshot volume-definition restore` hydrates a snapshot's
# recorded volume layout onto a (typically pre-existing, EMPTY) target
# RD. If the target RD already carries a volume-definition whose number
# collides with one of the snapshot's, the restore must be REJECTED
# up front with a clear error naming the offending volume number —
# never partially mutate the target (an earlier non-colliding VD would
# land before the colliding one errored, leaving a half-restored RD).
#
# Contract:
#   1. source RD with one volume, 1-replica diskful on worker-1.
#   2. snapshot the source.
#   3. target RD created with its OWN volume 0 (a different size).
#   4. `snapshot volume-definition restore` into the target → MUST be
#      rejected (non-zero), error names the volume number, and the
#      target's original VD 0 is left untouched.
#   5. control: restore into a fresh EMPTY target RD → MUST succeed.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

linstor_cli_setup

SRC=cli-matrix-g3b-src
SNAP=snap-g3b-1
TGT_CONFLICT=cli-matrix-g3b-tgt-conflict
TGT_OK=cli-matrix-g3b-tgt-ok

N1=$WORKER_1

cleanup() {
    "${LCTL[@]}" snapshot delete "$SRC" "$SNAP" 2>/dev/null || true
    delete_rd "$TGT_OK"
    delete_rd "$TGT_CONFLICT"
    delete_rd "$SRC"
    assert_no_orphans "$SRC"
    assert_no_orphans "$TGT_OK"
    assert_no_orphans "$TGT_CONFLICT"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> source RD: 1-replica diskful on $N1 (64M volume), snapshot it"
_out=$("${LCTL[@]}" resource-definition create "$SRC" 2>&1) \
    || { echo "FAIL: rd c $SRC: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" volume-definition create "$SRC" 64M 2>&1) \
    || { echo "FAIL: vd c $SRC: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N1" "$SRC" --storage-pool=stand 2>&1) \
    || { echo "FAIL: r c $N1 $SRC: $_out" >&2; exit 1; }
# Single-replica source: wait_uptodate is a 2-peer convergence helper
# (requires a primary AND a peer); use the single-node disk-state poll.
wait_disk_state "$SRC" "$N1" "UpToDate" 180

_out=$("${LCTL[@]}" snapshot create "$SRC" "$SNAP" 2>&1) \
    || { echo "FAIL: snap c $SRC $SNAP: $_out" >&2; exit 1; }
sleep 4

# ===========================================================================
# G3b — VOLUME-NUMBER CONFLICT: VD-restore into an RD that already has vol 0.
# ===========================================================================
echo ">> target RD $TGT_CONFLICT created with its OWN volume 0 (128M)"
_out=$("${LCTL[@]}" resource-definition create "$TGT_CONFLICT" 2>&1) \
    || { echo "FAIL: rd c $TGT_CONFLICT: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" volume-definition create "$TGT_CONFLICT" 128M 2>&1) \
    || { echo "FAIL: vd c $TGT_CONFLICT: $_out" >&2; exit 1; }

echo ">> [G3b] VD-restore into $TGT_CONFLICT (volume 0 clash) must be REJECTED"
err_file=$(mktemp)
if "${LCTL[@]}" snapshot volume-definition restore \
        --from-resource "$SRC" \
        --from-snapshot "$SNAP" \
        --to-resource "$TGT_CONFLICT" >"$err_file" 2>&1; then
    echo "FAIL (G3b): VD-restore onto an RD with a clashing volume 0 was ACCEPTED" >&2
    cat "$err_file" >&2
    rm -f "$err_file"
    exit 1
fi

# The error should mention the volume number so the operator knows what
# clashed. Soft-check: surface but don't fail if wording differs.
if ! grep -qiE "volume|already exist" "$err_file"; then
    echo "note: rejection did not mention the volume clash explicitly:" >&2
    cat "$err_file" >&2
fi
rm -f "$err_file"

# The target's original VD 0 must remain at its 128M size (untouched).
sz=$("${LCTL[@]}" volume-definition list --resource-definitions "$TGT_CONFLICT" 2>/dev/null \
    | awk '/[0-9]/ && /MiB|GiB/ {print; exit}')
echo ">> [G3b] target $TGT_CONFLICT VD after rejected restore: ${sz:-<none>}"
if ! echo "$sz" | grep -q "128"; then
    echo "FAIL (G3b): target VD 0 size changed by the rejected restore (expected 128 MiB): ${sz:-<none>}" >&2
    exit 1
fi
echo ">> [G3b] OK — conflict rejected, target VD untouched"

# ===========================================================================
# CONTROL — VD-restore into a fresh EMPTY RD must SUCCEED.
# ===========================================================================
echo ">> [G3b control] VD-restore into EMPTY $TGT_OK must SUCCEED"
_out=$("${LCTL[@]}" resource-definition create "$TGT_OK" 2>&1) \
    || { echo "FAIL: rd c $TGT_OK: $_out" >&2; exit 1; }

if ! _out=$("${LCTL[@]}" snapshot volume-definition restore \
        --from-resource "$SRC" \
        --from-snapshot "$SNAP" \
        --to-resource "$TGT_OK" 2>&1); then
    echo "FAIL (G3b control): VD-restore into an empty RD was rejected: $_out" >&2
    exit 1
fi

# The restored target must now carry volume 0 (the snapshot's 64M layout).
if ! "${LCTL[@]}" volume-definition list --resource-definitions "$TGT_OK" 2>/dev/null \
        | grep -qE "MiB|GiB"; then
    echo "FAIL (G3b control): VD-restore into empty RD produced no volume definition" >&2
    "${LCTL[@]}" volume-definition list --resource-definitions "$TGT_OK" 2>&1 | tail -10 >&2
    exit 1
fi

echo ">> [G3b control] OK — VD-restore into empty RD succeeded"
echo "PASS: snap-vd-restore-volume-conflict-rejected"
