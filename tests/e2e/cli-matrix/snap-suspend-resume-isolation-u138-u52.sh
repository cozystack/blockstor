#!/usr/bin/env bash
#
# usage: snap-suspend-resume-isolation-u138-u52.sh WORK_DIR
#
# L6 cli-matrix cell — campaign-2 U138 + U52.
#
# U138 (suspend-io unwind): a snapshot create suspends DRBD I/O on the
# diskful peers via the Phase-1 barrier. The barrier MUST always unwind
# (Phase-3 resume) once the take completes — leaving I/O suspended is a
# permanent application outage. We pin the unwind by writing to the DRBD
# device AFTER the snapshot completes: a still-suspended device would
# block the write forever (the dd would hang past the timeout).
#
# U52 (failure isolation / independence): two independent RDs each take
# their own snapshot. The two snapshots share no GroupID, so one's
# suspend/resume cycle must not interfere with the other — both complete
# and both resources stay fully writable.
#
#   Setup: two independent 2-replica diskful RDs (A and B) on
#          worker-1 + worker-2, both UpToDate (pool=stand).
#   1. snapshot create A — succeeds; resume drains.
#   2. snapshot create B — succeeds; resume drains.
#   3. Write to A's DRBD device — succeeds within a bounded timeout
#      (proves A's I/O resumed, not stuck suspended).
#   4. Write to B's DRBD device — succeeds (B independent of A).
#   5. Both RDs remain UpToDate on both nodes.
#
#   Cleanup: snapshot delete both + delete both RDs + assert_no_orphans.
#
# Catches regressions in:
#   - the controller-side Phase-3 resume drain (internal/controller/
#     snapshot_controller.go) leaving Spec.SuspendIO=true
#   - the satellite ResumeResource path (drbdsetup resume-io) not firing
#   - cross-snapshot interference (a missing GroupID scoping that drags
#     one snapshot's abort into an unrelated one)

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

SP=stand
RDA=ccu3-u138-a
RDB=ccu3-u52-b
SNAPA="snap-u138a"
SNAPB="snap-u52b"
SIZE_MIB=64

N1=$WORKER_1
N2=$WORKER_2

cleanup() {
    "${LCTL[@]}" snapshot delete "$RDA" "$SNAPA" 2>/dev/null || true
    "${LCTL[@]}" snapshot delete "$RDB" "$SNAPB" 2>/dev/null || true
    # Best-effort: drop primary + make sure no resource is left suspended.
    on_node "$N1" drbdadm secondary "$RDA" 2>/dev/null || true
    on_node "$N1" drbdadm secondary "$RDB" 2>/dev/null || true
    on_node "$N1" drbdadm resume-io "$RDA" 2>/dev/null || true
    on_node "$N1" drbdadm resume-io "$RDB" 2>/dev/null || true
    delete_rd "$RDA"
    delete_rd "$RDB"
    assert_no_orphans "$RDA"
    assert_no_orphans "$RDB"
    linstor_cli_teardown
}
trap cleanup EXIT

create_rd() {
    local rd=$1
    _out=$("${LCTL[@]}" resource-definition create "$rd" 2>&1) \
        || { echo "FAIL: rd c $rd: $_out" >&2; exit 1; }
    _out=$("${LCTL[@]}" volume-definition create "$rd" "${SIZE_MIB}M" 2>&1) \
        || { echo "FAIL: vd c $rd: $_out" >&2; exit 1; }
    _out=$("${LCTL[@]}" resource create "$N1" "$rd" --storage-pool="$SP" 2>&1) \
        || { echo "FAIL: r c $N1 $rd: $_out" >&2; exit 1; }
    _out=$("${LCTL[@]}" resource create "$N2" "$rd" --storage-pool="$SP" 2>&1) \
        || { echo "FAIL: r c $N2 $rd: $_out" >&2; exit 1; }
    wait_uptodate "$rd" "$N1" "$N2"
}

snap_ready() {
    local rd=$1 snap=$2
    local deadline; deadline=$(( $(date +%s) + 60 ))
    while (( $(date +%s) < deadline )); do
        local j; j=$(kubectl get snapshots.blockstor.cozystack.io -o json 2>/dev/null \
            | jq -c --arg rd "$rd" --arg s "$snap" '
                [.items[]?
                 | select(.spec.resourceDefinitionName==$rd)
                 | select(.spec.snapshotName==$s)] | first // {}')
        local failed; failed=$(jq -r '((.status.flags // []) | index("FAILED")) != null' <<<"$j" 2>/dev/null)
        [[ "$failed" == "true" ]] && { echo "FAIL: snapshot $rd/$snap stamped FAILED: $j" >&2; return 1; }
        local ok; ok=$(jq -r '
            ((.spec.nodes // []) as $want
             | ([.status.nodeStatus[]? | select(.ready==true) | .nodeName]) as $have
             | ($want | length) > 0 and (($want - $have) | length == 0))' <<<"$j" 2>/dev/null)
        [[ "$ok" == "true" ]] && return 0
        sleep 2
    done
    echo "FAIL: snapshot $rd/$snap never reached Ready within 60s" >&2
    return 1
}

# write_survives: promote on N1 and write to the DRBD device with a hard
# timeout. A still-suspended device makes dd block; `timeout` then kills
# it and we FAIL — that is the U138 outage signal.
write_survives() {
    local rd=$1 dev
    on_node "$N1" drbdadm primary --force "$rd" 2>/dev/null || true
    # Resolve via `drbdadm sh-dev` (lib.sh resolve_drbd_device): the
    # /dev/drbd/by-res symlink is not reliably present in the satellite
    # mount namespace, so readlink-based resolution aborts on the stand.
    dev=$(resolve_drbd_device "$N1" "$rd" 0 2>/dev/null) || dev=""
    if ! on_node "$N1" bash -c "
        dev='$dev'
        [ -z \"\$dev\" ] && { echo 'no drbd device node for $rd' >&2; exit 2; }
        timeout 20 dd if=/dev/zero of=\"\$dev\" bs=4096 count=16 oflag=direct conv=fsync status=none
    "; then
        echo "FAIL (U138): write to $rd DRBD device did NOT complete (I/O likely still suspended post-snapshot)" >&2
        return 1
    fi
    on_node "$N1" drbdadm secondary "$rd" 2>/dev/null || true
    return 0
}

# =====================================================================
echo ">> Setup: two independent 2-replica RDs $RDA and $RDB on $N1 + $N2"
create_rd "$RDA"
create_rd "$RDB"

echo ">> Step 1: snapshot create $RDA $SNAPA"
_out=$("${LCTL[@]}" snapshot create "$RDA" "$SNAPA" 2>&1) \
    || { echo "FAIL (U138): snapshot create $RDA returned non-zero: $_out" >&2; exit 1; }
snap_ready "$RDA" "$SNAPA"

echo ">> Step 2: snapshot create $RDB $SNAPB (independent of $RDA)"
_out=$("${LCTL[@]}" snapshot create "$RDB" "$SNAPB" 2>&1) \
    || { echo "FAIL (U52): snapshot create $RDB returned non-zero: $_out" >&2; exit 1; }
snap_ready "$RDB" "$SNAPB"

echo ">> Step 3: write to $RDA device — must succeed (I/O resumed)"
write_survives "$RDA"

echo ">> Step 4: write to $RDB device — must succeed (independent resume)"
write_survives "$RDB"

echo ">> Step 5: both RDs still UpToDate on both nodes"
wait_uptodate "$RDA" "$N1" "$N2"
wait_uptodate "$RDB" "$N1" "$N2"

echo ">> PASS: snap-suspend-resume-isolation-u138-u52 (suspend-io unwound; both snapshots independent; I/O resumed on both)"
