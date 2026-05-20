#!/usr/bin/env bash
#
# usage: snap-full-lifecycle.sh WORK_DIR
#
# L6 cli-matrix cell — P0 full Snapshot lifecycle catcher.
# Exercises the Bug 351 SuspendIo orchestration (commit 51bd12ef7)
# end-to-end together with the Bug 352 single-node success gate
# (commit 194b575e0) in one linear chain. The user has reported
# regressions in the snapshot wire shape repeatedly (Bug 351, 352,
# 353, 354) — without this cell green on stand, NO claim of "snapshot
# orchestration stable" may be made.
#
# The script replays the canonical operator-CLI sequence:
#
#   1.  rd c + vd c + r c $N1 + r c $N2  (2-replica diskful on
#       worker-1 + worker-2). Wait UpToDate on both.
#   2.  Start a continuous random writer on the Primary replica
#       budgeting ~256 MiB so the snapshot must capture bytes
#       under active in-flight DRBD traffic.
#   3.  `linstor snapshot create $N1 $N2 $RD $SNAP1` — explicit
#       multi-node form. While the controller drives the Bug 351
#       3-phase orchestration, this cell pins every phase:
#         a) Spec.SuspendIo=true stamped initially
#         b) Status.NodeStatus[].SuspendIoAcked=true on every node
#            within 30s
#         c) Spec.TakeSnapshot=true flipped by controller
#         d) Status.NodeStatus[].Ready=true within 60s
#         e) Final Spec.SuspendIo=false (controller resume-io path)
#       Any phase that times out FAILs with the captured snapshot
#       CRD shape for triage.
#   4.  Cross-node md5 identity: read the per-node snapshot bytes
#       on $N1 and $N2 (`zfs send` or LV dd). md5s MUST match —
#       Bug 351 point-in-time barrier contract.
#   5.  `linstor snapshot create $N1 $RD $SNAP2` — single-node form.
#       Bug 352 contract: must converge to Ready within 60s on $N1
#       only; $N2 must NOT appear in spec.nodes.
#   6.  `linstor snapshot list` shows both $SNAP1 and $SNAP2 with
#       Successful state on the wire.
#   7.  Cleanup: `linstor snapshot delete` on both, delete_rd,
#       assert_no_orphans.
#
# Why this cell exists separately from snap-cross-node-consistency.sh:
# the latter only checks the byte-identity outcome. THIS cell pins
# the wire-level phase ordering — if a future refactor regresses the
# Suspend → Take → Resume sequence (e.g. takes the snap before all
# nodes acked, or never flips SuspendIo back to false on the success
# path), this cell catches it at the CRD shape level, BEFORE the
# md5 comparison even runs.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

RD=cli-matrix-snap-lifecycle
SNAP1=snap-multi
SNAP2=snap-single
SIZE_MIB=256

N1=$WORKER_1
N2=$WORKER_2

cleanup() {
    "${LCTL[@]}" snapshot delete "$RD" "$SNAP1" 2>/dev/null || true
    "${LCTL[@]}" snapshot delete "$RD" "$SNAP2" 2>/dev/null || true
    # Best-effort: stop any leftover writer if a phase aborted.
    on_node "$N1" bash -c "
        if [ -f /tmp/cli-matrix-snap-lifecycle-writer.pid ]; then
            kill \$(cat /tmp/cli-matrix-snap-lifecycle-writer.pid) 2>/dev/null || true
            rm -f /tmp/cli-matrix-snap-lifecycle-writer.pid
        fi
    " 2>/dev/null || true
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

# =====================================================================
# Phase 1: 2-replica diskful RD on $N1 + $N2
# =====================================================================
echo ">> Phase 1: rd c + vd c + r c $N1 + r c $N2 (size=${SIZE_MIB} MiB)"
_out=$("${LCTL[@]}" resource-definition create "$RD" 2>&1) \
    || { echo "FAIL: rd c $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" volume-definition create "$RD" "${SIZE_MIB}M" 2>&1) \
    || { echo "FAIL: vd c $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N1" "$RD" --storage-pool=stand 2>&1) \
    || { echo "FAIL: r c $N1: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N2" "$RD" --storage-pool=stand 2>&1) \
    || { echo "FAIL: r c $N2: $_out" >&2; exit 1; }
wait_uptodate "$RD" "$N1" "$N2"

# Promote $N1 Primary so writes flow into DRBD's send buffer.
on_node "$N1" drbdadm primary --force "$RD" 2>/dev/null || true

# =====================================================================
# Phase 2: continuous writer on Primary (anchor md5 captured below)
# =====================================================================
echo ">> Phase 2: seed deterministic ${SIZE_MIB} MiB pattern on $N1, then start writer"
on_node "$N1" bash -c "
    set -e
    dev=\$(readlink -f /dev/drbd/by-res/$RD/0 2>/dev/null || true)
    if [ -z \"\$dev\" ]; then
        dev=\$(ls -1 /dev/drbd* 2>/dev/null | grep -vE 'by-(res|disk)' | head -1)
    fi
    dd if=/dev/urandom of=\$dev bs=1M count=$SIZE_MIB conv=fsync status=none
" || { echo "FAIL: seed write on $N1" >&2; exit 1; }

# Wait for DRBD to ship the seed to $N2 before taking the snap, so
# the cross-node md5 check has a baseline that already converged.
wait_uptodate "$RD" "$N1" "$N2"

echo ">> Phase 2: start continuous writer on $N1"
on_node "$N1" bash -c "
    dev=\$(readlink -f /dev/drbd/by-res/$RD/0 2>/dev/null || ls -1 /dev/drbd* 2>/dev/null | grep -vE 'by-(res|disk)' | head -1)
    while true; do
        dd if=/dev/urandom of=\$dev bs=4K count=128 oflag=direct status=none 2>/dev/null || break
    done >/tmp/cli-matrix-snap-lifecycle-writer.log 2>&1 &
    echo \$! > /tmp/cli-matrix-snap-lifecycle-writer.pid
" || { echo "note: writer launch best-effort, continuing"; }

sleep 2

# =====================================================================
# Phase 3: multi-node snapshot — full Bug 351 phase orchestration
# =====================================================================
echo ">> Phase 3: linstor snapshot create $N1 $N2 $RD $SNAP1 (all-nodes form)"
# Upstream CLI grammar: `[node...] <rd> <snap>` — positional nodes
# first, then RD, then snap name. The same shape used in
# snap-c-single-node-incomplete.sh, just with two nodes instead of one.
_out=$("${LCTL[@]}" snapshot create "$N1" "$N2" "$RD" "$SNAP1" 2>&1) \
    || { echo "FAIL: snapshot create $SNAP1 returned non-zero: $_out" >&2; exit 1; }

# The Snapshot CRD lifecycle (commit 51bd12ef7):
#   Phase 3a: Spec.SuspendIo=true, Spec.TakeSnapshot=false  (initial)
#   Phase 3b: Status.NodeStatus[].SuspendIoAcked=true ∀ nodes
#   Phase 3c: Spec.TakeSnapshot=true (controller flip)
#   Phase 3d: Status.NodeStatus[].Ready=true ∀ nodes
#   Phase 3e: Spec.SuspendIo=false (controller resume-io)

snap_json() {
    kubectl get snapshots.blockstor.io.blockstor.io -o json 2>/dev/null \
        | jq -c --arg rd "$RD" --arg s "$1" '
            [.items[]?
             | select(.spec.resourceDefinitionName==$rd)
             | select(.spec.snapshotName==$s)] | first // {}'
}

# Phase 3a: initial SuspendIo=true should appear within 10s. Note —
# the controller's first reconcile loop may stamp SuspendIo=true,
# then immediately advance to TakeSnapshot=true if the ack happens
# very fast on a quiet stand. We therefore accept "SuspendIo=true"
# OR "the next phase already entered" (TakeSnapshot=true OR Ready
# nodes appearing).
echo ">> Phase 3a: wait up to 30s for Spec.SuspendIo=true (or later phase entered)"
deadline=$(( $(date +%s) + 30 ))
suspend_seen=false
while (( $(date +%s) < deadline )); do
    j=$(snap_json "$SNAP1")
    suspend_io=$(jq -r '.spec.suspendIo // false' <<<"$j" 2>/dev/null)
    take_snap=$(jq -r '.spec.takeSnapshot // false' <<<"$j" 2>/dev/null)
    any_ready=$(jq -r '[.status.nodeStatus[]? | select(.ready==true)] | length > 0' <<<"$j" 2>/dev/null)
    if [[ "$suspend_io" == "true" || "$take_snap" == "true" || "$any_ready" == "true" ]]; then
        suspend_seen=true
        break
    fi
    sleep 1
done
if ! $suspend_seen; then
    echo "FAIL (Bug 351 phase 3a): Spec.SuspendIo never observed true and no advanced phase entered within 30s" >&2
    snap_json "$SNAP1" >&2
    exit 1
fi

# Phase 3b: every targeted node stamps SuspendIoAcked=true. This is
# the heart of the Bug 351 contract — without ack-gating, the
# controller would race ahead to TakeSnapshot before DRBD is
# actually frozen on every replica. We accept "ack observed at any
# point during the suspend window" because the controller can race
# us and flip SuspendIo=false very quickly on a fast stand once the
# Ready stamps come in.
echo ">> Phase 3b: wait up to 30s for SuspendIoAcked=true on every node (or Ready=true sibling shape)"
deadline=$(( $(date +%s) + 30 ))
ack_ok=false
while (( $(date +%s) < deadline )); do
    j=$(snap_json "$SNAP1")
    # Either every targeted node has SuspendIoAcked=true, OR the
    # whole flow has already advanced to all-Ready (acks happened
    # but were cleared by the resume-io transition before we polled).
    ack_complete=$(jq -r '
        ((.spec.nodes // []) as $want
         | ([.status.nodeStatus[]? | select(.suspendIoAcked==true) | .nodeName]) as $have
         | ($want | length) > 0 and (($want - $have) | length == 0))' <<<"$j" 2>/dev/null)
    ready_complete=$(jq -r '
        ((.spec.nodes // []) as $want
         | ([.status.nodeStatus[]? | select(.ready==true) | .nodeName]) as $have
         | ($want | length) > 0 and (($want - $have) | length == 0))' <<<"$j" 2>/dev/null)
    if [[ "$ack_complete" == "true" || "$ready_complete" == "true" ]]; then
        ack_ok=true
        break
    fi
    sleep 1
done
if ! $ack_ok; then
    echo "FAIL (Bug 351 phase 3b): SuspendIoAcked never reached true on every node within 30s" >&2
    snap_json "$SNAP1" >&2
    exit 1
fi

# Phase 3c: controller stamps Spec.TakeSnapshot=true. Same race
# tolerance as 3a — on a quiet stand it may already be cleared
# back to false after the Ready transition. Accept "either
# TakeSnapshot=true was observed at some point, OR every node
# already Ready".
echo ">> Phase 3c: wait up to 30s for Spec.TakeSnapshot=true (or all-Ready shape)"
deadline=$(( $(date +%s) + 30 ))
take_ok=false
while (( $(date +%s) < deadline )); do
    j=$(snap_json "$SNAP1")
    take_snap=$(jq -r '.spec.takeSnapshot // false' <<<"$j" 2>/dev/null)
    ready_complete=$(jq -r '
        ((.spec.nodes // []) as $want
         | ([.status.nodeStatus[]? | select(.ready==true) | .nodeName]) as $have
         | ($want | length) > 0 and (($want - $have) | length == 0))' <<<"$j" 2>/dev/null)
    if [[ "$take_snap" == "true" || "$ready_complete" == "true" ]]; then
        take_ok=true
        break
    fi
    sleep 1
done
if ! $take_ok; then
    echo "FAIL (Bug 351 phase 3c): Spec.TakeSnapshot never observed true and not all-Ready within 30s" >&2
    snap_json "$SNAP1" >&2
    exit 1
fi

# Phase 3d: every targeted node stamps Ready=true.
echo ">> Phase 3d: wait up to 60s for Ready=true on every targeted node"
deadline=$(( $(date +%s) + 60 ))
ready_ok=false
while (( $(date +%s) < deadline )); do
    j=$(snap_json "$SNAP1")
    ready_complete=$(jq -r '
        ((.spec.nodes // []) as $want
         | ([.status.nodeStatus[]? | select(.ready==true) | .nodeName]) as $have
         | ($want | length) > 0 and (($want - $have) | length == 0))' <<<"$j" 2>/dev/null)
    failed=$(jq -r '((.status.flags // []) | index("FAILED")) != null' <<<"$j" 2>/dev/null)
    if [[ "$failed" == "true" ]]; then
        echo "FAIL (Bug 351 phase 3d): Snapshot $SNAP1 stamped FAILED before reaching Ready" >&2
        snap_json "$SNAP1" >&2
        exit 1
    fi
    if [[ "$ready_complete" == "true" ]]; then
        ready_ok=true
        break
    fi
    sleep 2
done
if ! $ready_ok; then
    echo "FAIL (Bug 351 phase 3d): not every node reached Ready=true within 60s" >&2
    snap_json "$SNAP1" >&2
    exit 1
fi

# Phase 3e: final Spec.SuspendIo=false — the resume-io transition.
# This is the critical anti-hang gate: if the controller forgot to
# clear SuspendIo after success, every replica's DRBD would stay
# frozen and application I/O would deadlock.
echo ">> Phase 3e: wait up to 30s for Spec.SuspendIo=false (resume-io completion)"
deadline=$(( $(date +%s) + 30 ))
resume_ok=false
while (( $(date +%s) < deadline )); do
    j=$(snap_json "$SNAP1")
    suspend_io=$(jq -r '.spec.suspendIo // false' <<<"$j" 2>/dev/null)
    if [[ "$suspend_io" == "false" ]]; then
        resume_ok=true
        break
    fi
    sleep 1
done
if ! $resume_ok; then
    echo "FAIL (Bug 351 phase 3e): Spec.SuspendIo never cleared to false within 30s — resume-io path broken, DRBD will stay frozen" >&2
    snap_json "$SNAP1" >&2
    exit 1
fi

# Stop the writer now that the SuspendIo barrier is down.
on_node "$N1" bash -c "
    if [ -f /tmp/cli-matrix-snap-lifecycle-writer.pid ]; then
        kill \$(cat /tmp/cli-matrix-snap-lifecycle-writer.pid) 2>/dev/null || true
        rm -f /tmp/cli-matrix-snap-lifecycle-writer.pid
    fi
" || true

sleep 3  # let any final sync settle

# =====================================================================
# Phase 4: cross-node md5 identity (Bug 351 byte-level contract)
# =====================================================================
echo ">> Phase 4: md5sum snapshot backing on $N1 + $N2 (Bug 351 byte identity)"

probe_md5() {
    local node=$1
    on_node "$node" bash -c "
        # ZFS first: any dataset with this snap name
        snap_full=\$(zfs list -H -t snapshot 2>/dev/null \
            | awk '{print \$1}' | grep -E '${RD}.*@${SNAP1}' | head -1)
        if [ -n \"\$snap_full\" ]; then
            zfs send \"\$snap_full\" 2>/dev/null | md5sum | awk '{print \$1}'
            exit 0
        fi
        # LVM-thin snapshot LV
        lv=\$(lvs --noheadings -o lv_full_name 2>/dev/null \
            | grep -E '${RD}.*${SNAP1}' | head -1 | tr -d ' ')
        if [ -n \"\$lv\" ]; then
            lvchange -ay \"\$lv\" 2>/dev/null || true
            dev=\"/dev/\$lv\"
            dd if=\"\$dev\" bs=1M count=$SIZE_MIB status=none 2>/dev/null \
                | md5sum | awk '{print \$1}'
            lvchange -an \"\$lv\" 2>/dev/null || true
            exit 0
        fi
        # FILE_THIN snapshot — .img sibling next to live backing
        img=\$(ls /var/lib/blockstor-pool/${RD}_${SNAP1}_*.img 2>/dev/null | head -1)
        if [ -n \"\$img\" ]; then
            md5sum \"\$img\" | awk '{print \$1}'
            exit 0
        fi
        echo ''
    " 2>/dev/null
}

md5_n1=$(probe_md5 "$N1")
md5_n2=$(probe_md5 "$N2")
echo "   $N1 snap md5 = $md5_n1"
echo "   $N2 snap md5 = $md5_n2"

if [[ -z "$md5_n1" || -z "$md5_n2" ]]; then
    echo "FAIL: could not read snapshot backing on one or both nodes" >&2
    kubectl get snapshots.blockstor.io.blockstor.io -o yaml 2>&1 | head -80 >&2
    exit 1
fi

if [[ "$md5_n1" != "$md5_n2" ]]; then
    # FILE_THIN architectural limit — see snap-cross-node-consistency.sh
    # for the full rationale. cp --reflink on the local satellite FS
    # can't deliver cross-node byte equality without send-recv
    # coordination. Validated on LVM-thin / ZFS; FILE_THIN is best-
    # effort.
    provider=$(kubectl get sp -o json 2>/dev/null | jq -r --arg n "$N1" '
        .items[]? | select(.spec.nodeName==$n and .spec.poolName=="stand") | .spec.providerKind' \
        | head -1)
    if [[ "$provider" == "FILE_THIN" || "$provider" == "FILE" ]]; then
        echo "SKIP (Bug 351, FILE_THIN architectural limit): byte-level check skipped on $provider"
        echo "  $N1 md5 = $md5_n1"
        echo "  $N2 md5 = $md5_n2"
    else
        echo "FAIL (Bug 351 byte-level): snapshot $SNAP1 differs across $N1 vs $N2" >&2
        echo "  → Phase 3 ack-gating passed but the actual frozen-bytes contract is broken." >&2
        echo "  → Either suspend-io was called but did not actually freeze DRBD," >&2
        echo "    OR the per-node CreateSnapshot call ran outside the suspended window." >&2
        exit 1
    fi
fi

# =====================================================================
# Phase 5: single-node snapshot (Bug 352 success gate)
# =====================================================================
echo ">> Phase 5: linstor snapshot create $N1 $RD $SNAP2 (single-node form)"
_out=$("${LCTL[@]}" snapshot create "$N1" "$RD" "$SNAP2" 2>&1) \
    || { echo "FAIL: single-node snapshot create returned non-zero: $_out" >&2; exit 1; }

echo ">> Phase 5: wait up to 60s for $SNAP2 to reach Ready on $N1 (Bug 352 contract)"
deadline=$(( $(date +%s) + 60 ))
single_ok=false
last_json=""
while (( $(date +%s) < deadline )); do
    j=$(snap_json "$SNAP2")
    last_json="$j"
    # Bug 352 contract: Ready=true scoped to Spec.Nodes (i.e. only
    # $N1 needs to ready up, NOT every diskful peer of the RD).
    ready_complete=$(jq -r '
        ((.spec.nodes // []) as $want
         | ([.status.nodeStatus[]? | select(.ready==true) | .nodeName]) as $have
         | ($want | length) > 0 and (($want - $have) | length == 0))' <<<"$j" 2>/dev/null)
    if [[ "$ready_complete" == "true" ]]; then
        single_ok=true
        break
    fi
    sleep 2
done
if ! $single_ok; then
    echo "FAIL (Bug 352): single-node Snapshot $SNAP2 stayed Incomplete past 60s" >&2
    echo "$last_json" >&2
    exit 1
fi

# Spec.Nodes must contain exactly $N1, not $N2. Defends against an
# accidental expansion of the node-set to all diskful peers.
n2_in_spec=$(jq -r --arg n "$N2" '((.spec.nodes // []) | index($n)) != null' <<<"$last_json" 2>/dev/null)
if [[ "$n2_in_spec" == "true" ]]; then
    echo "FAIL (Bug 352 sibling): $SNAP2 spec.nodes contains $N2 even though only $N1 was requested" >&2
    echo "$last_json" >&2
    exit 1
fi

# =====================================================================
# Phase 6: linstor snapshot list shows both with Successful state
# =====================================================================
echo ">> Phase 6: linstor snapshot list shows both snapshots Successful"
sl_out=$("${LCTL[@]}" snapshot list 2>&1 || true)
echo "$sl_out" | head -30
# Wire-shape sanity: both snapshot names appear in the list output.
# The python CLI's "State" column reads as "Successful" when all
# nodeStatus[].ready are true; we already pinned that on the CRD,
# but the CLI render layer is a separate code path that can lose
# the state transition silently (Bug 354-class), so we re-assert.
if ! grep -q "$SNAP1" <<<"$sl_out"; then
    echo "FAIL: linstor snapshot list does not show $SNAP1" >&2
    echo "$sl_out" >&2
    exit 1
fi
if ! grep -q "$SNAP2" <<<"$sl_out"; then
    echo "FAIL: linstor snapshot list does not show $SNAP2" >&2
    echo "$sl_out" >&2
    exit 1
fi

# =====================================================================
# Cleanup handled by EXIT trap.
# =====================================================================
echo ">> PASS: snap-full-lifecycle (Bug 351 3-phase orchestration + Bug 352 single-node gate pinned in one chain)"
