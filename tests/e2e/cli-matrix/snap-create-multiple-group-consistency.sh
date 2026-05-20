#!/usr/bin/env bash
#
# usage: snap-create-multiple-group-consistency.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 353.
#
# User question 2026-05-19: how does `linstor s create-multiple`
# work? Verified by reading both sources:
#
# Upstream (Apache 2.0 / GPL — read-only research, no code copy)
# /v1/actions/snapshot/multi handler at
# controller/.../api/rest/v1/Actions.java:62-95 builds a
# CreateMultiSnapRequest with N SnapReq entries and calls the
# SAME snapCrtHandler.createSnapshot orchestrator that single-
# snapshot uses. That orchestrator (CtrlSnapshotCrtApiCallHandler
# .java:480-540) does a single 3-step flow over the UNION of all
# entries' Resources:
#   1. setSuspendIo(true) → updateSatellites for every Resource
#      of every RD in the batch → wait for ack from all peers.
#   2. takeSnapshot() — per-node provider.CreateSnapshot for
#      EACH entry, while DRBD on EVERY listed peer is suspended.
#   3. resumeIoPrivileged() — drbdsetup resume-io broadcast.
# Result: every snapshot in the batch captures bytes at the
# SAME wall-clock instant across ALL participating replicas.
# This is the "consistency group snapshot" contract DB
# operators rely on for restore (data + WAL volumes on separate
# RDs snapped at the same DRBD-frozen point).
#
# blockstor (pkg/rest/snapshot_multi.go:63-83): the handler
# iterates entries and calls createOneFromMulti per entry. Each
# call goes through the per-RD Store.Snapshots().Create path.
# NO cross-RD coordination, NO suspend-io (Bug 351 isn't wired
# at all), NO atomic batch. The N snapshots are taken at N
# distinct wall-clock instants, divergent by however long each
# Store.Create call takes (+ satellite reconcile lag). Concurrent
# writes between entries 1 and N produce divergent per-RD data.
#
# Failure mode in production: DB volume snapped at T=0, WAL
# volume snapped at T=+200ms with new commits → restore replays
# WAL referencing pages the data snapshot never captured →
# checksum failure / phantom row.
#
# Test contract:
#   1. Build 2 RDs (rd-a, rd-b), each 2-replica diskful on
#      worker-1 + worker-2. They model "data" + "wal" volumes.
#   2. Start a cross-RD correlated writer: write the SAME
#      monotonically-increasing 64-bit counter into the first 8
#      bytes of BOTH RDs in tight loop. This is the cheapest
#      reproducer of "DB writes data + WAL atomically; restore
#      requires both to match".
#   3. `linstor snapshot create-multiple rd-a:snap1 rd-b:snap1`
#      (via the multi endpoint).
#   4. Stop the writer, wait sync.
#   5. Read the first 8 bytes from EACH snapshot's backing.
#   6. The two snapshots' counters MUST be equal (or within ±1
#      because the writer could race the suspend with one write
#      ahead). If they differ by > 1, no point-in-time barrier
#      was applied — restore from this pair is broken.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup
trap linstor_cli_teardown EXIT

RD_A=cli-matrix-multi-snap-a
RD_B=cli-matrix-multi-snap-b
SNAP=snap1

N1=$WORKER_1
N2=$WORKER_2

cleanup() {
    "${LCTL[@]}" snapshot delete "$RD_A" "$SNAP" 2>/dev/null || true
    "${LCTL[@]}" snapshot delete "$RD_B" "$SNAP" 2>/dev/null || true
    delete_rd "$RD_A"
    delete_rd "$RD_B"
    assert_no_orphans "$RD_A"
    assert_no_orphans "$RD_B"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> create 2 RDs (data + WAL model) on $N1 + $N2"
for RD in "$RD_A" "$RD_B"; do
    _out=$("${LCTL[@]}" resource-definition create "$RD" 2>&1) \
        || { echo "FAIL: rd c $RD: $_out" >&2; exit 1; }
    _out=$("${LCTL[@]}" volume-definition create "$RD" 64M 2>&1) \
        || { echo "FAIL: vd c $RD: $_out" >&2; exit 1; }
    _out=$("${LCTL[@]}" resource create "$N1" "$RD" --storage-pool=stand 2>&1) \
        || { echo "FAIL: r c $N1 $RD: $_out" >&2; exit 1; }
    _out=$("${LCTL[@]}" resource create "$N2" "$RD" --storage-pool=stand 2>&1) \
        || { echo "FAIL: r c $N2 $RD: $_out" >&2; exit 1; }
    wait_uptodate "$RD" "$N1" "$N2"
done

# Promote $N1 Primary on both RDs so writes flow.
on_node "$N1" drbdadm primary --force "$RD_A" 2>/dev/null || true
on_node "$N1" drbdadm primary --force "$RD_B" 2>/dev/null || true

# ---- Cross-RD correlated writer ------------------------------------------
#
# Tight loop: write the same monotonically-increasing 64-bit
# counter into the first 8 bytes of both DRBD devices. Without a
# group suspend-io barrier, snap-A and snap-B will capture
# different counter values whenever the writer made progress
# between the two per-RD snap calls.
echo ">> start cross-RD correlated writer on $N1 (counter into rd-a + rd-b)"
on_node "$N1" bash -c "
    dev_a=\$(readlink -f /dev/drbd/by-res/$RD_A/0 2>/dev/null || true)
    dev_b=\$(readlink -f /dev/drbd/by-res/$RD_B/0 2>/dev/null || true)
    if [ -z \"\$dev_a\" ] || [ -z \"\$dev_b\" ]; then
        echo 'note: could not resolve drbd device paths'
        exit 0
    fi
    n=0
    while true; do
        n=\$((n+1))
        printf '%016x' \"\$n\" | xxd -r -p | dd of=\"\$dev_a\" bs=8 count=1 conv=fsync oflag=direct status=none 2>/dev/null || break
        printf '%016x' \"\$n\" | xxd -r -p | dd of=\"\$dev_b\" bs=8 count=1 conv=fsync oflag=direct status=none 2>/dev/null || break
    done >/tmp/cli-matrix-multi-writer.log 2>&1 &
    echo \$! > /tmp/cli-matrix-multi-writer.pid
" || true

# Let the writer accumulate state.
sleep 2

# ---- Multi-snapshot create -----------------------------------------------
#
# The CLI form: `linstor snapshot create-multiple
# <rd1>:<snap1> <rd2>:<snap2>`. Some versions also accept
# `--multi` on a per-rd call list; we use the colon form first
# and fall back via direct REST POST if the CLI rejects it.
echo ">> [Bug 353 trigger] linstor snapshot create-multiple $RD_A:$SNAP $RD_B:$SNAP"
if ! _out=$("${LCTL[@]}" snapshot create-multiple "${RD_A}:${SNAP}" "${RD_B}:${SNAP}" 2>&1); then
    echo "note: 'create-multiple' CLI form rejected (out=$_out); falling back to direct POST /v1/actions/snapshot/multi"
    body=$(cat <<EOF
{"snapshots":[
  {"resource_name":"${RD_A}","name":"${SNAP}"},
  {"resource_name":"${RD_B}","name":"${SNAP}"}
]}
EOF
)
    if ! _out=$(curl -fsS -X POST "http://127.0.0.1:${LCTL_PORT}/v1/actions/snapshot/multi" \
        -H 'Content-Type: application/json' -d "$body" 2>&1); then
        echo "FAIL: multi-snapshot POST failed: $_out" >&2
        exit 1
    fi
fi

# Wait both snapshots Successful.
echo ">> wait up to 90s for both snapshots Successful"
deadline=$(( $(date +%s) + 90 ))
all_ok=false
while (( $(date +%s) < deadline )); do
    # Why: CRD shape uses status.nodeStatus[].ready per spec.nodes[],
    # not legacy status.successful. The all-of-empty false-PASS guard.
    JQ_OK='
        [.items[]?
         | select(.spec.resourceDefinitionName==$rd)
         | select(.spec.snapshotName==$s)
         | ((.spec.nodes // []) as $want
            | ([.status.nodeStatus[]? | select(.ready==true) | .nodeName]) as $have
            | ($want | length) > 0 and (($want - $have) | length == 0))
        ] | length > 0 and all'
    snap_a_ok=$(kubectl get snapshots.blockstor.io.blockstor.io -o json 2>/dev/null \
        | jq -r --arg rd "$RD_A" --arg s "$SNAP" "$JQ_OK" 2>/dev/null || echo "false")
    snap_b_ok=$(kubectl get snapshots.blockstor.io.blockstor.io -o json 2>/dev/null \
        | jq -r --arg rd "$RD_B" --arg s "$SNAP" "$JQ_OK" 2>/dev/null || echo "false")
    if [[ "$snap_a_ok" == "true" && "$snap_b_ok" == "true" ]]; then
        all_ok=true
        break
    fi
    sleep 2
done

# Stop the writer.
on_node "$N1" bash -c "
    if [ -f /tmp/cli-matrix-multi-writer.pid ]; then
        kill \$(cat /tmp/cli-matrix-multi-writer.pid) 2>/dev/null || true
        rm -f /tmp/cli-matrix-multi-writer.pid
    fi
" || true

if ! $all_ok; then
    echo "FAIL (Bug 353): multi-snapshot did not converge to Successful on both RDs in 90s" >&2
    kubectl get snapshots.blockstor.io.blockstor.io 2>&1 | head -30 >&2
    exit 1
fi

sleep 3

# ---- Read the first 8 bytes (counter) from each snapshot ----------------
read_counter() {
    local node=$1 rd=$2
    on_node "$node" bash -c "
        # ZFS
        snap_full=\$(zfs list -H -t snapshot 2>/dev/null \
            | awk '{print \$1}' | grep -E '${rd}.*@${SNAP}' | head -1)
        if [ -n \"\$snap_full\" ]; then
            # Clone to ephemeral RW dataset to read first 8 bytes
            tmp_clone=\"\${snap_full%@*}/cli-matrix-counter-read-${rd}\"
            zfs clone \"\$snap_full\" \"\$tmp_clone\" 2>/dev/null && \
            head -c 8 \"/dev/zvol/\$tmp_clone\" 2>/dev/null | xxd -p
            zfs destroy \"\$tmp_clone\" 2>/dev/null
            exit 0
        fi
        # LVM-thin snapshot
        lv=\$(lvs --noheadings -o lv_full_name 2>/dev/null \
            | grep -E '${rd}.*${SNAP}' | head -1 | tr -d ' ')
        if [ -n \"\$lv\" ]; then
            lvchange -ay \"\$lv\" 2>/dev/null || true
            head -c 8 \"/dev/\$lv\" 2>/dev/null | xxd -p
            lvchange -an \"\$lv\" 2>/dev/null || true
            exit 0
        fi
        # FILE_THIN snapshot — .img sibling at /var/lib/blockstor-pool
        img=\$(ls /var/lib/blockstor-pool/${rd}_${SNAP}_*.img 2>/dev/null | head -1)
        if [ -n \"\$img\" ]; then
            head -c 8 \"\$img\" 2>/dev/null | xxd -p
            exit 0
        fi
        echo ''
    " 2>/dev/null
}

cnt_a=$(read_counter "$N1" "$RD_A")
cnt_b=$(read_counter "$N1" "$RD_B")
echo "   snap_${RD_A}.counter = $cnt_a"
echo "   snap_${RD_B}.counter = $cnt_b"

if [[ -z "$cnt_a" || -z "$cnt_b" ]]; then
    echo "FAIL: could not read counter byte from one or both snapshots" >&2
    exit 1
fi

# Convert to decimal for ±1 comparison.
dec_a=$((16#${cnt_a}))
dec_b=$((16#${cnt_b}))
diff=$(( dec_a - dec_b ))
if (( diff < 0 )); then diff=$(( -diff )); fi

# Allow ±1 because the writer might land one extra write between
# the suspend on RD_A and the suspend on RD_B even with a proper
# orchestrator (the suspend-io broadcast is itself sequential
# across kernel calls but happens within microseconds — bounded
# slip). Anything >1 means no barrier was applied.
if (( diff > 1 )); then
    echo "FAIL (Bug 353): snap_${RD_A}.counter=${dec_a} vs snap_${RD_B}.counter=${dec_b} differ by ${diff} > 1" >&2
    echo "  → blockstor's /v1/actions/snapshot/multi loop is NOT atomic across RDs." >&2
    echo "  → Upstream's CtrlSnapshotCrtApiCallHandler runs a single suspend-io broadcast" >&2
    echo "    across the union of all batched RDs' Resources, then takes all snaps." >&2
    echo "  → Consistency-group restore (data + WAL) is broken under this implementation." >&2
    exit 1
fi

echo ">> snap-create-multiple-group-consistency OK (Bug 353 pinned: cross-RD snapshots within ±1 counter)"
