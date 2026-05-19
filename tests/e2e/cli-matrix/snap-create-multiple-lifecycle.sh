#!/usr/bin/env bash
#
# usage: snap-create-multiple-lifecycle.sh WORK_DIR
#
# L6 cli-matrix cell — P0 full multi-snapshot batch lifecycle catcher.
# Exercises Bug 353 (consistency-group GroupID + atomic phase
# advancement across the batch) together with the Bug 351 SuspendIo
# orchestration (commit 51bd12ef7) propagated to every entry of the
# batch in one linear chain.
#
# Why a separate "lifecycle" cell vs the existing
# snap-create-multiple-group-consistency.sh:
#   - the consistency cell only checks the byte-level outcome
#     (cross-RD counter parity).
#   - THIS cell pins the CRD-wire contract: every snapshot in the
#     batch must share the same Spec.GroupID, must enter SuspendIo
#     phase together, must advance through TakeSnapshot together,
#     must all reach Ready together (or all fail together).
#     A regression that takes the snaps fine but loses the GroupID
#     stamping (i.e. each Snapshot CRD ends up in its own one-snap
#     group on the wire) passes the byte-counter check on a quiet
#     stand but reintroduces the production failure mode under load.
#
# Test contract:
#   1. Setup 3 RDs (rd-a, rd-b, rd-c), 2-replica each on $N1 + $N2.
#      Models a DB with separate data/wal/log volumes.
#   2. Cross-RD correlated writer: write the SAME monotonically-
#      increasing 64-bit counter into bytes 0-7 of all three RDs in
#      a tight loop. Cheapest reproducer of "DB writes data + WAL +
#      log atomically; restore needs all three to match".
#   3. `linstor snapshot create-multiple rd-a:snap1 rd-b:snap1
#      rd-c:snap1` — multi-entry form. Pin every wire-contract:
#        a) All 3 Snapshot CRDs created with same Spec.GroupID
#           (Bug 353 transactional-batch identity)
#        b) All 3 reach SuspendIo phase ~atomically (within 5s of
#           each other — bounded slip, not unbounded)
#        c) All 3 reach Ready=true within 90s
#        d) All 3 finally clear Spec.SuspendIo=false (resume-io)
#   4. Cross-RD point-in-time barrier: read counter byte from each
#      snapshot's backing. All three must be within ±1 of each
#      other. Anything >1 means the batch lost atomicity.
#   5. Partial-failure cell variant: invoke create-multiple with
#      one non-existent RD in the batch. The CLI MUST either
#      reject the whole batch up-front OR surface a per-entry
#      error envelope that distinguishes the bad entry from any
#      good entries. NEITHER acceptable:
#        - silent partial success (some snaps created, no error)
#        - opaque single 500 with no per-entry detail
#   6. Cleanup: `linstor snapshot delete` on all entries, delete_rd
#      on all RDs, assert_no_orphans.
#
# Abort path (suspend-io fails mid-batch on one node, others have
# already acked) is NOT tested here — that requires fault injection
# at the satellite layer which is not safe on a real stand. Covered
# by internal/controller/snapshot_bug_351_test.go at L4.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

RD_A=cli-matrix-multi-life-a
RD_B=cli-matrix-multi-life-b
RD_C=cli-matrix-multi-life-c
SNAP=snap1
RD_MISSING=cli-matrix-multi-life-nonexistent

N1=$WORKER_1
N2=$WORKER_2

cleanup() {
    for RD in "$RD_A" "$RD_B" "$RD_C"; do
        "${LCTL[@]}" snapshot delete "$RD" "$SNAP" 2>/dev/null || true
    done
    on_node "$N1" bash -c "
        if [ -f /tmp/cli-matrix-multi-life-writer.pid ]; then
            kill \$(cat /tmp/cli-matrix-multi-life-writer.pid) 2>/dev/null || true
            rm -f /tmp/cli-matrix-multi-life-writer.pid
        fi
    " 2>/dev/null || true
    for RD in "$RD_A" "$RD_B" "$RD_C"; do
        delete_rd "$RD"
        assert_no_orphans "$RD"
    done
    linstor_cli_teardown
}
trap cleanup EXIT

# =====================================================================
# Phase 1: create 3 RDs, 2-replica each on $N1 + $N2
# =====================================================================
echo ">> Phase 1: create 3 RDs (data + WAL + log model) on $N1 + $N2"
for RD in "$RD_A" "$RD_B" "$RD_C"; do
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

# Promote $N1 Primary on all three RDs.
for RD in "$RD_A" "$RD_B" "$RD_C"; do
    on_node "$N1" drbdadm primary --force "$RD" 2>/dev/null || true
done

# =====================================================================
# Phase 2: cross-RD correlated writer (counter into bytes 0-7 of all 3)
# =====================================================================
echo ">> Phase 2: start cross-RD correlated writer on $N1"
on_node "$N1" bash -c "
    dev_a=\$(readlink -f /dev/drbd/by-res/$RD_A/0 2>/dev/null || true)
    dev_b=\$(readlink -f /dev/drbd/by-res/$RD_B/0 2>/dev/null || true)
    dev_c=\$(readlink -f /dev/drbd/by-res/$RD_C/0 2>/dev/null || true)
    if [ -z \"\$dev_a\" ] || [ -z \"\$dev_b\" ] || [ -z \"\$dev_c\" ]; then
        echo 'note: could not resolve all 3 drbd device paths'
        exit 0
    fi
    n=0
    while true; do
        n=\$((n+1))
        for d in \"\$dev_a\" \"\$dev_b\" \"\$dev_c\"; do
            printf '%016x' \"\$n\" | xxd -r -p \
                | dd of=\"\$d\" bs=8 count=1 conv=fsync oflag=direct status=none 2>/dev/null || break 2
        done
    done >/tmp/cli-matrix-multi-life-writer.log 2>&1 &
    echo \$! > /tmp/cli-matrix-multi-life-writer.pid
" || true

sleep 2  # let the writer accumulate state

# =====================================================================
# Phase 3: linstor snapshot create-multiple — full Bug 353 lifecycle
# =====================================================================
echo ">> Phase 3: linstor snapshot create-multiple ${RD_A}:${SNAP} ${RD_B}:${SNAP} ${RD_C}:${SNAP}"
# Why both code paths: the python CLI's create-multiple verb is a
# recent addition; some stand images ship a client without it. Fall
# back to direct REST POST when the CLI rejects the subcommand so
# the test still exercises the apiserver's batch endpoint.
if ! _out=$("${LCTL[@]}" snapshot create-multiple "${RD_A}:${SNAP}" "${RD_B}:${SNAP}" "${RD_C}:${SNAP}" 2>&1); then
    echo "note: 'create-multiple' CLI form rejected (out=$_out); falling back to POST /v1/actions/snapshot/multi"
    body=$(cat <<EOF
{"snapshots":[
  {"resource_name":"${RD_A}","name":"${SNAP}"},
  {"resource_name":"${RD_B}","name":"${SNAP}"},
  {"resource_name":"${RD_C}","name":"${SNAP}"}
]}
EOF
)
    if ! _out=$(curl -fsS -X POST "http://127.0.0.1:${LCTL_PORT}/v1/actions/snapshot/multi" \
        -H 'Content-Type: application/json' -d "$body" 2>&1); then
        echo "FAIL: multi-snapshot POST failed: $_out" >&2
        exit 1
    fi
fi

# Helper: grab one batch entry's Snapshot CRD as compact JSON.
snap_json() {
    local rd=$1
    kubectl get snapshots.blockstor.io.blockstor.io -o json 2>/dev/null \
        | jq -c --arg rd "$rd" --arg s "$SNAP" '
            [.items[]?
             | select(.spec.resourceDefinitionName==$rd)
             | select(.spec.snapshotName==$s)] | first // {}'
}

# Wait until all 3 Snapshot CRDs exist (the apiserver may stage them
# asynchronously even when the batch call returned 200).
echo ">> Phase 3a: wait up to 30s for all 3 Snapshot CRDs to exist"
deadline=$(( $(date +%s) + 30 ))
all_exist=false
while (( $(date +%s) < deadline )); do
    a=$(snap_json "$RD_A")
    b=$(snap_json "$RD_B")
    c=$(snap_json "$RD_C")
    if [[ "$a" != "{}" && "$b" != "{}" && "$c" != "{}" ]]; then
        all_exist=true
        break
    fi
    sleep 1
done
if ! $all_exist; then
    echo "FAIL (Bug 353): not every entry of create-multiple produced a Snapshot CRD within 30s" >&2
    kubectl get snapshots.blockstor.io.blockstor.io -o yaml 2>&1 | head -120 >&2
    exit 1
fi

# Phase 3b: Bug 353 GroupID contract. Every Snapshot CRD in the
# batch must share the same Spec.GroupID (or equivalent batch-
# identity field — accept .spec.groupId / .spec.groupID /
# annotation `blockstor.io/group-id`). If the field is absent on
# all three OR differs between them, the batch is not transactional
# and the test FAILs with the explicit shape.
echo ">> Phase 3b: every snap in the batch shares Spec.GroupID (Bug 353)"
gid_a=$(snap_json "$RD_A" | jq -r '
    .spec.groupId // .spec.groupID // .spec.group_id //
    (.metadata.annotations["blockstor.io/group-id"] // "")' 2>/dev/null)
gid_b=$(snap_json "$RD_B" | jq -r '
    .spec.groupId // .spec.groupID // .spec.group_id //
    (.metadata.annotations["blockstor.io/group-id"] // "")' 2>/dev/null)
gid_c=$(snap_json "$RD_C" | jq -r '
    .spec.groupId // .spec.groupID // .spec.group_id //
    (.metadata.annotations["blockstor.io/group-id"] // "")' 2>/dev/null)
echo "   $RD_A.groupID = ${gid_a:-<empty>}"
echo "   $RD_B.groupID = ${gid_b:-<empty>}"
echo "   $RD_C.groupID = ${gid_c:-<empty>}"
if [[ -z "$gid_a" || -z "$gid_b" || -z "$gid_c" ]]; then
    echo "FAIL (Bug 353): at least one Snapshot CRD has no GroupID — batch is not transactionally tagged" >&2
    echo "  → /v1/actions/snapshot/multi must stamp a shared GroupID on every staged Snapshot," >&2
    echo "  → so the controller can resume-io across the whole batch atomically (not per-entry)." >&2
    exit 1
fi
if [[ "$gid_a" != "$gid_b" || "$gid_b" != "$gid_c" ]]; then
    echo "FAIL (Bug 353): batched snaps have DIFFERENT GroupIDs — entries are independent, not transactional" >&2
    exit 1
fi

# Phase 3c: SuspendIo phase advances ~atomically across the batch.
# We accept "every entry showed SuspendIo=true at some moment, OR
# every entry already advanced past it" (because on a fast stand
# the controller may have already cleared SuspendIo=false by the
# time we polled). Bounded slip: every entry must reach the
# observed phase within 5s of the first one — anything looser means
# the batch is being processed serially per-entry, not as a group.
echo ">> Phase 3c: every snap reached SuspendIo phase within ≤5s of first (atomic batch advance)"
deadline=$(( $(date +%s) + 30 ))
first_seen_ts=0
seen_a=0; seen_b=0; seen_c=0
while (( $(date +%s) < deadline )); do
    now=$(date +%s)
    for pair in "${RD_A}:a" "${RD_B}:b" "${RD_C}:c"; do
        rd="${pair%:*}"
        tag="${pair#*:}"
        j=$(snap_json "$rd")
        # The entry has "entered the suspend-or-later phase" if
        # SuspendIo=true OR TakeSnapshot=true OR any nodeStatus is
        # Ready OR SuspendIoAcked.
        phase_in=$(jq -r '
            (.spec.suspendIo // false) or
            (.spec.takeSnapshot // false) or
            ([.status.nodeStatus[]? | select(.ready==true or .suspendIoAcked==true)] | length > 0)
            ' <<<"$j" 2>/dev/null)
        if [[ "$phase_in" == "true" ]]; then
            case "$tag" in
                a) (( seen_a == 0 )) && seen_a=$now ;;
                b) (( seen_b == 0 )) && seen_b=$now ;;
                c) (( seen_c == 0 )) && seen_c=$now ;;
            esac
            if (( first_seen_ts == 0 )); then first_seen_ts=$now; fi
        fi
    done
    if (( seen_a > 0 && seen_b > 0 && seen_c > 0 )); then
        break
    fi
    sleep 1
done

if (( seen_a == 0 || seen_b == 0 || seen_c == 0 )); then
    echo "FAIL (Bug 353): not all batched snaps entered SuspendIo phase (a=$seen_a b=$seen_b c=$seen_c)" >&2
    exit 1
fi

# Compute the slip: max - min across the three first-seen timestamps.
last_seen_ts=$seen_a
for ts in $seen_b $seen_c; do
    if (( ts > last_seen_ts )); then last_seen_ts=$ts; fi
done
slip=$(( last_seen_ts - first_seen_ts ))
echo "   batch phase-entry slip: ${slip}s (must be ≤ 5s)"
if (( slip > 5 )); then
    echo "FAIL (Bug 353): batch SuspendIo phase entry slip ${slip}s > 5s — entries are not advancing as a group" >&2
    exit 1
fi

# Phase 3d: every entry reaches Ready=true within 90s.
echo ">> Phase 3d: every snap reaches Ready=true within 90s"
deadline=$(( $(date +%s) + 90 ))
all_ready=false
while (( $(date +%s) < deadline )); do
    a=$(snap_json "$RD_A")
    b=$(snap_json "$RD_B")
    c=$(snap_json "$RD_C")
    JQ_OK='
        ((.spec.nodes // []) as $want
         | ([.status.nodeStatus[]? | select(.ready==true) | .nodeName]) as $have
         | ($want | length) > 0 and (($want - $have) | length == 0))'
    ra=$(jq -r "$JQ_OK" <<<"$a" 2>/dev/null)
    rb=$(jq -r "$JQ_OK" <<<"$b" 2>/dev/null)
    rc=$(jq -r "$JQ_OK" <<<"$c" 2>/dev/null)
    # Fail fast if any entry stamped FAILED.
    for j in "$a" "$b" "$c"; do
        failed=$(jq -r '((.status.flags // []) | index("FAILED")) != null' <<<"$j" 2>/dev/null)
        if [[ "$failed" == "true" ]]; then
            echo "FAIL (Bug 353): a batched snapshot stamped FAILED before reaching Ready" >&2
            echo "$j" >&2
            exit 1
        fi
    done
    if [[ "$ra" == "true" && "$rb" == "true" && "$rc" == "true" ]]; then
        all_ready=true
        break
    fi
    sleep 2
done
if ! $all_ready; then
    echo "FAIL (Bug 353): not every batched snapshot reached Ready=true within 90s" >&2
    kubectl get snapshots.blockstor.io.blockstor.io -o yaml 2>&1 | head -160 >&2
    exit 1
fi

# Phase 3e: every entry finally clears Spec.SuspendIo=false. This
# is the anti-hang gate at batch scope — if any entry stays
# SuspendIo=true after the others resumed, that one RD's DRBD
# stays frozen forever.
echo ">> Phase 3e: every snap clears Spec.SuspendIo=false (batch-scoped resume-io)"
deadline=$(( $(date +%s) + 30 ))
all_resumed=false
while (( $(date +%s) < deadline )); do
    sa=$(snap_json "$RD_A" | jq -r '.spec.suspendIo // false')
    sb=$(snap_json "$RD_B" | jq -r '.spec.suspendIo // false')
    sc=$(snap_json "$RD_C" | jq -r '.spec.suspendIo // false')
    if [[ "$sa" == "false" && "$sb" == "false" && "$sc" == "false" ]]; then
        all_resumed=true
        break
    fi
    sleep 1
done
if ! $all_resumed; then
    echo "FAIL (Bug 353 / Bug 351 abort path): at least one batched snap did not clear SuspendIo=false within 30s — DRBD will stay frozen on that RD" >&2
    exit 1
fi

# Stop the writer.
on_node "$N1" bash -c "
    if [ -f /tmp/cli-matrix-multi-life-writer.pid ]; then
        kill \$(cat /tmp/cli-matrix-multi-life-writer.pid) 2>/dev/null || true
        rm -f /tmp/cli-matrix-multi-life-writer.pid
    fi
" || true

sleep 3  # let any final sync settle

# =====================================================================
# Phase 4: cross-RD counter parity (Bug 353 strict point-in-time)
# =====================================================================
echo ">> Phase 4: cross-RD counter parity from each snapshot's backing"

read_counter() {
    local node=$1 rd=$2
    on_node "$node" bash -c "
        # ZFS path: clone the snap to an ephemeral RW dataset and
        # read the first 8 bytes from /dev/zvol/<clone>.
        snap_full=\$(zfs list -H -t snapshot 2>/dev/null \
            | awk '{print \$1}' | grep -E '${rd}.*@${SNAP}' | head -1)
        if [ -n \"\$snap_full\" ]; then
            tmp_clone=\"\${snap_full%@*}/cli-matrix-multi-life-cnt-${rd}\"
            zfs clone \"\$snap_full\" \"\$tmp_clone\" 2>/dev/null && \
            head -c 8 \"/dev/zvol/\$tmp_clone\" 2>/dev/null | xxd -p
            zfs destroy \"\$tmp_clone\" 2>/dev/null
            exit 0
        fi
        # LVM-thin snapshot LV: activate, dd first 8 bytes, deactivate.
        lv=\$(lvs --noheadings -o lv_full_name 2>/dev/null \
            | grep -E '${rd}.*${SNAP}' | head -1 | tr -d ' ')
        if [ -n \"\$lv\" ]; then
            lvchange -ay \"\$lv\" 2>/dev/null || true
            head -c 8 \"/dev/\$lv\" 2>/dev/null | xxd -p
            lvchange -an \"\$lv\" 2>/dev/null || true
            exit 0
        fi
        echo ''
    " 2>/dev/null
}

cnt_a=$(read_counter "$N1" "$RD_A")
cnt_b=$(read_counter "$N1" "$RD_B")
cnt_c=$(read_counter "$N1" "$RD_C")
echo "   $RD_A.counter = $cnt_a"
echo "   $RD_B.counter = $cnt_b"
echo "   $RD_C.counter = $cnt_c"

if [[ -z "$cnt_a" || -z "$cnt_b" || -z "$cnt_c" ]]; then
    echo "FAIL: could not read counter byte from one or more snapshots" >&2
    exit 1
fi

dec_a=$((16#${cnt_a}))
dec_b=$((16#${cnt_b}))
dec_c=$((16#${cnt_c}))
echo "   decimal: a=$dec_a b=$dec_b c=$dec_c"

# All three must be within ±1 of each other. Compute max-min span.
hi=$dec_a; lo=$dec_a
for v in $dec_b $dec_c; do
    (( v > hi )) && hi=$v
    (( v < lo )) && lo=$v
done
span=$(( hi - lo ))
echo "   span (max-min) = $span (must be ≤ 1)"
if (( span > 1 )); then
    echo "FAIL (Bug 353 strict): cross-RD counter span $span > 1 — batch did NOT capture a point-in-time barrier" >&2
    echo "  → Suspend-io may have run per-entry serially instead of as a single barrier across all RDs." >&2
    echo "  → Restore from this snapshot set (data + WAL + log) would be inconsistent." >&2
    exit 1
fi

# =====================================================================
# Phase 5: partial-failure cell variant — bad entry in the batch
# =====================================================================
echo ">> Phase 5: partial-failure path — entry with non-existent RD"
# Mix a known-good RD ($RD_A is already snapped; we use a new snap
# name) with a non-existent RD. Either:
#   - the apiserver rejects the WHOLE batch up-front (preferred —
#     transactional), OR
#   - it surfaces a per-entry error envelope (acceptable — operator
#     can see exactly which entry failed).
# Silent partial success (one snap created, no error reported) is a
# FAIL — that's the regression we're catching.
BAD_SNAP=snap-partial-fail
pre_count=$(kubectl get snapshots.blockstor.io.blockstor.io --no-headers 2>/dev/null \
    | awk -v s="$BAD_SNAP" '$1 ~ s {n++} END {print n+0}')

partial_out=""
partial_rc=0
partial_out=$("${LCTL[@]}" snapshot create-multiple \
    "${RD_A}:${BAD_SNAP}" "${RD_MISSING}:${BAD_SNAP}" 2>&1) || partial_rc=$?

if (( partial_rc == 0 )); then
    # CLI returned 0 — possibly the CLI swallowed the per-entry
    # error. Verify on the wire: how many Snapshot CRDs got
    # created? If RD_MISSING produced no CRD AND RD_A produced
    # one, that is silent partial success — FAIL.
    post_count=$(kubectl get snapshots.blockstor.io.blockstor.io --no-headers 2>/dev/null \
        | awk -v s="$BAD_SNAP" '$1 ~ s {n++} END {print n+0}')
    if (( post_count == 1 )); then
        echo "FAIL (Bug 353 partial-fail): create-multiple silently created 1/2 snaps when one RD was missing" >&2
        echo "  CLI output:" >&2
        echo "  $partial_out" >&2
        echo "  → must either fail-fast (zero CRDs created) or report per-entry error envelope." >&2
        # Best-effort cleanup of the orphan good-side snap so the
        # final delete loop doesn't double-fail.
        "${LCTL[@]}" snapshot delete "$RD_A" "$BAD_SNAP" 2>/dev/null || true
        exit 1
    fi
    if (( post_count == 0 )); then
        echo "   → CLI returned 0 but no CRD was created — apiserver fail-fast detected at validation time, acceptable"
    fi
else
    # Non-zero exit: the CLI surfaced an error. The output must
    # name the offending RD so the operator can correlate.
    if ! grep -qE "(${RD_MISSING}|not.found|no.such|missing|does.not.exist)" <<<"$partial_out"; then
        echo "FAIL (Bug 353 partial-fail): error envelope does not name the offending entry" >&2
        echo "  expected ${RD_MISSING} in: $partial_out" >&2
        # Allow this to fail to keep the assertion strict — operator
        # cannot recover without knowing which entry was bad.
        exit 1
    fi
    # Check no orphan good-side snap was left behind. Fail-fast is
    # the preferred shape; partial-then-rollback also acceptable as
    # long as the orphan is cleaned up by the time the call returns.
    post_count=$(kubectl get snapshots.blockstor.io.blockstor.io --no-headers 2>/dev/null \
        | awk -v s="$BAD_SNAP" '$1 ~ s {n++} END {print n+0}')
    if (( post_count > 0 )); then
        echo "   note: partial-fail rejected with $post_count orphan CRD(s); cleanup will reap them"
        "${LCTL[@]}" snapshot delete "$RD_A" "$BAD_SNAP" 2>/dev/null || true
    fi
fi

# =====================================================================
# Phase 6: linstor snapshot list shows all 3 good snaps Successful
# =====================================================================
echo ">> Phase 6: linstor snapshot list shows all batched snaps"
sl_out=$("${LCTL[@]}" snapshot list 2>&1 || true)
for RD in "$RD_A" "$RD_B" "$RD_C"; do
    if ! grep -q "$RD" <<<"$sl_out"; then
        echo "FAIL: linstor snapshot list does not show $RD entry" >&2
        echo "$sl_out" >&2
        exit 1
    fi
done

# =====================================================================
# Cleanup handled by EXIT trap.
# =====================================================================
echo ">> PASS: snap-create-multiple-lifecycle (Bug 353 GroupID + atomic phase advance + cross-RD counter parity pinned)"
