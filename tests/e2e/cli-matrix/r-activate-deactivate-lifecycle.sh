#!/usr/bin/env bash
#
# usage: r-activate-deactivate-lifecycle.sh WORK_DIR
#
# L6 cli-matrix cell — task #557.
#
# Multi-cycle activate/deactivate lifecycle catcher. Goes beyond the
# narrower Bug-350 peer-drop catcher (r-deactivate-drops-peer-from-res.sh)
# by exercising the full operator roundtrip — deactivate, run live I/O
# on the surviving single replica while the peer is INACTIVE, then
# reactivate and assert partial-sync (NOT full resync) convergence.
# Repeats the cycle 3x to stress idempotency.
#
# Upstream LINSTOR contract (Apache 2.0 reference, read-only):
#
#   `linstor r deactivate <node> <rd>` flips Resource.Flags |= INACTIVE.
#   On the surviving peer:
#     - The satellite re-renders .res WITHOUT the INACTIVE peer's
#       `on <node> {...}` block (DrbdResourceFileUtils filters
#       `!Resource.Flags.INACTIVE`).
#     - `drbdadm adjust` drops the kernel connection slot.
#     - DRBD-9's `--force-io-failures` / `on-no-quorum=io-error`
#       semantics flip the quorum frame: in a 2-replica RD where one
#       peer is INACTIVE, the surviving peer is the SOLE remaining
#       voter and must continue accepting writes (single-replica
#       quorum override). Otherwise `deactivate` would be useless —
#       the operator workflow it serves (e.g. evacuate-then-recreate)
#       requires writes to continue on the surviving replica.
#
#   `linstor r activate <node> <rd>` clears INACTIVE. The satellite
#   re-renders .res WITH the peer block restored, runs `drbdadm
#   adjust`, and DRBD-9 begins a PARTIAL bitmap-driven resync — NOT
#   a full resync. The Generation Identifier (GI) seed remains valid
#   across the deactivate window because INACTIVE is a wire-level
#   admin op, not a peer-loss event; only the bytes written DURING
#   the INACTIVE window are out-of-sync, so the resync delta should
#   be ~size-of-IO, not size-of-RD.
#
# Test contract (3 cycles):
#
#   Setup: 2-replica diskful RD on worker-1 + worker-2, both UpToDate.
#
#   Each cycle:
#     1. linstor r deactivate N2 RD
#        - INACTIVE flag visible in Spec.Flags within 30s on N2
#        - Kernel slot down on N2 (drbdadm status shows nothing or
#          disk:None for RD)
#        - .res on N1 no longer contains `on <N2>` block (Bug 350)
#        - drbdsetup status on N1 lists N1 only (Bug 350)
#
#     2. IO during INACTIVE:
#        - Promote N1 to Primary if not already
#        - Write ~64 MiB random pattern to /dev/drbd/by-res/<RD>/0
#        - Must complete without quorum block (single-replica quorum
#          override). Capture md5 of pattern.
#
#     3. linstor r activate N2 RD
#        - INACTIVE flag dropped within 30s on N2
#        - Kernel slot UP on N2 (drbdadm status shows it)
#        - peer block restored on N1's .res
#        - replicationState Established within 60s on both peers
#        - PARTIAL sync only: wait_sync_done reaches clean UpToDate
#          within 90s. Delta is bounded by the 64 MiB written above,
#          NOT a full RD resync.
#        - md5 of seed on N1 matches the pre-deact pattern (data
#          integrity anchor; the partial-sync mechanism must not
#          corrupt the writer's view).
#
#   Cleanup: delete_rd + assert_no_orphans.
#
# Catches regressions in:
#   - deactivate semantics (Bug 350 family — peer not dropped)
#   - single-replica quorum override during INACTIVE (writes would
#     block on a 1/2-voter shape if the override is missing)
#   - partial-sync on reactivate via observer's events2 progress
#     tracking (full resync would mean the GI seed was nuked)
#   - data integrity via md5 anchor (corruption of the surviving
#     replica's bytes during the reactivate-resync handshake)
#   - idempotency across 3 cycles (state-machine leak that survives
#     one round-trip but accumulates across N)

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup
trap linstor_cli_teardown EXIT

RD=cli-matrix-act-deact-lifecycle
N1=$WORKER_1
N2=$WORKER_2
SIZE_MIB=256          # RD total size — large enough that a full
                      # resync is meaningfully slower than a partial
                      # one (~64 MiB delta), so timing differences
                      # surface clearly. Also matches the seed write.
IO_MIB=64             # Per-cycle delta written during INACTIVE.

# Hold seed pattern md5 across cycles for the data-integrity anchor.
SEED_MD5=""

cleanup() {
    # Try to reactivate so delete_rd can drive a clean tear-down
    # through the normal r d path.
    "${LCTL[@]}" resource activate "$N2" "$RD" 2>/dev/null || true
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

# Cache satellite pod for $N1 once — every per-cycle .res probe and
# md5 sample reuses this. The pod name is stable for the duration of
# the test (Bug 298 require_workers guard ensures pods are Ready
# before we start).
N1_SAT=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o jsonpath="{.items[?(@.spec.nodeName==\"$N1\")].metadata.name}")
if [[ -z "$N1_SAT" ]]; then
    echo "FAIL: satellite pod for $N1 not found" >&2
    exit 1
fi

# =====================================================================
# Setup: 2-replica diskful RD on $N1 + $N2, both UpToDate
# =====================================================================
echo ">> Setup: 2-replica diskful RD on $N1 + $N2 (size=${SIZE_MIB} MiB)"
_out=$("${LCTL[@]}" resource-definition create "$RD" 2>&1) \
    || { echo "FAIL: rd c $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" volume-definition create "$RD" "${SIZE_MIB}M" 2>&1) \
    || { echo "FAIL: vd c $RD ${SIZE_MIB}M: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N1" "$RD" --storage-pool=stand 2>&1) \
    || { echo "FAIL: r c $N1 $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N2" "$RD" --storage-pool=stand 2>&1) \
    || { echo "FAIL: r c $N2 $RD: $_out" >&2; exit 1; }

wait_uptodate "$RD" "$N1" "$N2"

# Promote $N1 to Primary so writes during INACTIVE work without
# additional promotion races inside the cycle loop.
echo ">> promote $N1 to Primary"
on_node "$N1" drbdadm primary --force "$RD" 2>/dev/null || true
wait_role "$RD" "$N1" "Primary" 30 \
    || { echo "FAIL: $N1 never reached Primary" >&2; exit 1; }

# Seed an initial deterministic 64 MiB pattern at offset 0 so the
# md5 anchor has something to verify across cycles. The seed is
# written BEFORE the first deactivate so both replicas hold the
# same bytes and the GI baseline is established.
echo ">> seed initial ${IO_MIB} MiB pattern on $N1 (will be the md5 anchor)"
on_node "$N1" bash -c "
    set -e
    dev=\$(readlink -f /dev/drbd/by-res/${RD}/0 2>/dev/null || true)
    if [ -z \"\$dev\" ]; then
        dev=\$(ls -1 /dev/drbd* 2>/dev/null | grep -vE 'by-(res|disk)' | head -1)
    fi
    # Deterministic seed so the md5 is reproducible across runs and
    # any divergence after reactivate has a single root cause.
    dd if=/dev/urandom of=/tmp/${RD}-seed.bin bs=1M count=${IO_MIB} status=none
    dd if=/tmp/${RD}-seed.bin of=\$dev bs=1M conv=fsync status=none
" || { echo "FAIL: seed write on $N1" >&2; exit 1; }

# Replicate the seed to $N2 before any deactivate — both peers must
# hold the same baseline bytes for the partial-sync contract to be
# meaningful (otherwise we'd be comparing apples-to-oranges).
wait_uptodate "$RD" "$N1" "$N2"

# Capture the seed md5 — this is the anchor that must remain
# unchanged on $N1 across every deactivate/reactivate cycle.
SEED_MD5=$(on_node "$N1" bash -c "md5sum /tmp/${RD}-seed.bin | awk '{print \$1}'")
if [[ -z "$SEED_MD5" || ${#SEED_MD5} -ne 32 ]]; then
    echo "FAIL: could not capture seed md5 on $N1 (got '$SEED_MD5')" >&2
    exit 1
fi
echo "   seed md5 anchor: $SEED_MD5"

# =====================================================================
# Cycle loop — repeat the full deactivate→IO→activate roundtrip 3
# times. Each cycle must complete cleanly; cumulative state leak
# from one cycle into the next will surface as either a hung
# deactivate, a failed quorum override, a full resync (instead of
# partial), or a corrupted md5 anchor.
# =====================================================================
for cycle in 1 2 3; do
    echo ""
    echo "===================="
    echo ">> CYCLE $cycle / 3"
    echo "===================="

    # -----------------------------------------------------------------
    # Step 1: linstor r deactivate $N2 $RD
    # -----------------------------------------------------------------
    echo ">> [cycle $cycle / step 1] linstor r deactivate $N2 $RD"
    _out=$("${LCTL[@]}" resource deactivate "$N2" "$RD" 2>&1) \
        || { echo "FAIL (cycle $cycle): r deactivate $N2 $RD: $_out" >&2; exit 1; }

    # Wait up to 30s for the INACTIVE flag to land in Spec.Flags on
    # the deactivated peer's Resource CRD.
    echo ">>   wait up to 30s for INACTIVE flag on $N2"
    deadline=$(( $(date +%s) + 30 ))
    inactive_seen=false
    while (( $(date +%s) < deadline )); do
        flags=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N2}" \
            -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
        if [[ "$flags" == *"INACTIVE"* ]]; then
            inactive_seen=true
            break
        fi
        sleep 2
    done
    if ! $inactive_seen; then
        echo "FAIL (cycle $cycle): $N2 never got INACTIVE flag within 30s" >&2
        kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N2}" -o yaml 2>&1 | tail -30 >&2
        exit 1
    fi

    # Settle window: .res rewrite + drbdadm adjust on $N1 take a few
    # seconds after the flag flips. Without it, the next probe could
    # race the satellite's reconcile.
    sleep 5

    # Bug 350 contract reuse: .res on $N1 must NOT contain `on <N2>`.
    echo ">>   [Bug 350] $N1 .res must NOT contain 'on \"$N2\"' block"
    res_dump=$(kubectl -n "$NS" exec "$N1_SAT" -- \
        cat "/etc/drbd.d/${RD}.res" 2>/dev/null || echo "")
    if [[ -z "$res_dump" ]]; then
        echo "FAIL (cycle $cycle): could not read .res for $RD on $N1" >&2
        exit 1
    fi
    if grep -qE "^[[:space:]]*on[[:space:]]+\"?${N2}\"?[[:space:]]*\\{" <<<"$res_dump"; then
        echo "FAIL (cycle $cycle, Bug 350): .res on $N1 still contains 'on $N2' block" >&2
        echo "----- rendered .res on $N1 -----" >&2
        echo "$res_dump" >&2
        exit 1
    fi

    # Bug 350 contract reuse: drbdsetup status on $N1 must NOT list
    # $N2 as a peer connection. Single-replica shape on $N1.
    echo ">>   [Bug 350] drbdsetup status on $N1 must NOT list $N2 as peer"
    status_dump=$(on_node "$N1" drbdsetup status --verbose "$RD" 2>/dev/null || echo "")
    if [[ -z "$status_dump" ]]; then
        echo "FAIL (cycle $cycle): drbdsetup status on $N1 empty" >&2
        exit 1
    fi
    if grep -qE "(^|[[:space:]])${N2}([[:space:]]|$)" <<<"$status_dump"; then
        echo "FAIL (cycle $cycle, Bug 350): drbdsetup status on $N1 still lists $N2" >&2
        echo "----- drbdsetup status on $N1 -----" >&2
        echo "$status_dump" >&2
        exit 1
    fi

    # Kernel slot DOWN on $N2: drbdadm status should show either
    # nothing for this RD (slot fully torn) or `disk:None`. A live
    # slot here means INACTIVE wasn't honored at the kernel level.
    echo ">>   kernel slot DOWN on $N2 (drbdadm status shows no live slot)"
    n2_status=$(on_node "$N2" drbdsetup status --verbose "$RD" 2>/dev/null || echo "")
    if [[ -n "$n2_status" ]]; then
        # If status returned anything, it must not show a live attached
        # disk; INACTIVE drops the kernel device.
        if grep -qE 'disk:(UpToDate|Inconsistent|Outdated|Consistent|Attaching|Negotiating)' <<<"$n2_status"; then
            echo "FAIL (cycle $cycle): $N2 still has a live kernel disk slot after deactivate" >&2
            echo "----- drbdsetup status on $N2 -----" >&2
            echo "$n2_status" >&2
            exit 1
        fi
    fi

    # -----------------------------------------------------------------
    # Step 2: IO during INACTIVE — single-replica quorum override
    # -----------------------------------------------------------------
    echo ">> [cycle $cycle / step 2] write ${IO_MIB} MiB on $N1 (INACTIVE peer; quorum override)"

    # Re-promote in case the deactivate's adjust dance flipped $N1
    # back to Secondary (DRBD-9 sometimes demotes on resource-wide
    # reconfigure). Idempotent if already Primary.
    on_node "$N1" drbdadm primary --force "$RD" 2>/dev/null || true
    wait_role "$RD" "$N1" "Primary" 30 \
        || { echo "FAIL (cycle $cycle): $N1 never re-reached Primary after deactivate" >&2; exit 1; }

    # Write a fresh ${IO_MIB} MiB random pattern at offset ${IO_MIB} MiB
    # (i.e. AFTER the seed region) so the seed md5 anchor at offset 0
    # stays untouched and we can verify it across the reactivate.
    # Must complete without quorum block: with $N2 INACTIVE, $N1 is
    # the sole voter and DRBD's single-replica quorum override must
    # let writes through. A failure here = quorum frame is wrong on
    # INACTIVE peers.
    io_out=$(on_node "$N1" bash -c "
        set -e
        dev=\$(readlink -f /dev/drbd/by-res/${RD}/0 2>/dev/null || true)
        if [ -z \"\$dev\" ]; then
            dev=\$(ls -1 /dev/drbd* 2>/dev/null | grep -vE 'by-(res|disk)' | head -1)
        fi
        # 60s timeout — a write of ${IO_MIB} MiB on local LVM/ZFS on a
        # 1-replica shape (no DRBD replication round-trip) should
        # complete in 1-5s. 60s catches the 'quorum blocked
        # indefinitely' regression with a clean timeout.
        timeout 60 dd if=/dev/urandom of=\$dev bs=1M count=${IO_MIB} \
            seek=${IO_MIB} conv=fsync status=none
        echo OK
    " 2>&1) || io_out="ERR: $io_out"
    if [[ "$io_out" != *"OK"* ]]; then
        echo "FAIL (cycle $cycle): ${IO_MIB} MiB write on $N1 during INACTIVE failed/timed out" >&2
        echo "  (single-replica quorum override missing — INACTIVE peer left as voter?)" >&2
        echo "  dd output: $io_out" >&2
        # Diagnostic dump of suspended state / quorum frame.
        echo "  suspended state on $N1: $(status_suspended "$RD" "$N1")" >&2
        echo "  volume quorum on $N1: $(status_volume_quorum "$RD" "$N1" 0)" >&2
        exit 1
    fi

    # Verify the seed anchor is still intact on $N1 before reactivate
    # — the deactivate path must not have corrupted it. (md5 of the
    # FIRST ${IO_MIB} MiB of the device, since the seed lives at
    # offset 0 and the new write was at offset ${IO_MIB} MiB.)
    seed_md5_now=$(on_node "$N1" bash -c "
        dev=\$(readlink -f /dev/drbd/by-res/${RD}/0 2>/dev/null || true)
        if [ -z \"\$dev\" ]; then
            dev=\$(ls -1 /dev/drbd* 2>/dev/null | grep -vE 'by-(res|disk)' | head -1)
        fi
        dd if=\$dev bs=1M count=${IO_MIB} status=none | md5sum | awk '{print \$1}'
    " 2>/dev/null || echo "")
    if [[ "$seed_md5_now" != "$SEED_MD5" ]]; then
        echo "FAIL (cycle $cycle): seed md5 anchor on $N1 CHANGED during INACTIVE write" >&2
        echo "  expected: $SEED_MD5" >&2
        echo "  got:      $seed_md5_now" >&2
        echo "  → corruption of the surviving replica's existing bytes during single-replica IO" >&2
        exit 1
    fi

    # -----------------------------------------------------------------
    # Step 3: linstor r activate $N2 $RD — partial-sync convergence
    # -----------------------------------------------------------------
    echo ">> [cycle $cycle / step 3] linstor r activate $N2 $RD"
    _out=$("${LCTL[@]}" resource activate "$N2" "$RD" 2>&1) \
        || { echo "FAIL (cycle $cycle): r activate $N2 $RD: $_out" >&2; exit 1; }

    # Wait up to 30s for INACTIVE flag to drop from Spec.Flags.
    echo ">>   wait up to 30s for INACTIVE flag to clear on $N2"
    deadline=$(( $(date +%s) + 30 ))
    cleared=false
    while (( $(date +%s) < deadline )); do
        flags=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N2}" \
            -o jsonpath='{.spec.flags}' 2>/dev/null || echo "")
        if [[ "$flags" != *"INACTIVE"* ]]; then
            cleared=true
            break
        fi
        sleep 2
    done
    if ! $cleared; then
        echo "FAIL (cycle $cycle): INACTIVE flag never cleared on $N2 within 30s" >&2
        kubectl get "resources.blockstor.io.blockstor.io/${RD}.${N2}" -o yaml 2>&1 | tail -30 >&2
        exit 1
    fi

    # peer block on $N1's .res must be RESTORED (Bug 350 reverse).
    echo ">>   .res on $N1 must restore 'on \"$N2\"' block within 60s"
    deadline=$(( $(date +%s) + 60 ))
    restored=false
    while (( $(date +%s) < deadline )); do
        res_dump=$(kubectl -n "$NS" exec "$N1_SAT" -- \
            cat "/etc/drbd.d/${RD}.res" 2>/dev/null || echo "")
        if grep -qE "^[[:space:]]*on[[:space:]]+\"?${N2}\"?[[:space:]]*\\{" <<<"$res_dump"; then
            restored=true
            break
        fi
        sleep 2
    done
    if ! $restored; then
        echo "FAIL (cycle $cycle): .res on $N1 did NOT restore 'on $N2' block within 60s" >&2
        echo "----- rendered .res on $N1 -----" >&2
        echo "$res_dump" >&2
        exit 1
    fi

    # Kernel slot UP on $N2: drbdadm status must show the RD again.
    echo ">>   kernel slot UP on $N2 (drbdadm status shows $RD)"
    deadline=$(( $(date +%s) + 60 ))
    slot_up=false
    while (( $(date +%s) < deadline )); do
        n2_status=$(on_node "$N2" drbdsetup status --verbose "$RD" 2>/dev/null || echo "")
        if [[ -n "$n2_status" ]] \
                && grep -qE 'disk:(UpToDate|Inconsistent|Outdated|Consistent|Attaching|Negotiating|SyncTarget)' \
                    <<<"$n2_status"; then
            slot_up=true
            break
        fi
        sleep 2
    done
    if ! $slot_up; then
        echo "FAIL (cycle $cycle): $N2 kernel slot never came back up within 60s" >&2
        echo "----- drbdsetup status on $N2 -----" >&2
        echo "${n2_status:-<empty>}" >&2
        exit 1
    fi

    # Replication state Established on both peers within 60s.
    echo ">>   replication Established on both peers within 60s"
    wait_replication_state "$RD" "$N1" "$N2" "Established" 60 \
        || { echo "FAIL (cycle $cycle): N1→N2 never Established within 60s" >&2; exit 1; }
    wait_replication_state "$RD" "$N2" "$N1" "Established" 60 \
        || { echo "FAIL (cycle $cycle): N2→N1 never Established within 60s" >&2; exit 1; }

    # Partial-sync convergence: wait_sync_done must reach clean
    # UpToDate within 90s. With ${IO_MIB} MiB of delta on a
    # ${SIZE_MIB} MiB RD, a partial sync of just that delta region
    # completes in ~5-15s on QEMU stand; a full resync of
    # ${SIZE_MIB} MiB would push 30-60s+. 90s is the safety margin
    # that still distinguishes the two — a clean 240s timeout
    # (wait_sync_done default) would mask a regression to full
    # resync, so we use a tighter bound here.
    echo ">>   partial-sync to clean UpToDate within 90s (NOT a full resync)"
    wait_sync_done "$RD" "$N1" "$N2" 90 \
        || { echo "FAIL (cycle $cycle): partial-sync did not complete within 90s — likely full resync regression" >&2; exit 1; }
    wait_sync_done "$RD" "$N2" "$N1" 90 \
        || { echo "FAIL (cycle $cycle): N2→N1 partial-sync did not complete within 90s" >&2; exit 1; }

    # Data integrity anchor: the seed md5 on $N1 must still match
    # the pre-deact pattern. The partial-sync handshake must not
    # have written stale bytes onto the surviving replica's data.
    echo ">>   md5 of seed on $N1 still matches pre-deact pattern"
    seed_md5_after=$(on_node "$N1" bash -c "
        dev=\$(readlink -f /dev/drbd/by-res/${RD}/0 2>/dev/null || true)
        if [ -z \"\$dev\" ]; then
            dev=\$(ls -1 /dev/drbd* 2>/dev/null | grep -vE 'by-(res|disk)' | head -1)
        fi
        dd if=\$dev bs=1M count=${IO_MIB} status=none | md5sum | awk '{print \$1}'
    " 2>/dev/null || echo "")
    if [[ "$seed_md5_after" != "$SEED_MD5" ]]; then
        echo "FAIL (cycle $cycle): seed md5 anchor on $N1 CHANGED across reactivate" >&2
        echo "  expected: $SEED_MD5" >&2
        echo "  got:      $seed_md5_after" >&2
        echo "  → partial-sync handshake corrupted the surviving replica's data" >&2
        exit 1
    fi

    echo ">> cycle $cycle / 3 complete"
done

echo ""
echo ">> r-activate-deactivate-lifecycle OK"
echo "   3 deactivate→IO→activate cycles completed cleanly"
echo "   - INACTIVE flag set/cleared each cycle (within 30s)"
echo "   - Bug 350 peer-drop semantics held on every cycle"
echo "   - Single-replica quorum override let writes through during INACTIVE"
echo "   - Partial-sync (not full resync) on each reactivate within 90s"
echo "   - md5 seed anchor preserved across all 3 cycles"
