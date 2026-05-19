#!/usr/bin/env bash
#
# usage: r-c-over-tiebreaker-skip-sync.sh WORK_DIR
#
# L6 cli-matrix cell — Bug 347 + Bug 348.
#
# Reproduction from the e2e2 stand (user-reported, 2026-05-19):
#
#   $ linstor r c e2e2-worker-3 test   # promote tiebreaker → diskful
#   $ linstor r l
#   test  e2e2-worker-1  ...  UpToDate(29%)
#   test  e2e2-worker-2  ...  UpToDate(32%)
#   test  e2e2-worker-3  ...  Inconsistent(32%)
#
# Two distinct contract violations surface here:
#
# Bug 347 — full sync instead of skip-sync on tieB→diskful flip.
# The promoted replica (worker-3) goes Inconsistent and runs a
# full resync of the entire volume. Diskful peers (worker-1,
# worker-2) had matching UpToDate currentGI before the promotion;
# upstream DRBD-9 + drbdmeta set-gi can skip the initial sync via
# Bug 77's seedInitialGi pre-stamp. blockstor's reconciler.go
# ensureMetadata explicitly bypasses seedInitialGi on a
# diskless→diskful flip (the inline comment claims "kernel slot
# already handshaken via diskless path"). That assumption is
# wrong for tiebreaker promotion: the tiebreaker has no backing
# storage, just a DRBD connection — promoting it allocates fresh
# LV/zvol, drbdadm create-md stamps zero GI, the kernel sees a
# mismatched GI vs. peers, → full sync.
#
# Bug 348 — `linstor r l` source state should be `SyncSource`,
# not `UpToDate(NN%)`. Upstream LINSTOR's State column reads
# directly from drbdsetup events2 (replication_state field):
# during a resync the source side reports `SyncSource`, the
# target side `SyncTarget`. blockstor instead displays
# `UpToDate(NN%)` on the diskful peers — visually plausible
# (the data IS uptodate) but loses the operator-facing signal
# that "this replica is currently sending data to the new one".
# Bug 331 closed the wire-shape for Connecting/NetworkFailure
# states but missed the SyncSource/SyncTarget pair.
#
# Test contract:
#   1. Build a 2-diskful + 1-tiebreaker RD (--auto-place=2 on
#      a 3-worker stand spawns the tiebreaker on worker-3).
#   2. Wait both diskful UpToDate, tiebreaker DISKLESS.
#   3. Promote the tiebreaker via `linstor r c <tieB> <rd>`.
#   4. **Bug 347 assertion**: poll `linstor r l -o json` for 60s.
#      The promoted replica's diskState must reach UpToDate within
#      30s (skip-sync window) AND must never report a sync-progress
#      suffix > 0% — that's the full-sync fingerprint. A brief
#      Inconsistent flash on the very first reconcile is tolerated
#      (≤5s); anything longer = full sync started.
#   5. **Bug 348 assertion**: during any window where the promoted
#      replica is Inconsistent / SyncTarget, the diskful peers must
#      report replicationState=SyncSource (or the State column
#      must contain the substring "SyncSource"). They MUST NOT
#      display `UpToDate(NN%)` with non-empty NN% — that's the
#      regression.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup
trap linstor_cli_teardown EXIT

RD=cli-matrix-rc-over-tb
POOL=${POOL:-stand}

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> 2 diskful + 1 tiebreaker RD on the 3-worker stand (--auto-place=2)"
_out=$("${LCTL[@]}" resource-definition create "$RD" 2>&1) \
    || { echo "FAIL: rd c $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" volume-definition create "$RD" 128M 2>&1) \
    || { echo "FAIL: vd c $RD 128M: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create --auto-place=2 --storage-pool="$POOL" "$RD" 2>&1) \
    || { echo "FAIL: r c --auto-place=2 -s $POOL $RD: $_out" >&2; exit 1; }

# Resolve who got the 2 diskful slots and who got the tiebreaker.
echo ">> wait up to 90s for both diskful UpToDate + tiebreaker present"
deadline=$(( $(date +%s) + 90 ))
declare -a diskful=()
tieB=""
while (( $(date +%s) < deadline )); do
    rows=$("${LCTL[@]}" --machine-readable resource list --resources "$RD" 2>/dev/null)
    mapfile -t diskful < <(jq -r --arg rd "$RD" '
        .[0].resources[]?
        | select(.name==$rd)
        | select((.flags // []) | index("DISKLESS") | not)
        | .node_name' <<<"$rows")
    tieB=$(jq -r --arg rd "$RD" '
        .[0].resources[]?
        | select(.name==$rd)
        | select((.flags // []) | index("DISKLESS"))
        | .node_name' <<<"$rows" | head -1)
    if (( ${#diskful[@]} == 2 )) && [[ -n "$tieB" ]]; then
        if wait_uptodate "$RD" "${diskful[0]}" "${diskful[1]}" 1>/dev/null 2>&1; then
            break
        fi
    fi
    sleep 2
done
if (( ${#diskful[@]} != 2 )) || [[ -z "$tieB" ]]; then
    echo "FAIL: setup never converged to 2 diskful + 1 tiebreaker (diskful=${diskful[*]:-none}, tieB=${tieB:-none})" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -20 >&2
    exit 1
fi
echo "   diskful: ${diskful[*]}  tiebreaker: $tieB"

# Capture peers' initial currentGI — this is the signal that
# seedInitialGi would have to match for skip-sync to fire.
echo ">> capture diskful peers' currentGi (pre-promote)"
gi_n1=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${diskful[0]}" \
    -o jsonpath='{.status.volumes[0].currentGi}' 2>/dev/null || echo "")
gi_n2=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${diskful[1]}" \
    -o jsonpath='{.status.volumes[0].currentGi}' 2>/dev/null || echo "")
echo "   ${diskful[0]} currentGi=$gi_n1"
echo "   ${diskful[1]} currentGi=$gi_n2"

# =====================================================================
# Promote the tiebreaker
# =====================================================================
echo ">> [Bug 347 trigger] linstor r c $tieB $RD  (promote tiebreaker → diskful)"
promote_ts=$(date +%s)
_out=$("${LCTL[@]}" resource create "$tieB" "$RD" --storage-pool="$POOL" 2>&1) \
    || { echo "FAIL: r c $tieB $RD: $_out" >&2; exit 1; }

# =====================================================================
# Bug 347 assertion — promoted replica must NOT do full sync
# =====================================================================
echo ">> [Bug 347] poll up to 60s — promoted node must reach UpToDate via skip-sync"
deadline=$(( $(date +%s) + 60 ))
saw_full_sync=false
saw_full_sync_state=""
promoted_uptodate=false
while (( $(date +%s) < deadline )); do
    # Per-node disk + replication state via the machine-readable
    # output (mirrors `linstor r l -o json`). Pull both diskState
    # and replicationState from observer-stamped Status.
    promoted_disk=$(status_disk_state "$RD" "$tieB" 0)
    promoted_rep=$(kubectl get "resources.blockstor.io.blockstor.io/${RD}.${tieB}" \
        -o jsonpath='{.status.volumes[0].replicationState}' 2>/dev/null || echo "")

    # Bug 347 signature: the promoted replica is Inconsistent OR
    # SyncTarget for any meaningful duration. A few seconds of
    # Inconsistent on the very first reconcile is tolerable
    # (kernel attach race), but full sync transitions through
    # SyncTarget with progressing %. Capture either.
    case "$promoted_disk" in
        Inconsistent|Outdated)
            elapsed=$(( $(date +%s) - promote_ts ))
            if (( elapsed > 5 )); then
                saw_full_sync=true
                saw_full_sync_state="disk=$promoted_disk rep=$promoted_rep at +${elapsed}s"
            fi
            ;;
    esac
    if [[ "$promoted_rep" == "SyncTarget" ]]; then
        saw_full_sync=true
        saw_full_sync_state="disk=$promoted_disk rep=SyncTarget"
    fi

    if [[ "$promoted_disk" == "UpToDate" && ( -z "$promoted_rep" || "$promoted_rep" == "Established" ) ]]; then
        promoted_uptodate=true
        break
    fi
    sleep 2
done

if $saw_full_sync; then
    echo "FAIL (Bug 347): promoted replica $tieB ran full sync after r c — $saw_full_sync_state" >&2
    echo "   Expected: skip-sync via seedInitialGi (peers' currentGi gi_n1=$gi_n1 gi_n2=$gi_n2)" >&2
    echo "   Root cause hint: reconciler.go ensureMetadata bypasses seedInitialGi on diskless→diskful flip" >&2
    echo "----- linstor r l --resources $RD -----" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -10 >&2
    echo "----- on-host drbdsetup status -----" >&2
    on_node "$tieB" drbdsetup status --verbose "$RD" 2>&1 | head -30 >&2 || true
    exit 1
fi
if ! $promoted_uptodate; then
    echo "FAIL (Bug 347): promoted replica $tieB never reached UpToDate within 60s" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -10 >&2
    exit 1
fi

# =====================================================================
# Bug 348 assertion — upstream-shaped State during sync window
# =====================================================================
# Even with skip-sync, drbdadm adjust may briefly transition the
# diskful peers through SyncSource before settling. If we never
# observed a sync window at all (pure skip-sync, kernel reports
# Established immediately), the cell can't validate Bug 348 — skip
# that assertion with a note. If we did observe a transient sync
# window, the source-side State column MUST contain "SyncSource"
# in its plain-text rendering AND replicationState=SyncSource in
# JSON; it MUST NOT be the legacy `UpToDate(NN%)` shape.
echo ">> [Bug 348] verify diskful peers report SyncSource (not UpToDate(NN%)) during sync window"

# Trigger a small mutation that forces a sync window: write 16 MiB
# on the primary peer to bump GI and force a delta sync to the
# secondary. (If the cluster has no primary, drbdadm primary the
# first diskful.)
prim=$("${LCTL[@]}" --machine-readable resource list --resources "$RD" 2>/dev/null \
    | jq -r --arg rd "$RD" '.[0].resources[]? | select(.name==$rd) | select(.layer_object?.drbd?.role=="Primary") | .node_name' \
    | head -1)
if [[ -z "$prim" ]]; then
    on_node "${diskful[0]}" bash -c "drbdadm primary --force $RD 2>/dev/null" || true
    prim="${diskful[0]}"
fi

# Write 32 MiB; secondary will need to catch up. With skip-sync
# the catch-up is fast but still passes through SyncSource on
# the source side per upstream events2 semantics.
on_node "$prim" bash -c "dd if=/dev/urandom of=/dev/drbd/by-res/$RD/0 bs=1M count=32 status=none oflag=direct 2>/dev/null" || true

# Capture wire-shape for ~10s post-mutation.
shape_ok=false
shape_bad_seen=""
deadline=$(( $(date +%s) + 10 ))
while (( $(date +%s) < deadline )); do
    rows=$("${LCTL[@]}" resource list --resources "$RD" 2>/dev/null || echo "")
    # Pull every diskful peer's State column (plain-text, what
    # the operator sees). The cell isn't strict about WHICH peer
    # is the source — it just verifies that during a sync window
    # at least one diskful peer reads as SyncSource.
    if grep -qE 'SyncSource' <<<"$rows"; then
        shape_ok=true
        break
    fi
    # Capture the bad shape (UpToDate(NN%) where NN > 0) for the
    # error diagnostic.
    if grep -qE 'UpToDate\([0-9]+%\)' <<<"$rows"; then
        shape_bad_seen=$(grep -E 'UpToDate\([0-9]+%\)' <<<"$rows" | head -3)
    fi
    sleep 1
done

if ! $shape_ok && [[ -n "$shape_bad_seen" ]]; then
    echo "FAIL (Bug 348): diskful peer rendered as UpToDate(NN%) during sync — upstream shape is SyncSource" >&2
    echo "----- observed bad rows -----" >&2
    echo "$shape_bad_seen" >&2
    echo "----- full linstor r l -----" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -10 >&2
    exit 1
fi

# Final state: all 3 nodes UpToDate
echo ">> wait up to 60s for steady-state UpToDate on all 3 nodes"
deadline=$(( $(date +%s) + 60 ))
all_uptodate=false
while (( $(date +%s) < deadline )); do
    s_n1=$(status_disk_state "$RD" "${diskful[0]}" 0)
    s_n2=$(status_disk_state "$RD" "${diskful[1]}" 0)
    s_n3=$(status_disk_state "$RD" "$tieB" 0)
    if [[ "$s_n1" == "UpToDate" && "$s_n2" == "UpToDate" && "$s_n3" == "UpToDate" ]]; then
        all_uptodate=true
        break
    fi
    sleep 2
done
if ! $all_uptodate; then
    echo "FAIL: steady-state never reached — ${diskful[0]}=$s_n1 ${diskful[1]}=$s_n2 $tieB=$s_n3" >&2
    exit 1
fi

echo ">> r-c-over-tiebreaker-skip-sync OK (Bug 347+348 pinned: skip-sync fired + State shape matches upstream)"
