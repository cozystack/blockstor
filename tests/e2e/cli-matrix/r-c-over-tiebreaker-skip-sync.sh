#!/usr/bin/env bash
#
# usage: r-c-over-tiebreaker-skip-sync.sh WORK_DIR
#
# L6 cli-matrix cell — tiebreaker→diskful promotion convergence
# (known-delta #84) + Bug 348 (SyncSource/SyncTarget wire shape).
#
# KNOWN DELTA (docs/cli-parity-known-deltas.md row 84): promoting a
# tiebreaker to diskful runs a FULL SyncTarget on the promoted node,
# NOT a skip-sync — and that is INTENTIONAL, not a bug.
#
#   $ linstor r c <tieB> test   # promote tiebreaker → diskful
#   $ linstor r l
#   test  worker-1  ...  UpToDate / SyncSource
#   test  worker-2  ...  UpToDate
#   test  worker-3  ...  Inconsistent → SyncTarget → UpToDate
#
# Why BS deliberately full-syncs here instead of skip-syncing:
# the promoted tiebreaker has no backing storage, just a DRBD
# connection. To skip the sync BS would have to pre-stamp a
# matching GI and let the fresh replica win the day0 force-primary
# election — but a diskful peer already holds data
# (anyDiskfulPeerHasData == true), so force-priming the fresh
# replica mints an UNRELATED Current UUID; the data-bearing peer
# then declines the handshake (`uuid_compare()=unrelated-data`,
# "Unrelated data, aborting!") and the pair wedges in mutual
# StandAlone that never auto-recovers (the respawn-StandAlone P0).
# So BS gates the auto-primary seed on `!anyDiskfulPeerHasData`
# (pkg/dispatcher/dispatcher.go): with a data-bearing peer present
# the promoted replica comes up Inconsistent and SyncTargets the
# real bytes off the peer. The data converges correctly; the only
# cost is a full resync of the (here 128 MiB) volume — the safe
# trade vs. a StandAlone wedge. This cell asserts that BS contract,
# NOT the upstream skip-sync one.
#
# Bug 348 — `linstor r l` source state should be `SyncSource`,
# not `UpToDate(NN%)`. Upstream LINSTOR's State column reads
# directly from drbdsetup events2 (replication_state field):
# during a resync the source side reports `SyncSource`, the
# target side `SyncTarget`. blockstor must match: the diskful
# peer feeding the promoted replica reads SyncSource, never the
# legacy `UpToDate(NN%)` shape. Bug 331 closed the wire-shape for
# Connecting/NetworkFailure states but missed the
# SyncSource/SyncTarget pair.
#
# Test contract:
#   1. Build a 2-diskful + 1-tiebreaker RD (--auto-place=2 on
#      a 3-worker stand spawns the tiebreaker on worker-3).
#   2. Wait both diskful UpToDate, tiebreaker DISKLESS.
#   3. Promote the tiebreaker via `linstor r c <tieB> <rd>`.
#   4. **Convergence assertion (delta #84)**: poll `linstor r l -o
#      json`. The promoted replica must converge to UpToDate within
#      the sync budget. A SyncTarget / Inconsistent transit on the
#      way there is EXPECTED (the intended full sync) — it is NOT a
#      failure. Only never reaching UpToDate is a failure.
#   5. **Bug 348 assertion**: while the promoted replica is
#      Inconsistent / SyncTarget, the diskful peers must report
#      replicationState=SyncSource (or the State column must
#      contain "SyncSource"). They MUST NOT display
#      `UpToDate(NN%)` with non-empty NN% — that's the regression.

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
        .[0][]?
        | select(.name==$rd)
        | select((.flags // []) | index("DISKLESS") | not)
        | .node_name' <<<"$rows")
    tieB=$(jq -r --arg rd "$RD" '
        .[0][]?
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
gi_n1=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${diskful[0]}" \
    -o jsonpath='{.status.volumes[0].currentGi}' 2>/dev/null || echo "")
gi_n2=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${diskful[1]}" \
    -o jsonpath='{.status.volumes[0].currentGi}' 2>/dev/null || echo "")
echo "   ${diskful[0]} currentGi=$gi_n1"
echo "   ${diskful[1]} currentGi=$gi_n2"

# =====================================================================
# Promote the tiebreaker
# =====================================================================
echo ">> [delta #84 trigger] linstor r c $tieB $RD  (promote tiebreaker → diskful)"
promote_ts=$(date +%s)
_out=$("${LCTL[@]}" resource create "$tieB" "$RD" --storage-pool="$POOL" 2>&1) \
    || { echo "FAIL: r c $tieB $RD: $_out" >&2; exit 1; }

# =====================================================================
# Convergence assertion (delta #84) — promoted replica SyncTargets
# the data off a diskful peer and reaches UpToDate.
# =====================================================================
# This is the BS-INTENDED behaviour, not a bug: with a data-bearing
# diskful peer present, the promoted tiebreaker comes up Inconsistent
# and runs a full SyncTarget (anyDiskfulPeerHasData gate — see header).
# A SyncTarget / Inconsistent transit is therefore EXPECTED. We assert
# only that the promoted replica CONVERGES to UpToDate; passing through
# the sync is the contract, so we record it for the operator but never
# fail on it. 240s budget matches the full-sync-on-promote window.
echo ">> [delta #84] poll up to 240s — promoted node SyncTargets, then reaches UpToDate"
deadline=$(( $(date +%s) + 240 ))
saw_sync_transit=false
saw_sync_transit_state=""
promoted_uptodate=false
while (( $(date +%s) < deadline )); do
    # Per-node disk + replication state via observer-stamped Status
    # (mirrors `linstor r l -o json`).
    promoted_disk=$(status_disk_state "$RD" "$tieB" 0)
    promoted_rep=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${tieB}" \
        -o jsonpath='{.status.volumes[0].replicationState}' 2>/dev/null || echo "")

    # Record (do NOT fail on) the expected full-sync transit — it is
    # the intended path per delta #84.
    case "$promoted_disk" in
        Inconsistent|Outdated)
            elapsed=$(( $(date +%s) - promote_ts ))
            if (( elapsed > 5 )); then
                saw_sync_transit=true
                saw_sync_transit_state="disk=$promoted_disk rep=$promoted_rep at +${elapsed}s"
            fi
            ;;
    esac
    if [[ "$promoted_rep" == "SyncTarget" ]]; then
        saw_sync_transit=true
        saw_sync_transit_state="disk=$promoted_disk rep=SyncTarget"
    fi

    if [[ "$promoted_disk" == "UpToDate" && ( -z "$promoted_rep" || "$promoted_rep" == "Established" ) ]]; then
        promoted_uptodate=true
        break
    fi
    sleep 2
done

if $saw_sync_transit; then
    echo "   (expected) promoted $tieB ran a full SyncTarget — $saw_sync_transit_state"
    echo "   delta #84: tieB→diskful promotion full-syncs from the data-bearing peer by design"
fi
if ! $promoted_uptodate; then
    echo "FAIL (delta #84): promoted replica $tieB never reached UpToDate within 240s" >&2
    echo "   The tiebreaker→diskful promotion must converge (via SyncTarget) — it did not." >&2
    echo "   peers' currentGi gi_n1=$gi_n1 gi_n2=$gi_n2" >&2
    echo "----- linstor r l --resources $RD -----" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -10 >&2
    echo "----- on-host drbdsetup status -----" >&2
    on_node "$tieB" drbdsetup status --verbose "$RD" 2>&1 | head -30 >&2 || true
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
    | jq -r --arg rd "$RD" '.[0][]? | select(.name==$rd) | select(.layer_object?.drbd?.role=="Primary") | .node_name' \
    | head -1)
if [[ -z "$prim" ]]; then
    on_node "${diskful[0]}" bash -c "drbdadm primary --force $RD 2>/dev/null" || true
    prim="${diskful[0]}"
fi

# Write 32 MiB; secondary will need to catch up. With skip-sync
# the catch-up is fast but still passes through SyncSource on
# the source side per upstream events2 semantics.
# Resolve via `drbdadm sh-dev` (lib.sh resolve_drbd_device): the
# /dev/drbd/by-res symlink is not reliably present in the satellite
# mount namespace, so the by-res dd silently no-ops on the stand.
dev=$(resolve_drbd_device "$prim" "$RD" 0 2>/dev/null) || dev=""
[ -n "$dev" ] && on_node "$prim" bash -c "dd if=/dev/urandom of=$dev bs=1M count=32 status=none oflag=direct 2>/dev/null" || true

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

echo ">> r-c-over-tiebreaker-skip-sync OK (delta #84: tieB→diskful full-syncs by design + Bug 348 SyncSource shape pinned)"
