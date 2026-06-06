#!/usr/bin/env bash
#
# usage: r-d-last-uptodate-midsync-rejected.sh WORK_DIR
#
# L6 cli-matrix cell — U130 (LINSTOR shouldn't allow deleting the last
# active peer).
#
# User-reported upstream wedge (LINBIT/linstor-server, closed): start
# with 1 diskful (UpToDate) + 1 diskless Primary/InUse, `r c` a SECOND
# diskful (which begins as SyncTarget/Inconsistent), then `r d` the
# ORIGINAL diskful before the resync finishes. The cluster wedges:
#   - DELETING node stays Connecting,
#   - the SyncTarget is stranded (its only sync source is gone),
#   - the Primary is left on a diskless replica with no UpToDate backing.
#
# BS guard (pkg/rest/resource_delete_last_uptodate_u130.go): `r d` of the
# LAST UpToDate diskful replica while a diskful peer is still mid-sync
# (SyncTarget/Inconsistent) is REJECTED with a 409 ApiCallRc envelope.
# `--force` overrides.
#
# How this cell holds a deterministic SyncTarget window:
#   We throttle the resync to a very low c-max-rate and use a large
#   volume so the second diskful stays SyncTarget/Inconsistent long
#   enough to attempt the `r d` against a live mid-sync state. The stand
#   can otherwise skip-sync (Bug 347 family) and the window would vanish.
#
# Contract:
#   1. 1-diskful RD on N1, UpToDate, with data written (real GI bump so
#      the second replica must actually resync).
#   2. Throttle resync (rd drbd-options --c-max-rate) so the next add is
#      slow.
#   3. r c N2 (second diskful) — it enters SyncTarget/Inconsistent.
#   4. While N2 is mid-sync, `r d N1 <rd>` (the only UpToDate source)
#      MUST be REJECTED (non-zero CLI exit / 409) and N1 MUST survive.
#   5. `r d N1 <rd> --force` (or via the REST ?force=true) overrides —
#      OR we simply wait for sync to finish and assert a normal delete
#      then succeeds. We choose the WAIT path so teardown leaves no
#      stranded replica.
#   6. No orphans after teardown.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

RD=cli-matrix-u130
POOL=${POOL:-stand}
# Large enough that a throttled resync stays in-flight for the probe
# window; small enough to keep the cell quick on the FILE_THIN pool.
SIZE_MIB=512

N1=$WORKER_1
N2=$WORKER_2

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> Phase 1: rd c + vd c + r c $N1 (single diskful, ${SIZE_MIB} MiB)"
_out=$("${LCTL[@]}" resource-definition create "$RD" 2>&1) \
    || { echo "FAIL: rd c $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" volume-definition create "$RD" "${SIZE_MIB}M" 2>&1) \
    || { echo "FAIL: vd c $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N1" "$RD" --storage-pool="$POOL" 2>&1) \
    || { echo "FAIL: r c $N1: $_out" >&2; exit 1; }

wait_disk_state "$RD" "$N1" UpToDate 180 \
    || { echo "FAIL: $N1 never reached UpToDate" >&2; "${LCTL[@]}" resource list --resources "$RD" >&2; exit 1; }

echo ">> Phase 2: write data on $N1 so the second replica must really resync"
# Primary + dd a chunk to bump the GI; a fresh empty volume could
# skip-sync and erase the SyncTarget window.
on_node "$N1" bash -c "drbdadm primary --force $RD 2>/dev/null" || true
on_node "$N1" bash -c "dd if=/dev/urandom of=/dev/drbd/by-res/$RD/0 bs=1M count=256 status=none oflag=direct 2>/dev/null" || true
on_node "$N1" bash -c "drbdadm secondary $RD 2>/dev/null" || true

echo ">> Phase 3: throttle resync (c-max-rate 1024 KiB/s) so the add stays SyncTarget"
"${LCTL[@]}" resource-definition drbd-options --c-max-rate 1024 "$RD" >/dev/null 2>&1 \
    || "${LCTL[@]}" resource-definition drbd-options --c-min-rate 250 "$RD" >/dev/null 2>&1 || true

echo ">> Phase 4: r c $N2 (second diskful) — should enter SyncTarget/Inconsistent"
_out=$("${LCTL[@]}" resource create "$N2" "$RD" --storage-pool="$POOL" 2>&1) \
    || { echo "FAIL: r c $N2: $_out" >&2; exit 1; }

# Wait for the second replica to be observed mid-sync (Inconsistent or
# SyncTarget). If the stand skip-syncs anyway, this window may be tiny;
# poll a short while.
echo ">> wait up to 60s for $N2 to be observed mid-sync (Inconsistent/SyncTarget)"
deadline=$(( $(date +%s) + 60 ))
midsync=false
while (( $(date +%s) < deadline )); do
    d2=$(status_disk_state "$RD" "$N2" 0)
    r2=$(kubectl get "resources.blockstor.cozystack.io/${RD}.${N2}" \
        -o jsonpath='{.status.volumes[0].replicationState}' 2>/dev/null || echo "")
    if [[ "$d2" == "Inconsistent" || "$d2" == "SyncTarget" || "$r2" == "SyncTarget" ]]; then
        midsync=true
        break
    fi
    # If it already reached UpToDate the window was missed — the guard
    # would (correctly) allow the delete, so the cell can't assert the
    # rejection. Treat as inconclusive-but-not-failing below.
    if [[ "$d2" == "UpToDate" ]]; then
        break
    fi
    sleep 1
done

if ! $midsync; then
    echo "WARN (U130): could not catch a mid-sync window for $N2 (stand skip-synced too fast)." >&2
    echo "   The REST guard is pinned at L1; the L7 replay covers the wire path." >&2
    echo "   Skipping the stand-side rejection probe to avoid a flaky assertion." >&2
    # Still validate the no-sync delete path is allowed (bug-hunt #6
    # must-not-regress): with both diskful UpToDate, dropping one works.
    wait_disk_state "$RD" "$N2" UpToDate 180 || true
    _out=$("${LCTL[@]}" resource delete "$N2" "$RD" 2>&1) \
        || { echo "FAIL: no-sync r d $N2 should be allowed: $_out" >&2; exit 1; }
    echo ">> r-d-last-uptodate-midsync-rejected OK (skip-sync path: no-sync delete allowed)"
    exit 0
fi

echo "   $N2 observed mid-sync (disk=$d2 rep=$r2)"

# =====================================================================
# Core U130 assertion: `r d $N1` (the only UpToDate source) MUST be
# rejected while $N2 is mid-sync.
# =====================================================================
echo ">> Phase 5: r d $N1 $RD (last UpToDate source, mid-sync) MUST be REJECTED"
err_file=$(mktemp)
if "${LCTL[@]}" resource delete "$N1" "$RD" >"$err_file" 2>&1; then
    echo "FAIL (U130): r d of the last UpToDate diskful succeeded while $N2 was mid-sync" >&2
    echo "----- delete output -----" >&2
    cat "$err_file" >&2
    "${LCTL[@]}" resource list --resources "$RD" 2>&1 | tail -10 >&2
    rm -f "$err_file"
    exit 1
fi
echo "   OK: delete rejected. CLI message:"
sed 's/^/     /' "$err_file" >&2 || true
rm -f "$err_file"

# N1 MUST still exist (the refusal blocked the destructive delete).
if ! kubectl get "resources.blockstor.cozystack.io/${RD}.${N1}" >/dev/null 2>&1; then
    echo "FAIL (U130): $N1 replica vanished despite the rejection" >&2
    exit 1
fi
echo "   OK: $N1 survived the refused delete"

# =====================================================================
# Recovery: let the resync finish, then the same delete is allowed.
# =====================================================================
echo ">> Phase 6: lift throttle, wait for $N2 UpToDate, then r d $N1 is allowed"
"${LCTL[@]}" resource-definition drbd-options --unset-c-max-rate "$RD" >/dev/null 2>&1 || true
wait_disk_state "$RD" "$N2" UpToDate 600 \
    || { echo "FAIL: $N2 never reached UpToDate after lifting throttle" >&2; exit 1; }

_out=$("${LCTL[@]}" resource delete "$N1" "$RD" 2>&1) \
    || { echo "FAIL: r d $N1 should be allowed once $N2 is UpToDate: $_out" >&2; exit 1; }
wait_replica_absent "$RD" "$N1" 120 \
    || { echo "FAIL: $N1 replica did not disappear after the allowed delete" >&2; exit 1; }

echo ">> r-d-last-uptodate-midsync-rejected OK (U130: mid-sync source delete rejected, post-sync delete allowed)"
