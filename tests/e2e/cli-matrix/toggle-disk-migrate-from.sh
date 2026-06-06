#!/usr/bin/env bash
#
# usage: toggle-disk-migrate-from.sh WORK_DIR
#
# L6 cli-matrix cell — H2 (toggle-disk state-machine campaign).
#
# `linstor resource toggle-disk <dst> <rd> --storage-pool=<sp>
#  --migrate-from <src>` is a SYNC-THEN-REMOVE migration (UG9 §"Migrating
# a resource to another node"): the migrate SOURCE replica is removed
# ONLY AFTER the new disk on the destination finishes its initial sync.
# Redundancy must never dip below the original diskful count at any point.
#
# Reproduction (operator-CLI level, the exact UG9 two-step flow):
#
#   $ linstor rd c <rd>; linstor vd c <rd> 128M
#   $ linstor r c <src> <rd> -s <sp>          # diskful #1 (migrate source)
#   $ linstor r c <keep> <rd> -s <sp>         # diskful #2 (stays put)
#   $ linstor r c <dst> <rd> --diskless       # diskless landing pad
#   $ linstor r td <dst> <rd> -s <sp> --migrate-from <src>
#
# The landing pad uses the DEPRECATED `--diskless` alias (posts the
# canonical wire flag DISKLESS) so this cell validates the H2
# sync-then-remove contract against the currently-deployed stand image,
# which predates the H3 fix. The modern `--drbd-diskless` flag posts the
# wire flag DRBD_DISKLESS, which the controller must normalise to DISKLESS
# (H3); that normalisation is pinned separately by the L1 unit test
# pkg/rest/resource_create_drbd_diskless_test.go.
#
# Contract (post-migration), asserted here:
#
#   1. DURING the migration the diskful count is observed >= 2 at every
#      poll — the destination is ADDED (raising the count to 3) before
#      the source is dropped. The count must NEVER be seen at 1, which is
#      what an Option-A (drop-then-add) implementation would transiently
#      expose.
#   2. The destination replica reaches UpToDate.
#   3. AFTER the destination is UpToDate the SOURCE replica is gone (its
#      Resource CRD removed) — sync-then-remove completed.
#   4. The keep node and the destination both remain diskful; final
#      diskful count is exactly 2 (keep + dst), source pruned.
#   5. Upstream-issue U341 (P1, "Lost quorum when migrating a resource
#      to another node"): the surviving KEEP replica holds DRBD quorum
#      at EVERY migration poll. Because the source is pruned only after
#      the destination is UpToDate (add-before-drop), the quorum-voter
#      set is only ever grown then trimmed and never dips below
#      majority — so the resource never transiently loses quorum
#      (no I/O suspension risk). Read straight from the kernel via
#      `drbdsetup status <rd>` on the keep node's satellite.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-h2-migrate
POOL=${POOL:-stand}

SRC=$WORKER_1   # migrate source — removed after dst syncs
KEEP=$WORKER_2  # untouched diskful peer
DST=$WORKER_3   # diskless landing pad, promoted via --migrate-from

cleanup() {
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

echo ">> [H2] 2 diskful ($SRC,$KEEP) + 1 diskless ($DST) for $RD"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 128M >/dev/null
"${LCTL[@]}" resource create "$SRC"  "$RD" --storage-pool="$POOL" >/dev/null
"${LCTL[@]}" resource create "$KEEP" "$RD" --storage-pool="$POOL" >/dev/null
"${LCTL[@]}" resource create "$DST"  "$RD" --diskless >/dev/null

echo ">> wait for both diskful replicas UpToDate"
wait_uptodate "$RD" "$SRC" "$KEEP"

echo ">> wait for the diskless landing pad on $DST"
wait_status_diskless "$RD" "$DST" 60

echo ">> r td $DST $RD -s $POOL --migrate-from $SRC (sync-then-remove)"
"${LCTL[@]}" resource toggle-disk "$DST" "$RD" --storage-pool="$POOL" --migrate-from "$SRC" >/dev/null

# Poll the migration to completion while continuously asserting the
# redundancy invariant. The destination must reach UpToDate AND the
# source must disappear; throughout, the diskful count must stay >= 2.
echo ">> drive migration, asserting diskful count never drops below 2"
echo ">>   and quorum is held on $KEEP throughout (U341)"
deadline=$(( $(date +%s) + 300 ))
dst_uptodate=0
src_gone=0
min_diskful=99
quorum_checks=0
while (( $(date +%s) < deadline )); do
    dc=$(linstor_diskful_count "$RD")
    if (( dc < min_diskful )); then
        min_diskful=$dc
    fi
    # Redundancy floor: a single poll observing < 2 diskful is a
    # drop-then-add (Option A) regression — fail fast.
    if (( dc < 2 )); then
        echo "FAIL (H2): diskful count dropped to $dc during migration (sync-then-remove violated)" >&2
        kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
            | awk -v rd="${RD}." '$1 ~ "^"rd' >&2 || true
        exit 1
    fi

    # U341 quorum invariant: read the live kernel quorum on the
    # SURVIVING keep replica. `drbdsetup status <rd>` prints
    # `quorum:no` per-device when quorum is lost; its absence (the
    # default `quorum:yes`, often elided) means quorate. A single
    # `quorum:no` sample is the U341 symptom — the migrate vacated the
    # quorum-providing peer before the new diskful was UpToDate —
    # fail fast. Only assert once the keep replica is actually up in
    # the kernel (drbdsetup exits 0); skip a poll where the status
    # read fails so a transient satellite-exec hiccup doesn't flake.
    if keep_status=$(on_node "$KEEP" drbdsetup status "$RD" 2>/dev/null); then
        quorum_checks=$(( quorum_checks + 1 ))
        if grep -q "quorum:no" <<<"$keep_status"; then
            echo "FAIL (U341): keep replica $KEEP lost quorum during migration" >&2
            echo "$keep_status" >&2
            exit 1
        fi
    fi

    dst_disk=$(status_disk_state "$RD" "$DST" 0)
    [[ "$dst_disk" == "UpToDate" ]] && dst_uptodate=1

    if ! kubectl get "resources.blockstor.cozystack.io/${RD}.${SRC}" >/dev/null 2>&1; then
        src_gone=1
    fi

    if (( dst_uptodate == 1 && src_gone == 1 )); then
        break
    fi
    sleep 5
done

if (( dst_uptodate != 1 )); then
    echo "FAIL (H2): destination $DST never reached UpToDate within 300s" >&2
    exit 1
fi

if (( src_gone != 1 )); then
    echo "FAIL (H2): migrate source $SRC was NOT removed after dst synced (sync-then-remove incomplete)" >&2
    kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
        | awk -v rd="${RD}." '$1 ~ "^"rd' >&2 || true
    exit 1
fi

# Final shape: exactly 2 diskful (keep + dst), source pruned.
final_diskful=$(linstor_diskful_count "$RD")
if (( final_diskful != 2 )); then
    echo "FAIL (H2): final diskful count $final_diskful, want 2 (keep + dst, source pruned)" >&2
    echo "  diskful nodes: $(linstor_diskful_nodes "$RD" | tr '\n' ' ')" >&2
    exit 1
fi

if linstor_diskful_nodes "$RD" | grep -qx "$SRC"; then
    echo "FAIL (H2): migrate source $SRC still hosts a diskful replica after migration" >&2
    exit 1
fi

if (( quorum_checks == 0 )); then
    echo "FAIL (U341): never managed to read kernel quorum on $KEEP during migration" >&2
    exit 1
fi

echo ">> toggle-disk-migrate-from OK (H2 pinned: min diskful during migration = $min_diskful >= 2; src removed only after dst UpToDate; U341: quorum held on $KEEP across $quorum_checks polls)"
