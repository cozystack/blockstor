#!/usr/bin/env bash
#
# usage: vd-resize-with-diskless-peer.sh WORK_DIR
#
# L6 cli-matrix cell — U48 (P1) resize-with-a-diskless-peer-present.
#
# Upstream LINSTOR bug (issue-mining U48): a `vd set-size` grow fanned
# `drbdadm resize` out to a DISKLESS replica too. The kernel rejects a
# resize on a node with no local disk ("requires a local disk"), the
# satellite errors, and the whole volume wedges mid-resize — the diskful
# peers never get their grow either.
#
# blockstor's gate is structural: the satellite skips storage + the
# downstream `drbdadm resize` for any diskless replica (resized=false in
# applyStorageIfDiskful → the `if resized { Adm.Resize() }` block in
# finishDRBDApply is unreachable on diskless). This cell proves it at the
# operator-CLI level on the live stand:
#
#   1. rd c + vd c 1G + r c --auto-place=2 -s lvm-thin  (2 diskful peers)
#   2. Add an EXPLICIT diskless replica on a 3rd node (`r c -d`).
#   3. Wait both diskful UpToDate and the diskless peer Diskless+attached.
#   4. linstor vd s <rd> 0 2G  (grow).
#   5. Assert (≤180s):
#        - vd size == 2 GiB
#        - both DISKFUL replicas grew their backing LV to >= 2 GiB and
#          re-reached UpToDate (the resize actually landed end-to-end)
#        - the DISKLESS peer has NO backing LV (resize was NOT fanned to
#          it — the U48 footgun) and its diskState is Diskless / UpToDate,
#          never Failed / Inconsistent (it did not wedge)
#        - the resource as a whole has no Failed volume row
#   6. Cleanup RD; assert_no_orphans.
#
# If U48 regressed, the diskless peer's satellite would have shelled out
# `drbdadm resize` and either left a "requires a local disk" error in the
# Resource status or wedged the diskful grow — both caught below.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

POOL="${POOL:-lvm-thin}"
RD="cli-matrix-resize-diskless-peer"
SIZE_2G_KIB=2097152

cleanup() {
    [[ -n "${RD:-}" ]] && delete_rd "$RD"
    [[ -n "${RD:-}" ]] && assert_no_orphans "$RD"
}
trap 'cleanup; linstor_cli_teardown' EXIT

echo "============================================================"
echo ">> vd-resize-with-diskless-peer (U48) :: POOL=$POOL RD=$RD"
echo "============================================================"

echo ">> pre-flight: $POOL on >=2 nodes + a 3rd worker for the diskless peer"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 2 )); then
    echo "SKIP ($POOL): pool not on >=2 nodes (got $ok_nodes)"
    exit 0
fi

echo ">> rd c + vd c 1G + r c --auto-place=2 -s $POOL"
_out=$("${LCTL[@]}" resource-definition create "$RD" 2>&1) \
    || { echo "FAIL: rd c $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" volume-definition create "$RD" 1G 2>&1) \
    || { echo "FAIL: vd c $RD 1G: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create --auto-place=2 --storage-pool="$POOL" "$RD" 2>&1) \
    || { echo "FAIL: r c --auto-place=2 -s $POOL $RD: $_out" >&2; exit 1; }

# Resolve the two diskful replicas (exclude DISKLESS / TIE_BREAKER).
deadline=$(( $(date +%s) + 90 ))
diskful=()
while (( $(date +%s) < deadline )); do
    mapfile -t diskful < <(linstor_diskful_nodes "$RD")
    if (( ${#diskful[@]} >= 2 )); then break; fi
    sleep 2
done
if (( ${#diskful[@]} < 2 )); then
    echo "FAIL: autoplace did not stage 2 diskful replicas (got ${#diskful[@]})" >&2
    exit 1
fi
N1="${diskful[0]}"
N2="${diskful[1]}"
echo ">> diskful replicas: N1=$N1 N2=$N2"
wait_uptodate "$RD" "$N1" "$N2"

# Pick a 3rd worker that does NOT already hold a replica for the explicit
# diskless peer. If autoplace already left a TieBreaker there, reuse that
# node (a TieBreaker is itself a diskless replica — perfect for U48).
DLNODE="$(linstor_tiebreaker_node "$RD" || true)"
if [[ -z "$DLNODE" ]]; then
    DLNODE="$(linstor_pick_free_node "$RD" "$N1" "$N2" || true)"
    if [[ -z "$DLNODE" ]]; then
        echo "SKIP: no free 3rd worker for the diskless peer" >&2
        exit 0
    fi
    echo ">> adding explicit diskless replica on $DLNODE"
    _out=$("${LCTL[@]}" resource create --drbd-diskless "$RD" "$DLNODE" 2>&1) \
        || { echo "FAIL: r c --drbd-diskless $RD $DLNODE: $_out" >&2; exit 1; }
else
    echo ">> reusing auto-placed TieBreaker on $DLNODE as the diskless peer"
fi

# The diskless peer should converge to a Diskless (or UpToDate-diskless)
# disk state, NOT Failed. Accept either Diskless or UpToDate — a diskless
# DRBD replica that has connected reads UpToDate from peers.
echo ">> wait diskless peer $DLNODE attaches (Diskless/UpToDate, not Failed)"
dl_deadline=$(( $(date +%s) + 120 ))
dl_state=""
while (( $(date +%s) < dl_deadline )); do
    dl_state="$(status_disk_state "$RD" "$DLNODE" 0)"
    case "$dl_state" in
        Diskless|UpToDate) break ;;
    esac
    sleep 3
done
case "$dl_state" in
    Diskless|UpToDate) echo "   diskless peer state=$dl_state OK" ;;
    *)
        echo "FAIL: diskless peer $DLNODE never reached Diskless/UpToDate (last=$dl_state)" >&2
        exit 1
        ;;
esac

# Sanity: the diskless peer must have NO backing LV before the grow.
dl_lv_before=$(on_node "$DLNODE" bash -c \
    "lvs --noheadings -o lv_name 2>/dev/null | awk '/${RD}_00000/{print \$1}' | head -1" 2>/dev/null || true)
if [[ -n "$dl_lv_before" ]]; then
    echo "FAIL: diskless peer $DLNODE already has a backing LV '$dl_lv_before' (not actually diskless?)" >&2
    exit 1
fi

# ---- Grow 1G → 2G with the diskless peer present --------------------------
echo ">> linstor vd s $RD 0 2G (grow with diskless peer on $DLNODE present)"
"${LCTL[@]}" volume-definition set-size "$RD" 0 2G >/dev/null

echo ">> assert vd size == 2 GiB"
wait_vd_size "$RD" 0 "$SIZE_2G_KIB" 60

echo ">> assert BOTH diskful replicas grew their backing LV to >= 2 GiB"
for node in "$N1" "$N2"; do
    grow_deadline=$(( $(date +%s) + 120 ))
    lv_kib=0
    while (( $(date +%s) < grow_deadline )); do
        lv_kib=$(on_node "$node" bash -c "
            lvs --units k --nosuffix --noheadings -o lv_name,lv_size 2>/dev/null \
                | awk '/${RD}_00000/{gsub(/\..*/,\"\",\$2); print \$2; exit}'
        " 2>/dev/null || echo 0)
        [[ -z "$lv_kib" ]] && lv_kib=0
        if (( lv_kib >= SIZE_2G_KIB )); then break; fi
        sleep 3
    done
    if (( lv_kib < SIZE_2G_KIB )); then
        echo "FAIL: diskful $node backing LV did not grow to >= 2 GiB (got ${lv_kib} KiB)" >&2
        echo "      (U48 regression suspected: the resize wedged because it was" >&2
        echo "       fanned out to the diskless peer $DLNODE)" >&2
        exit 1
    fi
    echo "   $node backing LV = ${lv_kib} KiB (>= 2 GiB) OK"
done

echo ">> assert both diskful replicas re-reached UpToDate after grow"
wait_uptodate "$RD" "$N1" "$N2"

echo ">> assert the DISKLESS peer $DLNODE still has NO backing LV (resize NOT fanned to it)"
dl_lv_after=$(on_node "$DLNODE" bash -c \
    "lvs --noheadings -o lv_name 2>/dev/null | awk '/${RD}_00000/{print \$1}' | head -1" 2>/dev/null || true)
if [[ -n "$dl_lv_after" ]]; then
    echo "FAIL (U48): a backing LV '$dl_lv_after' appeared on the diskless peer $DLNODE" >&2
    echo "      after the grow — the resize path provisioned/resized storage on a" >&2
    echo "      diskless replica (exactly the upstream footgun)." >&2
    exit 1
fi
echo "   diskless peer $DLNODE has no backing LV — resize correctly skipped it"

echo ">> assert the diskless peer did not wedge (not Failed/Inconsistent)"
dl_state_post="$(status_disk_state "$RD" "$DLNODE" 0)"
case "$dl_state_post" in
    Diskless|UpToDate) echo "   diskless peer post-grow state=$dl_state_post OK" ;;
    *)
        echo "FAIL (U48): diskless peer $DLNODE post-grow state=$dl_state_post" >&2
        echo "      (\"requires a local disk\" wedge — resize was fanned to diskless)" >&2
        exit 1
        ;;
esac

echo ">> vd-resize-with-diskless-peer (U48) OK"
cleanup
trap 'linstor_cli_teardown' EXIT
echo ">> vd-resize-with-diskless-peer COMPLETE"
