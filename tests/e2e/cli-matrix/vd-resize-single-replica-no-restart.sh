#!/usr/bin/env bash
#
# usage: vd-resize-single-replica-no-restart.sh WORK_DIR
#
# L6 cli-matrix cell — U329 (P1) resize-converges-without-restarts.
#
# Upstream LINSTOR bug (issue-mining U329): a `vd set-size` grow only
# took effect after a satellite/controller restart — the live reconcile
# path missed the size change, so operators saw the VD size update but
# the backing LV + DRBD device stayed at the old size until something
# bounced the satellite. CSI online-resize then hung.
#
# This cell pins the no-restart contract on the simplest possible
# topology — a SINGLE diskful replica (no peer to mask a missed local
# reconcile, no resync to hide behind):
#
#   1. rd c + vd c 1G + r c (single diskful replica on one node).
#   2. Wait UpToDate. Snapshot the satellite pod's restartCount on that
#      node BEFORE the grow.
#   3. linstor vd s <rd> 0 2G.
#   4. Assert WITHOUT touching any pod (no rollout, no delete):
#        - linstor vd l SizeKib == 2 GiB
#        - the backing LV grew to >= 2 GiB (lvs ground truth)
#        - the kernel DRBD device grew (drbdsetup status / blockdev)
#        - the replica is still UpToDate
#   5. Assert the satellite pod restartCount is UNCHANGED — the grow
#      converged purely through the live reconcile loop, not a bounce.
#   6. Cleanup RD; assert_no_orphans.
#
# If U329 regressed, step 4 would time out (size never propagates to the
# LV/kernel without a restart) OR step 5 would catch an unexpected pod
# restart papering over the missed reconcile.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

linstor_cli_setup

POOL="${POOL:-lvm-thin}"
RD="cli-matrix-resize-single-norestart"
SIZE_2G_KIB=2097152
# Satellite DaemonSet/pod selector + namespace. Default matches the
# stand's blockstor-system deploy; override if the launcher renamed it.
SAT_NS="${SAT_NS:-blockstor-system}"
SAT_LABEL="${SAT_LABEL:-app=blockstor-satellite}"

cleanup() {
    [[ -n "${RD:-}" ]] && delete_rd "$RD"
    [[ -n "${RD:-}" ]] && assert_no_orphans "$RD"
}
trap 'cleanup; linstor_cli_teardown' EXIT

echo "============================================================"
echo ">> vd-resize-single-replica-no-restart (U329) :: POOL=$POOL RD=$RD"
echo "============================================================"

echo ">> pre-flight: $POOL on >=1 node"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
ok_nodes=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( ok_nodes < 1 )); then
    echo "SKIP ($POOL): pool not on any node (got $ok_nodes)"
    exit 0
fi
N1=$(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | first // ""' <<<"$sp_json" 2>/dev/null || echo "")
if [[ -z "$N1" ]]; then
    echo "SKIP ($POOL): could not resolve a diskful node"
    exit 0
fi
echo ">> target node: $N1"

echo ">> rd c + vd c 1G + r c $N1 (single diskful replica)"
_out=$("${LCTL[@]}" resource-definition create "$RD" 2>&1) \
    || { echo "FAIL: rd c $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" volume-definition create "$RD" 1G 2>&1) \
    || { echo "FAIL: vd c $RD 1G: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N1" "$RD" --storage-pool="$POOL" 2>&1) \
    || { echo "FAIL: r c $N1 $RD: $_out" >&2; exit 1; }

# Single replica reaches UpToDate from its own day0 GI seed (no peer).
echo ">> wait single replica UpToDate on $N1"
wait_disk_state "$RD" "$N1" "UpToDate" 240 0

# Snapshot the satellite pod restartCount on $N1 BEFORE the grow.
sat_pod=$(kubectl -n "$SAT_NS" get pod -l "$SAT_LABEL" \
    --field-selector "spec.nodeName=$N1" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [[ -z "$sat_pod" ]]; then
    echo "SKIP: could not resolve satellite pod on $N1 (ns=$SAT_NS label=$SAT_LABEL)"
    exit 0
fi
restarts_before=$(kubectl -n "$SAT_NS" get pod "$sat_pod" \
    -o jsonpath='{.status.containerStatuses[*].restartCount}' 2>/dev/null || echo "")
echo ">> satellite pod=$sat_pod restartCount(before)='$restarts_before'"

# ---- Grow 1G → 2G, no restarts allowed ------------------------------------
echo ">> linstor vd s $RD 0 2G"
"${LCTL[@]}" volume-definition set-size "$RD" 0 2G >/dev/null

echo ">> 1. vd l SizeKib reaches 2 GiB"
wait_vd_size "$RD" 0 "$SIZE_2G_KIB" 60

echo ">> 2. backing LV grew to >= 2 GiB on $N1 (live reconcile, no bounce)"
lv_deadline=$(( $(date +%s) + 120 ))
lv_kib=0
while (( $(date +%s) < lv_deadline )); do
    lv_kib=$(on_node "$N1" bash -c "
        lvs --units k --nosuffix --noheadings -o lv_name,lv_size 2>/dev/null \
            | awk '/${RD}_00000/{gsub(/\..*/,\"\",\$2); print \$2; exit}'
    " 2>/dev/null || echo 0)
    [[ -z "$lv_kib" ]] && lv_kib=0
    if (( lv_kib >= SIZE_2G_KIB )); then break; fi
    sleep 3
done
if (( lv_kib < SIZE_2G_KIB )); then
    echo "FAIL (U329): backing LV on $N1 did not grow to >= 2 GiB (got ${lv_kib} KiB)" >&2
    echo "      The size change never reached the LV via the live reconcile loop." >&2
    exit 1
fi
echo "   backing LV = ${lv_kib} KiB (>= 2 GiB) OK"

echo ">> 3. kernel DRBD device grew on $N1"
dev=$(device_for_rd "$RD" "$N1" || true)
if [[ -n "$dev" ]]; then
    drbd_deadline=$(( $(date +%s) + 60 ))
    dev_kib=0
    while (( $(date +%s) < drbd_deadline )); do
        # blockdev --getsize64 reports bytes; convert to KiB.
        bytes=$(on_node "$N1" bash -c "blockdev --getsize64 '${dev}' 2>/dev/null || echo 0" 2>/dev/null || echo 0)
        dev_kib=$(( ${bytes:-0} / 1024 ))
        if (( dev_kib >= SIZE_2G_KIB )); then break; fi
        sleep 3
    done
    if (( dev_kib < SIZE_2G_KIB )); then
        echo "FAIL (U329): kernel DRBD device ${dev} on $N1 is ${dev_kib} KiB, want >= 2 GiB" >&2
        echo "      drbdadm resize did not run / did not re-probe the grown disk." >&2
        exit 1
    fi
    echo "   kernel device ${dev} = ${dev_kib} KiB (>= 2 GiB) OK"
else
    echo "   WARN: could not resolve /dev/drbdN for $RD on $N1 — skipping kernel-size check"
fi

echo ">> 4. replica still UpToDate after grow"
wait_disk_state "$RD" "$N1" "UpToDate" 120 0

# ---- The no-restart assertion ---------------------------------------------
restarts_after=$(kubectl -n "$SAT_NS" get pod "$sat_pod" \
    -o jsonpath='{.status.containerStatuses[*].restartCount}' 2>/dev/null || echo "")
echo ">> satellite pod=$sat_pod restartCount(after)='$restarts_after'"
if [[ "$restarts_after" != "$restarts_before" ]]; then
    echo "FAIL (U329): satellite pod $sat_pod restartCount changed during the grow" >&2
    echo "      before='$restarts_before' after='$restarts_after' — the resize must" >&2
    echo "      converge through the LIVE reconcile loop, never via a restart." >&2
    exit 1
fi
# Also confirm the pod was not recreated (same name/uid).
sat_pod_now=$(kubectl -n "$SAT_NS" get pod -l "$SAT_LABEL" \
    --field-selector "spec.nodeName=$N1" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [[ "$sat_pod_now" != "$sat_pod" ]]; then
    echo "FAIL (U329): satellite pod on $N1 was recreated during the grow" >&2
    echo "      before='$sat_pod' now='$sat_pod_now'" >&2
    exit 1
fi
echo "   satellite pod restartCount unchanged + pod not recreated — no-restart OK"

echo ">> vd-resize-single-replica-no-restart (U329) OK"
cleanup
trap 'linstor_cli_teardown' EXIT
echo ">> vd-resize-single-replica-no-restart COMPLETE"
