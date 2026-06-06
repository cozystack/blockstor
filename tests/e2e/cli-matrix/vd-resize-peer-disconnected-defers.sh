#!/usr/bin/env bash
#
# usage: vd-resize-peer-disconnected-defers.sh WORK_DIR
#
# L6 cli-matrix cell — U204/U388 (P2) resize-while-a-peer-is-unhealthy.
#
# Upstream LINSTOR reports: issuing `vd set-size` while one replica is
# Inconsistent / disconnected could wedge the volume — the grow stalled
# waiting on the unreachable peer and never completed on the healthy one,
# or left the resource in a half-resized limbo.
#
# blockstor's resize path has NO peer-health gate at REST by design: a
# DRBD grow with a disconnected peer is safe — the reachable diskful
# peers extend their backing LV + `drbdadm resize` the kernel device
# immediately, and DRBD marks the grown region out-of-sync so the
# disconnected peer RESYNCS it on reconnect (the grow is "deferred" for
# that peer, never wedged). This cell pins that contract on the live
# stand using a real network partition (iptables DROP on the DRBD port,
# with a trap that always flushes the rules — NOT a satellite stop, so
# the satellite keeps reconciling throughout):
#
#   1. rd c + vd c 1G + r c node1 + r c node2 (2 diskful). Wait UpToDate.
#   2. Partition N2 from N1 (drop tcp/<drbd-port> in+out on N2). Wait
#      until N1 sees the connection drop (peer not Established).
#   3. linstor vd s <rd> 0 2G WHILE N2 is partitioned.
#   4. Assert the resize does not wedge:
#        - the REST call returned 0
#        - vd l SizeKib == 2 GiB (the controller committed the size)
#        - N1 (reachable) grew its backing LV to >= 2 GiB and is still
#          UpToDate (the healthy peer resized without waiting on N2)
#   5. Heal the partition (flush iptables). Assert N2 catches up:
#        - N2's backing LV grows to >= 2 GiB
#        - both peers reconverge to UpToDate (N2 resynced the grown
#          region — the deferred grow completed)
#   6. Cleanup RD; assert_no_orphans.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

linstor_cli_setup

POOL="${POOL:-lvm-thin}"
RD="cli-matrix-resize-peer-disco"
SIZE_2G_KIB=2097152

# Set by the placement step; the trap reads them defensively.
N1=""
N2=""
DRBD_PORT=""

cleanup() {
    # ALWAYS flush the iptables rules on N2 first so a failure mid-test
    # never leaves the stand partitioned for the next cell.
    if [[ -n "$N2" && -n "$DRBD_PORT" ]]; then
        on_node "$N2" iptables -D INPUT  -p tcp --dport "$DRBD_PORT" -j DROP 2>/dev/null || true
        on_node "$N2" iptables -D OUTPUT -p tcp --dport "$DRBD_PORT" -j DROP 2>/dev/null || true
        on_node "$N2" iptables -D INPUT  -p tcp --sport "$DRBD_PORT" -j DROP 2>/dev/null || true
        on_node "$N2" iptables -D OUTPUT -p tcp --sport "$DRBD_PORT" -j DROP 2>/dev/null || true
    fi
    [[ -n "${RD:-}" ]] && delete_rd "$RD"
    [[ -n "${RD:-}" ]] && assert_no_orphans "$RD"
}
trap 'cleanup; linstor_cli_teardown' EXIT

echo "============================================================"
echo ">> vd-resize-peer-disconnected-defers (U204/U388) :: POOL=$POOL RD=$RD"
echo "============================================================"

echo ">> pre-flight: $POOL on >=2 nodes"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
mapfile -t pool_nodes < <(jq -r '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique | .[]' <<<"$sp_json" 2>/dev/null || true)
if (( ${#pool_nodes[@]} < 2 )); then
    echo "SKIP ($POOL): pool not on >=2 nodes (got ${#pool_nodes[@]})"
    exit 0
fi
N1="${pool_nodes[0]}"
N2="${pool_nodes[1]}"
echo ">> diskful replicas: N1=$N1 (kept) N2=$N2 (will be partitioned)"

echo ">> rd c + vd c 1G + r c $N1 + r c $N2"
_out=$("${LCTL[@]}" resource-definition create "$RD" 2>&1) \
    || { echo "FAIL: rd c $RD: $_out" >&2; exit 1; }
# Disable auto-tiebreaker so we keep exactly the 2 diskful peers.
"${LCTL[@]}" resource-definition set-property "$RD" DrbdOptions/AutoAddQuorumTiebreaker false >/dev/null 2>&1 || true
_out=$("${LCTL[@]}" volume-definition create "$RD" 1G 2>&1) \
    || { echo "FAIL: vd c $RD 1G: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N1" "$RD" --storage-pool="$POOL" 2>&1) \
    || { echo "FAIL: r c $N1 $RD: $_out" >&2; exit 1; }
_out=$("${LCTL[@]}" resource create "$N2" "$RD" --storage-pool="$POOL" 2>&1) \
    || { echo "FAIL: r c $N2 $RD: $_out" >&2; exit 1; }

wait_uptodate "$RD" "$N1" "$N2"

echo ">> discover DRBD port from the rendered .res on $N2"
DRBD_PORT=$(on_node "$N2" bash -c "grep -oE 'address.*:[0-9]+' /etc/drbd.d/${RD}.res | head -1 | grep -oE '[0-9]+$'" 2>/dev/null || true)
if [[ -z "$DRBD_PORT" ]]; then
    echo "FAIL: could not parse DRBD port from ${RD}.res on $N2" >&2
    exit 1
fi
echo "   DRBD_PORT=$DRBD_PORT"

echo ">> partition $N2 from peers (drop tcp/$DRBD_PORT in+out, both dport+sport)"
on_node "$N2" iptables -A INPUT  -p tcp --dport "$DRBD_PORT" -j DROP
on_node "$N2" iptables -A OUTPUT -p tcp --dport "$DRBD_PORT" -j DROP
on_node "$N2" iptables -A INPUT  -p tcp --sport "$DRBD_PORT" -j DROP
on_node "$N2" iptables -A OUTPUT -p tcp --sport "$DRBD_PORT" -j DROP

echo ">> wait until $N1 no longer sees $N2 Established"
disco_deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < disco_deadline )); do
    if ! on_node "$N1" drbdsetup status "$RD" 2>/dev/null | grep -q 'connection:Connected'; then
        # Either the peer dropped to Connecting/NetworkFailure or the
        # whole connection row is gone — both mean "not Established".
        break
    fi
    sleep 2
done
echo "   $N1 view of $N2 connection: $(on_node "$N1" bash -c "drbdsetup status '${RD}' 2>/dev/null | grep -oE 'connection:[A-Za-z]+' | head -1" 2>/dev/null || echo '?')"

# ---- Grow 1G → 2G WHILE N2 is partitioned ---------------------------------
echo ">> linstor vd s $RD 0 2G (while $N2 partitioned — MUST NOT wedge)"
if ! "${LCTL[@]}" volume-definition set-size "$RD" 0 2G >/dev/null 2>&1; then
    echo "FAIL (U204/U388): vd s rejected/errored while a peer was disconnected" >&2
    echo "      blockstor must defer the disconnected peer, not reject the grow." >&2
    exit 1
fi

echo ">> 1. vd l SizeKib reaches 2 GiB"
wait_vd_size "$RD" 0 "$SIZE_2G_KIB" 60

echo ">> 2. reachable peer $N1 grew its backing LV to >= 2 GiB"
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
    echo "FAIL (U204/U388): reachable peer $N1 LV did not grow to >= 2 GiB (got ${lv_kib} KiB)" >&2
    echo "      The grow stalled waiting on the disconnected peer $N2 — wedge." >&2
    exit 1
fi
echo "   $N1 backing LV = ${lv_kib} KiB (>= 2 GiB) OK"

echo ">> 3. reachable peer $N1 still UpToDate during partition"
wait_disk_state "$RD" "$N1" "UpToDate" 120 0

# ---- Heal the partition; the deferred grow on N2 must complete -------------
echo ">> heal partition (flush iptables on $N2)"
on_node "$N2" iptables -D INPUT  -p tcp --dport "$DRBD_PORT" -j DROP 2>/dev/null || true
on_node "$N2" iptables -D OUTPUT -p tcp --dport "$DRBD_PORT" -j DROP 2>/dev/null || true
on_node "$N2" iptables -D INPUT  -p tcp --sport "$DRBD_PORT" -j DROP 2>/dev/null || true
on_node "$N2" iptables -D OUTPUT -p tcp --sport "$DRBD_PORT" -j DROP 2>/dev/null || true

echo ">> 4. formerly-partitioned peer $N2 grows its backing LV to >= 2 GiB"
lv2_deadline=$(( $(date +%s) + 180 ))
lv2_kib=0
while (( $(date +%s) < lv2_deadline )); do
    lv2_kib=$(on_node "$N2" bash -c "
        lvs --units k --nosuffix --noheadings -o lv_name,lv_size 2>/dev/null \
            | awk '/${RD}_00000/{gsub(/\..*/,\"\",\$2); print \$2; exit}'
    " 2>/dev/null || echo 0)
    [[ -z "$lv2_kib" ]] && lv2_kib=0
    if (( lv2_kib >= SIZE_2G_KIB )); then break; fi
    sleep 3
done
if (( lv2_kib < SIZE_2G_KIB )); then
    echo "FAIL (U204/U388): $N2 LV did not catch up to >= 2 GiB after heal (got ${lv2_kib} KiB)" >&2
    echo "      The deferred grow never completed on the reconnected peer." >&2
    exit 1
fi
echo "   $N2 backing LV = ${lv2_kib} KiB (>= 2 GiB) OK"

echo ">> 5. both peers reconverge to UpToDate (N2 resynced the grown region)"
wait_uptodate "$RD" "$N1" "$N2"

echo ">> vd-resize-peer-disconnected-defers (U204/U388) OK"
cleanup
trap 'linstor_cli_teardown' EXIT
echo ">> vd-resize-peer-disconnected-defers COMPLETE"
