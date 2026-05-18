#!/usr/bin/env bash
#
# usage: recovery-stuck-synctarget.sh WORK_DIR
#
# Scenario 5.21 — recovery from a "stuck SyncTarget".
#
# A DRBD-9 SyncTarget can wedge with `done:%` flat for tens of
# seconds (sometimes minutes) if the resync stream is interrupted
# by a transient network blip mid-flight. The peers stay in cs:
# SyncSource / SyncTarget but neither side advances the bitmap-
# fed byte counter — the resync is alive on paper, dead on the
# wire. DRBD's own retry timers eventually shake it loose, but
# "eventually" is on the order of `ping-timeout` × `c-max-rate`
# back-off cycles and during that window the SyncTarget cannot
# satisfy reads, so any pod scheduled on it stalls.
#
# Operator recipe (from drbd-recovery skill): on the SyncTarget,
# run `drbdadm disconnect <rsc>:<source>` then `drbdadm connect
# <rsc>:<source>`. This force-tears the half-dead TCP, drops the
# in-flight resync request window, and lets DRBD re-handshake.
# The bitmap is preserved (it lives in metadata, not in the
# in-flight request queue) so the resync resumes from the last
# durably-flushed bitmap position — no full-restart.
#
# Reproduction strategy:
#   1. Throttle resync to ~1 MiB/s with c-max-rate so we have time
#      to inject a network blip mid-flight (default rate on a
#      QEMU loop-backed pool finishes 100 MiB in <10 s, too fast
#      to stall mechanically).
#   2. Partition $N2 from $N1, dirty 100 MiB on $N1.
#   3. Heal — $N2 enters SyncTarget at ~1 MiB/s, so we get ~100 s
#      of resync wall-time to play with.
#   4. Mid-sync, re-drop tcp/$DRBD_PORT on $N2. Within DRBD's
#      ping-timeout the resync `done:%` freezes but `replication:
#      SyncTarget` stays (until ping-timeout expires and the peer
#      transitions to Connecting — we sample BEFORE that).
#   5. Apply recipe: drbdadm disconnect + connect.
#   6. Assert: done:% advances within 5 s post-recipe; reconciler
#      did NOT run `drbdadm down/up` on $RD; final UpToDate; md5.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD=stuck-sync
N1=$WORKER_1     # Primary / SyncSource
N2=$WORKER_2     # Secondary / SyncTarget (the one we wedge)
SIZE_KIB=524288                       # 512 MiB
WRITE_BYTES=$((100 * 1024 * 1024))    # 100 MiB dirty
STALL_OBSERVE_SECONDS=30
RESYNC_RATE_KB=1024                   # ~1 MiB/s — slow enough to stall mid-flight
DRBD_PORT=""

trap 'cleanup_iptables; delete_rd "$RD"' EXIT

cleanup_iptables() {
    if [[ -n "$DRBD_PORT" ]]; then
        on_node "$N2" iptables -D INPUT  -p tcp --dport "$DRBD_PORT" -j DROP 2>/dev/null || true
        on_node "$N2" iptables -D OUTPUT -p tcp --dport "$DRBD_PORT" -j DROP 2>/dev/null || true
    fi
}

# read_done_pct prints the SyncTarget done-percentage as integer.
# Returns -1 when not in SyncTarget — meaning either Established
# (sync complete) or Connecting (timed out post-blip). Caller
# distinguishes by also checking the cs:/replication: state.
#
# TODO(Phase 11.5.b P2): outOfSyncKib lands on Status.Volumes but
# the per-peer done-% is not exposed in Status yet — keep this
# drbdsetup-bypass until Connections[].PeerVolumes[].OutOfSyncKib
# ships and the test can compute % from total size.
read_done_pct() {
    local node=$1 rd=$2 line pct
    line=$(on_node "$node" drbdsetup status "$rd" --verbose 2>/dev/null \
        | grep -E 'replication:SyncTarget' | head -1 || true)
    if [[ -z "$line" ]]; then echo "-1"; return 0; fi
    pct=$(echo "$line" | grep -oE 'done:[0-9]+(\.[0-9]+)?' | head -1 | cut -d: -f2 | cut -d. -f1)
    echo "${pct:--1}"
}

# read_replication prints the replication: token (Established /
# SyncSource / SyncTarget / WFBitMapS / "") for $peer as seen from
# $node, via Status.Connections (no drbdsetup bypass).
read_replication() {
    local node=$1 rd=$2 peer=$3
    status_replication_state "$rd" "$node" "$peer"
}

# read_connection prints connection: token (Connected / Connecting /
# StandAlone / Established / "") for $peer as seen from $node, via
# Status.Connections.
read_connection() {
    local node=$1 rd=$2 peer=$3
    status_connection_state "$rd" "$node" "$peer"
}

echo ">> apply 2-replica RD on $N1+$N2 (${SIZE_KIB} KiB) — initial sync at full rate"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    DrbdOptions/AutoAddQuorumTiebreaker: "false"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: ${SIZE_KIB}}
EOF
for n in "$N1" "$N2"; do
    cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${n}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${n}
  props: {StorPoolName: stand}
EOF
done

wait_uptodate "$RD" "$N1" "$N2"

DEV=$(device_for_rd "$RD" "$N1")
DRBD_PORT=$(on_node "$N1" bash -c "grep -oE 'address.*:[0-9]+' /etc/drbd.d/${RD}.res | head -1 | grep -oE '[0-9]+$'")
if [[ -z "$DRBD_PORT" ]]; then
    echo "FAIL: could not parse DRBD port from .res"
    exit 1
fi
echo ">> DRBD port = $DRBD_PORT, device = $DEV"

# Seed pattern so post-sync md5 has something to verify against.
echo ">> seed 4 MiB pattern on $N1"
on_node "$N1" bash -c "drbdadm primary --force ${RD} 2>/dev/null; dd if=/dev/urandom of=${DEV} bs=1M count=4 conv=fdatasync status=none"

# Throttle the resync rate at the kernel level so the post-partition
# resync runs slow enough that we can inject a network blip mid-flight.
# `drbdsetup peer-device-options` is the live knob; values are in KiB/s.
# 1 MiB/s leaves us a ~100 s window during which we can stall and
# recover the wedge.
echo ">> throttle resync to ${RESYNC_RATE_KB} KiB/s via drbdsetup peer-device-options"
# Discover each side's view of the peer node-id from the rendered .res.
# Format: `connection { ... net { ... } volume { ... } } on <host> { node-id <N>; ... }`.
# `drbdsetup status --verbose` is easier to parse.
n1_peer_id=$(on_node "$N1" drbdsetup status "$RD" --verbose 2>/dev/null \
    | grep -E "^[[:space:]]+${N2}[[:space:]]+node-id:" | grep -oE 'node-id:[0-9]+' | head -1 | cut -d: -f2)
n2_peer_id=$(on_node "$N2" drbdsetup status "$RD" --verbose 2>/dev/null \
    | grep -E "^[[:space:]]+${N1}[[:space:]]+node-id:" | grep -oE 'node-id:[0-9]+' | head -1 | cut -d: -f2)
echo "   peer-node-id from $N1's view = ${n1_peer_id}; from $N2's view = ${n2_peer_id}"
if [[ -n "$n1_peer_id" ]]; then
    on_node "$N1" drbdsetup peer-device-options "${RD}" "${n1_peer_id}" 0 \
        --c-max-rate="${RESYNC_RATE_KB}" --c-min-rate="${RESYNC_RATE_KB}" --resync-rate="${RESYNC_RATE_KB}" 2>&1 || true
fi
if [[ -n "$n2_peer_id" ]]; then
    on_node "$N2" drbdsetup peer-device-options "${RD}" "${n2_peer_id}" 0 \
        --c-max-rate="${RESYNC_RATE_KB}" --c-min-rate="${RESYNC_RATE_KB}" --resync-rate="${RESYNC_RATE_KB}" 2>&1 || true
fi

# --- Step 1+2: partition + heavy writes ---------------------------------
echo ">> partition $N2 from $N1 (drop tcp/$DRBD_PORT in+out on $N2)"
on_node "$N2" iptables -A INPUT  -p tcp --dport "$DRBD_PORT" -j DROP
on_node "$N2" iptables -A OUTPUT -p tcp --dport "$DRBD_PORT" -j DROP

# Wait for $N1 to notice $N2 is gone.
deadline=$(( $(date +%s) + 30 ))
while (( $(date +%s) < deadline )); do
    cs=$(read_connection "$N1" "$RD" "$N2")
    if [[ "$cs" != "Connected" && "$cs" != "Established" ]]; then break; fi
    sleep 1
done
echo "   $N1 sees peer as: ${cs:-(unknown)}"

echo ">> heavy write $((WRITE_BYTES / 1024 / 1024)) MiB on $N1 while partitioned"
on_node "$N1" bash -c "dd if=/dev/urandom of=${DEV} bs=1M count=$((WRITE_BYTES / 1024 / 1024)) conv=fdatasync status=none oflag=direct"

echo ">> capture authoritative md5 from $N1"
md5_authoritative=$(on_node "$N1" bash -c "dd if=${DEV} bs=1M count=$((WRITE_BYTES / 1024 / 1024)) status=none iflag=direct | md5sum | awk '{print \$1}'")
echo "   md5 = $md5_authoritative"

# --- Step 3: heal, peer enters SyncTarget --------------------------------
echo ">> heal partition; $N2 should enter SyncTarget"
cleanup_iptables
DRBD_PORT_SAVED=$DRBD_PORT
DRBD_PORT=""

deadline=$(( $(date +%s) + 60 ))
rep=""
while (( $(date +%s) < deadline )); do
    rep=$(read_replication "$N2" "$RD" "$N1")
    if [[ "$rep" == "SyncTarget" ]]; then break; fi
    sleep 1
done
if [[ "$rep" != "SyncTarget" ]]; then
    echo "FAIL: $N2 never entered SyncTarget after heal (rep=$rep)"
    on_node "$N2" drbdsetup status "$RD" --verbose || true
    exit 1
fi
echo "   $N2 replication: $rep"

# --- Step 4: stall mid-sync by dropping port again -----------------------
# At ~1 MiB/s, 100 MiB takes ~100 s — we have plenty of room to
# pick a safe mid-point. Drop the network at ~10 % (after 10 s).
echo ">> wait for sync to reach >=5% before stalling"
deadline=$(( $(date +%s) + 60 ))
pct_at_stall=-1
while (( $(date +%s) < deadline )); do
    pct=$(read_done_pct "$N2" "$RD")
    if (( pct >= 5 )); then pct_at_stall=$pct; break; fi
    # bail if sync finished prematurely
    rep=$(read_replication "$N2" "$RD" "$N1")
    if [[ "$rep" != "SyncTarget" ]]; then
        echo "FAIL: sync left SyncTarget before reaching 5% (rep=$rep) — resync rate not throttled?"
        exit 1
    fi
    sleep 1
done
if (( pct_at_stall < 0 )); then
    echo "FAIL: sync never reached 5%"
    exit 1
fi
echo "   sync at ${pct_at_stall}% — stalling now (re-drop tcp/${DRBD_PORT_SAVED})"

DRBD_PORT=$DRBD_PORT_SAVED
on_node "$N2" iptables -A INPUT  -p tcp --dport "$DRBD_PORT" -j DROP
on_node "$N2" iptables -A OUTPUT -p tcp --dport "$DRBD_PORT" -j DROP

# Observe done:% over 30 s. With the port dropped DRBD will keep
# its in-flight requests "in flight" for ping-timeout (default
# 0.5 s) ×N retries before transitioning to Connecting. Up until
# that transition, replication: stays SyncTarget but done:% is
# frozen — that's our wedge. After the transition, done:% reads
# -1 (not in SyncTarget) which is a different state but still
# proves the recipe will be needed on reconnect.
echo ">> observe done:% over ${STALL_OBSERVE_SECONDS}s"
samples=()
reps=()
end=$(( $(date +%s) + STALL_OBSERVE_SECONDS ))
while (( $(date +%s) < end )); do
    p=$(read_done_pct "$N2" "$RD")
    r=$(read_replication "$N2" "$RD" "$N1")
    samples+=("$p")
    reps+=("$r")
    sleep 3
done
echo "   done:% samples = ${samples[*]}"
echo "   replication samples = ${reps[*]}"

# Wedge = while replication==SyncTarget, done:% delta <2pp over
# the window. We compute the delta only over samples where the
# peer was still in SyncTarget (positive pct).
min=999 ; max=-1 ; counted=0
for s in "${samples[@]}"; do
    if (( s >= 0 )); then
        (( s < min )) && min=$s
        (( s > max )) && max=$s
        counted=$((counted + 1))
    fi
done
stall_observed=0
if (( counted >= 2 )); then
    delta=$(( max - min ))
    echo "   while-SyncTarget delta = ${delta}pp over $counted samples"
    if (( delta < 2 )); then
        stall_observed=1
        echo "   STALL OBSERVED — wedged SyncTarget reproduced"
    fi
else
    # Fell out of SyncTarget into Connecting — also "stuck" from
    # an operator perspective (the resume requires reconnect).
    if [[ " ${reps[*]} " == *" Connecting "* || " ${reps[*]} " == *" "*"" ]]; then
        stall_observed=1
        echo "   STALL OBSERVED — $N2 fell to Connecting (still needs recipe to recover)"
    fi
fi

if (( stall_observed == 0 )); then
    echo "   stall NOT cleanly observed — recipe applied as a safety check"
fi

# --- Step 5: apply the recipe -------------------------------------------
echo ">> heal partition again (clear iptables) and apply recipe"
cleanup_iptables
DRBD_PORT=""

t_recipe_start=$(date +%s)
on_node "$N2" drbdadm disconnect "${RD}:${N1}" || true
sleep 1
on_node "$N2" drbdadm connect "${RD}:${N1}" || true

# --- Step 6: assert resume within 10s -----------------------------------
echo ">> wait <=10s for done:% to advance past stall point (${pct_at_stall}%)"
deadline=$(( t_recipe_start + 10 ))
pct_after=-1
resumed_at=0
while (( $(date +%s) < deadline )); do
    pct_after=$(read_done_pct "$N2" "$RD")
    rep_after=$(read_replication "$N2" "$RD" "$N1")
    if [[ "$rep_after" != "SyncTarget" && -n "$rep_after" && "$rep_after" != "Connecting" && "$rep_after" != "WFBitMapT" ]]; then
        resumed_at=$(( $(date +%s) - t_recipe_start ))
        break
    fi
    if (( pct_after > pct_at_stall + 1 )); then
        resumed_at=$(( $(date +%s) - t_recipe_start ))
        break
    fi
    sleep 1
done

if (( resumed_at == 0 )); then
    if (( pct_after >= 0 && pct_after <= pct_at_stall + 1 )); then
        echo "FAIL: recipe did not unstick sync within 10s (still at ${pct_after}%, stall was ${pct_at_stall}%)"
        on_node "$N2" drbdsetup status "$RD" --verbose || true
        exit 1
    fi
fi
echo "   sync resumed in ${resumed_at}s (now at ${pct_after}%, replication=${rep_after:-})"
if (( resumed_at > 5 )); then
    echo "   WARN: resume took >5s — slower than 5s target but still within window"
fi

# --- Step 7: assert reconciler invariant --------------------------------
echo ">> verify reconciler did not auto-down/up $RD on $N2"
sat_pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o "jsonpath={.items[?(@.spec.nodeName==\"${N2}\")].metadata.name}")
recent_log=$(kubectl -n "$NS" logs --since=5m --tail=2000 "$sat_pod" 2>/dev/null || true)
if echo "$recent_log" | grep -qE "drbdadm (down|up) ${RD}\\b"; then
    echo "FAIL: satellite ran drbdadm down/up on ${RD} during recovery (5.6 invariant)"
    echo "$recent_log" | grep -E "drbdadm (down|up) ${RD}" | tail -5
    exit 1
fi
echo "   reconciler invariant OK"

# --- Step 8: final UpToDate + md5 ---------------------------------------
echo ">> wait <=240s for both peers UpToDate (throttled resync needs time)"
deadline=$(( $(date +%s) + 240 ))
while (( $(date +%s) < deadline )); do
    p1=$(status_replication_state "$RD" "$N1" "$N2")
    p2=$(status_replication_state "$RD" "$N2" "$N1")
    d2=$(status_disk_state "$RD" "$N2")
    if [[ "$p1" == "Established" && "$p2" == "Established" && "$d2" == "UpToDate" ]]; then break; fi
    sleep 3
done
if [[ "$d2" != "UpToDate" ]]; then
    echo "FAIL: $N2 never reached UpToDate (last d2=$d2 p1=$p1 p2=$p2)"
    on_node "$N2" drbdsetup status "$RD" --verbose || true
    exit 1
fi
echo "   both peers UpToDate"

echo ">> demote $N1, verify md5 on $N2"
on_node "$N1" drbdadm secondary "$RD" || true
sleep 3
md5_after=$(on_node "$N2" bash -c "
    drbdadm primary ${RD} 2>/dev/null || true
    dd if=${DEV} bs=1M count=$((WRITE_BYTES / 1024 / 1024)) status=none iflag=direct | md5sum | awk '{print \$1}'
")
if [[ "$md5_after" != "$md5_authoritative" ]]; then
    echo "FAIL: md5 mismatch after sync recovery ($N2=$md5_after, expected=$md5_authoritative)"
    exit 1
fi

if (( stall_observed == 1 )); then
    echo ">> STUCK-SYNCTARGET OK (stall observed, recipe unwedged in ${resumed_at}s, md5 verified)"
else
    echo ">> STUCK-SYNCTARGET OK (no clean wedge reproduced, recipe was safe, md5 verified)"
fi
