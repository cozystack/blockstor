#!/usr/bin/env bash
#
# usage: recovery-stuck-synctarget-down-up.sh WORK_DIR
#
# Scenario 5.33 — stuck SyncTarget recovers via `drbdadm down + up`
# cycle (SKILL Group B recipe). The companion 5.21 test
# (recovery-stuck-synctarget.sh) covers the disconnect+connect
# recipe; this one is the heavier-hammer fallback when the
# half-dead TCP / paused-resync workflow can't be reconnected
# back into life (rare — P2 — but real).
#
# Why down+up over disconnect+connect
# -----------------------------------
# `drbdadm disconnect <r>:<peer>` + `connect <r>:<peer>` tears one
# TCP and re-handshakes. It assumes the kernel slot itself is
# healthy — only the wire is wedged. There are pathological
# states where the slot is wedged too: SyncTarget with a stuck
# in-kernel resync state machine that disconnect alone can't
# nudge. The operator's last-resort recovery before destroying
# state is to bring the WHOLE resource down and back up on the
# SyncTarget side:
#
#     drbdadm down <r>     # tear kernel slot, keep .res + meta
#     drbdadm up   <r>     # rebuild slot from .res, re-attach
#                          # the (bitmap-fed) lower disk, peer
#                          # handshakes, resync resumes from
#                          # the last durable bitmap position
#
# SKILL constraint: down+up is ONLY safe while the resource is
# Unused (no open() against /dev/drbd<minor>). If a workload is
# holding the device the down silently keeps the slot half-up
# and the up is a no-op — the wedge is unchanged. This test
# does not pin a workload to the SyncTarget side, so Unused is
# trivially true here; the SKILL warning still applies to anyone
# running this against a live volume.
#
# Reconciler-survival invariant (THE point of the test)
# -----------------------------------------------------
# The blockstor satellite has two independent re-converge paths:
#
#   1. ResourceReconciler (controllers/resource.go) — watches the
#      Resource CRD; calls satellite.Apply on every change /
#      requeue. Apply renders the .res file and calls
#      `drbdadm adjust` (which loads / reconciles kernel state
#      against the rendered .res).
#   2. ObserverRunnable (controllers/observer.go) — tails
#      `drbdsetup events2`, caches per-resource state, and
#      writes Status.Volumes / Status.Connections back via SSA.
#      observerResyncInterval=5s — every 5s it re-emits cached
#      state so a slow-arrived `exists` frame doesn't lose its
#      DiskState.
#
# The window between the operator's `drbdadm down` and `drbdadm
# up` is short (single seconds) but it's exactly long enough for
# (a) one or two observer ticks and (b) any pending Resource
# CRD change to trigger reconciliation. If either of those races
# the operator's recipe by:
#
#   - re-rendering .res (wouldn't change anything — same peers,
#     same port — but the rewrite is observable in /var/lib/...)
#   - running `drbdadm adjust` while the slot is gone (fails
#     with `Unknown resource (158)`, which floods logs and
#     races with the operator's `up` if it lands first)
#   - running `drbdadm up` independently of the operator's
#     command (Bug 8's symptom: a default `up` after kernel
#     state clears clobbers the in-flight resync bitmap state
#     and restarts the sync from 0%)
#
# … then 5.33's contract is broken. The satellite MUST stay
# quiet for the brief window. The defer-gate that makes this
# safe is the IsResourceSyncing check the reconciler runs
# before `drbdadm adjust` / `up` — discussed in
# pkg/satellite/reconciler_drbd_test.go::TestApplyDefersAdjust
# DuringSyncTarget and unit-pinned in
# pkg/satellite/controllers/observer_internal_test.go::
# TestObserverSyncGateCoversDownUpWindow.
#
# Reproduction strategy
# ---------------------
# Inducing a TRULY stuck SyncTarget that disconnect alone can't
# fix is hard in a synthetic test — the natural triggers are
# kernel-version-specific corner cases. We use a deterministic
# proxy: pause the source-side resync via `drbdadm pause-sync
# <r>:<source>` from the SyncSource. Once paused, the kernel
# reports `replication:PausedSyncS` / `PausedSyncT` and the
# done:% counter freezes — same observable as a stuck target
# from the reconciler's perspective (any peer in a Paused* /
# Sync* state must defer the reconciler's adjust). After 30s
# of confirmed stall, we apply the down+up recipe on the
# SyncTarget side and verify:
#
#   (a) the satellite did NOT issue its own `drbdadm down`,
#       `drbdadm up`, or `drbdadm adjust` calls during the
#       window (log scrape on the SyncTarget satellite pod)
#   (b) sync resumes within 60s post-up
#   (c) both peers reach UpToDate (Established replication)
#
# Steps
#   1. Apply 2-replica RD on $N1+$N2, wait UpToDate.
#   2. Throttle resync to ~1 MiB/s so we have a ~100s window
#      to stall and recover.
#   3. Partition $N2 from $N1 via iptables on tcp/$DRBD_PORT.
#   4. Heavy-write 100 MiB on $N1 (Primary).
#   5. Heal the partition; $N2 enters SyncTarget.
#   6. Wait until sync reaches ~10%, then pause-sync from $N1.
#   7. Observe stall for 30s (done:% frozen, replication:
#      PausedSyncS / PausedSyncT held).
#   8. Apply recipe on $N2: `drbdadm down <rd>; drbdadm up <rd>`.
#   9. Assert satellite log has no `drbdadm (down|up|adjust)`
#      entries for $RD during the wall-clock window of steps 6-8
#      (reconciler quiet-window invariant).
#  10. Resume-sync from $N1; wait <=60s for done:% to advance
#      past the stall point, then <=240s for both UpToDate.
#  11. Cleanup via delete_rd EXIT trap.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD=stuck-sync-down-up
N1=$WORKER_1     # Primary / SyncSource
N2=$WORKER_2     # Secondary / SyncTarget (the one we down+up)
SIZE_KIB=524288                       # 512 MiB
WRITE_BYTES=$((100 * 1024 * 1024))    # 100 MiB dirty
STALL_OBSERVE_SECONDS=30
RESYNC_RATE_KB=1024                   # ~1 MiB/s — slow enough to stall mid-flight
RESUME_DEADLINE_SECS=60
UPTODATE_DEADLINE_SECS=240
DRBD_PORT=""

trap 'cleanup_iptables; delete_rd "$RD"' EXIT

cleanup_iptables() {
    if [[ -n "$DRBD_PORT" ]]; then
        on_node "$N2" iptables -D INPUT  -p tcp --dport "$DRBD_PORT" -j DROP 2>/dev/null || true
        on_node "$N2" iptables -D OUTPUT -p tcp --dport "$DRBD_PORT" -j DROP 2>/dev/null || true
    fi
}

# read_done_pct prints the SyncTarget done-percentage as integer,
# or -1 when the peer is not in any Sync* / Paused* state.
read_done_pct() {
    local node=$1 rd=$2 line pct
    line=$(on_node "$node" drbdsetup status "$rd" --verbose 2>/dev/null \
        | grep -E 'replication:(SyncTarget|PausedSyncT|PausedSyncS)' | head -1 || true)
    if [[ -z "$line" ]]; then echo "-1"; return 0; fi
    pct=$(echo "$line" | grep -oE 'done:[0-9]+(\.[0-9]+)?' | head -1 | cut -d: -f2 | cut -d. -f1)
    echo "${pct:--1}"
}

read_replication() {
    local node=$1 rd=$2
    on_node "$node" drbdsetup status "$rd" --verbose 2>/dev/null \
        | grep -oE 'replication:[A-Za-z]+' | head -1 | cut -d: -f2 || true
}

read_connection() {
    local node=$1 rd=$2
    on_node "$node" drbdsetup status "$rd" --verbose 2>/dev/null \
        | grep -oE 'connection:[A-Za-z]+' | head -1 | cut -d: -f2 || true
}

# satellite_pod_on resolves the satellite pod name on a node.
satellite_pod_on() {
    local node=$1
    kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o "jsonpath={.items[?(@.spec.nodeName==\"${node}\")].metadata.name}"
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

# Seed pattern so post-recovery md5 has something to verify against.
echo ">> seed 4 MiB pattern on $N1"
on_node "$N1" bash -c "drbdadm primary --force ${RD} 2>/dev/null; dd if=/dev/urandom of=${DEV} bs=1M count=4 conv=fdatasync status=none"

# Throttle resync — same rationale as the 5.21 cousin: 1 MiB/s
# leaves us ~100s wall-time to pause-sync mid-flight.
echo ">> throttle resync to ${RESYNC_RATE_KB} KiB/s"
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

# Step 3-4: partition + heavy writes -------------------------------------
echo ">> partition $N2 from $N1 (drop tcp/$DRBD_PORT in+out on $N2)"
on_node "$N2" iptables -A INPUT  -p tcp --dport "$DRBD_PORT" -j DROP
on_node "$N2" iptables -A OUTPUT -p tcp --dport "$DRBD_PORT" -j DROP

deadline=$(( $(date +%s) + 30 ))
cs=""
while (( $(date +%s) < deadline )); do
    cs=$(read_connection "$N1" "$RD")
    if [[ "$cs" != "Connected" ]]; then break; fi
    sleep 1
done
echo "   $N1 sees peer as: ${cs:-(unknown)}"

echo ">> heavy write $((WRITE_BYTES / 1024 / 1024)) MiB on $N1 while partitioned"
on_node "$N1" bash -c "dd if=/dev/urandom of=${DEV} bs=1M count=$((WRITE_BYTES / 1024 / 1024)) conv=fdatasync status=none oflag=direct"

echo ">> capture authoritative md5 from $N1"
md5_authoritative=$(on_node "$N1" bash -c "dd if=${DEV} bs=1M count=$((WRITE_BYTES / 1024 / 1024)) status=none iflag=direct | md5sum | awk '{print \$1}'")
echo "   md5 = $md5_authoritative"

# Step 5: heal, peer enters SyncTarget -----------------------------------
echo ">> heal partition; $N2 should enter SyncTarget"
cleanup_iptables
DRBD_PORT_SAVED=$DRBD_PORT
DRBD_PORT=""

deadline=$(( $(date +%s) + 60 ))
rep=""
while (( $(date +%s) < deadline )); do
    rep=$(read_replication "$N2" "$RD")
    if [[ "$rep" == "SyncTarget" ]]; then break; fi
    sleep 1
done
if [[ "$rep" != "SyncTarget" ]]; then
    echo "FAIL: $N2 never entered SyncTarget after heal (rep=$rep)"
    on_node "$N2" drbdsetup status "$RD" --verbose || true
    exit 1
fi
echo "   $N2 replication: $rep"

# Step 6: pause-sync from $N1 once we're ~10% in.
echo ">> wait for sync to reach >=5% before inducing stall via pause-sync"
deadline=$(( $(date +%s) + 60 ))
pct_at_stall=-1
while (( $(date +%s) < deadline )); do
    pct=$(read_done_pct "$N2" "$RD")
    if (( pct >= 5 )); then pct_at_stall=$pct; break; fi
    rep=$(read_replication "$N2" "$RD")
    if [[ "$rep" != "SyncTarget" ]]; then
        echo "FAIL: sync left SyncTarget before reaching 5% (rep=$rep) — throttle ineffective?"
        exit 1
    fi
    sleep 1
done
if (( pct_at_stall < 0 )); then
    echo "FAIL: sync never reached 5%"
    exit 1
fi
echo "   sync at ${pct_at_stall}% — pausing from $N1 to induce stuck state"

# Pause the resync from the SyncSource ($N1). drbd-9 transitions
# the peer-device into PausedSyncS / PausedSyncT and freezes done:%.
on_node "$N1" drbdadm pause-sync "${RD}:${N1}" 2>/dev/null \
    || on_node "$N1" drbdadm pause-sync "${RD}" 2>/dev/null || true

# Mark the start of the reconciler-quiet-window for the post-recovery
# log scrape. Anything `drbdadm` the satellite logs from now until
# the assertion below is an invariant violation.
t_window_start=$(date +%s)

# Step 7: observe stall for 30s.
echo ">> observe stall over ${STALL_OBSERVE_SECONDS}s (replication frozen in Paused* / Sync*)"
samples=()
reps=()
end=$(( $(date +%s) + STALL_OBSERVE_SECONDS ))
while (( $(date +%s) < end )); do
    p=$(read_done_pct "$N2" "$RD")
    r=$(read_replication "$N2" "$RD")
    samples+=("$p")
    reps+=("$r")
    sleep 3
done
echo "   done:% samples = ${samples[*]}"
echo "   replication samples = ${reps[*]}"

# Stall = while replication ∈ {Sync*, Paused*}, done:% delta <2pp
# over the window. We compute the delta only on positive pct samples.
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
    echo "   while-stuck delta = ${delta}pp over $counted samples"
    if (( delta < 2 )); then
        stall_observed=1
        echo "   STALL OBSERVED — stuck SyncTarget reproduced (via pause-sync proxy)"
    fi
else
    if [[ " ${reps[*]} " == *" PausedSyncS "* || " ${reps[*]} " == *" PausedSyncT "* ]]; then
        stall_observed=1
        echo "   STALL OBSERVED — peer is in Paused* state, still needs recipe to recover"
    fi
fi

if (( stall_observed == 0 )); then
    echo "   stall NOT cleanly observed — recipe applied as a safety check"
fi

# Step 8: apply down+up recipe on $N2.
echo ">> apply recipe on $N2: drbdadm down ${RD} ; drbdadm up ${RD}"
t_recipe_start=$(date +%s)
on_node "$N2" drbdadm down "${RD}" || true
sleep 1
on_node "$N2" drbdadm up "${RD}" || true

# Step 9: reconciler-quiet-window invariant.
# The window we care about is from t_window_start (the moment we
# induced the stall) through the recipe finishing. Any `drbdadm
# down/up/adjust` issued by the satellite's reconciler in that
# window would race the operator's recipe — that's the 5.33
# contract violation.
echo ">> verify satellite did NOT run drbdadm down/up/adjust on ${RD} during the quiet window"
sat_pod=$(satellite_pod_on "$N2")
if [[ -z "$sat_pod" ]]; then
    echo "FAIL: could not resolve satellite pod on $N2"
    exit 1
fi
window_seconds=$(( $(date +%s) - t_window_start + 5 ))
# kubectl logs --since takes a duration string.
recent_log=$(kubectl -n "$NS" logs --since="${window_seconds}s" --tail=5000 "$sat_pod" 2>/dev/null || true)
violation=""
if echo "$recent_log" | grep -qE "drbdadm (down|up|adjust) ${RD}\\b"; then
    violation=$(echo "$recent_log" | grep -E "drbdadm (down|up|adjust) ${RD}\\b" | tail -5)
fi
if [[ -n "$violation" ]]; then
    echo "FAIL: reconciler interfered during the down+up window:"
    echo "$violation" | sed 's/^/      /'
    exit 1
fi
echo "   reconciler quiet-window OK (no drbdadm down/up/adjust on ${RD} for ${window_seconds}s)"

# Cross-check: kubectl events shouldn't show a Resource-CRD-driven
# restart cycle either (e.g. a `RequeueAfter` storm that landed
# Apply repeatedly during the window). Best-effort — events have
# 1-hour retention and the SAT Apply path doesn't always emit them.
events_during=$(kubectl get events -n "$NS" --field-selector "involvedObject.name=${RD}.${N2}" \
    -o jsonpath='{range .items[*]}{.lastTimestamp}{"\t"}{.reason}{"\t"}{.message}{"\n"}{end}' 2>/dev/null \
    | awk -v t="$(date -u -d "-${window_seconds} seconds" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v "-${window_seconds}S" +%Y-%m-%dT%H:%M:%SZ)" \
        '$1 >= t' \
    || true)
if echo "$events_during" | grep -qiE "restart|reconcil.*fail|adjust.*fail"; then
    echo "WARN: Resource events during quiet window:"
    echo "$events_during" | sed 's/^/      /'
fi

# Step 10: resume sync from $N1; wait for the resync to advance
# past the stall point.
echo ">> resume-sync from $N1 (clear pause flag) and wait <=${RESUME_DEADLINE_SECS}s for done:% to advance"
on_node "$N1" drbdadm resume-sync "${RD}:${N1}" 2>/dev/null \
    || on_node "$N1" drbdadm resume-sync "${RD}" 2>/dev/null || true

deadline=$(( $(date +%s) + RESUME_DEADLINE_SECS ))
pct_after=-1
rep_after=""
resumed_at=0
while (( $(date +%s) < deadline )); do
    pct_after=$(read_done_pct "$N2" "$RD")
    rep_after=$(read_replication "$N2" "$RD")
    # If we've left Sync* / Paused* entirely (Established) — sync
    # completed during the window. That's a successful resume.
    if [[ "$rep_after" == "Established" ]]; then
        resumed_at=$(( $(date +%s) - t_recipe_start ))
        break
    fi
    if (( pct_after > pct_at_stall + 1 )); then
        resumed_at=$(( $(date +%s) - t_recipe_start ))
        break
    fi
    sleep 2
done

if (( resumed_at == 0 )); then
    echo "FAIL: sync did not resume within ${RESUME_DEADLINE_SECS}s after recipe (pct=${pct_after}, rep=${rep_after})"
    on_node "$N2" drbdsetup status "$RD" --verbose || true
    exit 1
fi
echo "   sync resumed ${resumed_at}s after recipe (pct=${pct_after}, rep=${rep_after})"

# Step 11: wait for both peers UpToDate + md5 verify.
echo ">> wait <=${UPTODATE_DEADLINE_SECS}s for both peers UpToDate (throttled resync needs time)"
deadline=$(( $(date +%s) + UPTODATE_DEADLINE_SECS ))
d2=""
while (( $(date +%s) < deadline )); do
    p1=$(on_node "$N1" drbdsetup status "$RD" --verbose 2>/dev/null | grep -E 'replication:Established' | head -1 || true)
    p2=$(on_node "$N2" drbdsetup status "$RD" --verbose 2>/dev/null | grep -E 'replication:Established' | head -1 || true)
    d2=$(on_node "$N2" drbdsetup status "$RD" --verbose 2>/dev/null | grep -E 'disk:UpToDate' | head -1 || true)
    if [[ -n "$p1" && -n "$p2" && -n "$d2" ]]; then break; fi
    sleep 3
done
if [[ -z "$d2" ]]; then
    echo "FAIL: $N2 never reached UpToDate"
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
    echo "FAIL: md5 mismatch after down+up recovery ($N2=$md5_after, expected=$md5_authoritative)"
    exit 1
fi

if (( stall_observed == 1 )); then
    echo ">> PASS 5.33 — stall observed, down+up recipe unwedged in ${resumed_at}s, reconciler stayed quiet, md5 verified"
else
    echo ">> PASS 5.33 — no clean stall reproduced (pause-sync may have raced), recipe was safe, reconciler stayed quiet, md5 verified"
fi
