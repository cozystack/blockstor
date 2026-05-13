#!/usr/bin/env bash
#
# usage: state-auto-resync.sh WORK_DIR
#
# Scenario 5.15 — Inconsistent auto-recovers when peers reconnect
# (reconciler doesn't interfere).
#
# Companion to 5.16 (recovery-synctarget-defer.sh), but covers the
# *automatic* recovery path rather than the controller-triggered
# adjust path:
#
#   - 5.16 asserts: a Spec change mid-SyncTarget must NOT trigger
#     drbdadm adjust on the satellite (Bug 8 defer logic).
#   - 5.15 asserts: an idle reconciler must NOT spontaneously
#     adjust during a kernel-driven bitmap resync. The reconciler
#     should sit on its hands while the underlying DRBD-9 layer
#     resyncs the diverged peer, and the observer should surface
#     the Inconsistent → SyncTarget → UpToDate trajectory cleanly
#     in the linstor r l / kubectl-get output without ever
#     dropping the row to Unknown or Outdated.
#
# Flow:
#   1. 3-replica RD on workers 1/2/3, autoplace, wait UpToDate.
#   2. On worker-3: iptables-drop the DRBD TCP port (in+out).
#      The kernel's net layer takes the connection down within the
#      DRBD ping-timeout (~6s on this stand) and the row goes
#      Connecting/StandAlone toward both peers — worker-3 sees its
#      bitmap diverge as new writes land on the Primary.
#
#      We deliberately do NOT use `drbdadm disconnect` here: the
#      satellite resource reconciler reasserts the desired peer-
#      connection state on every reconcile (it renders .res and
#      shells `drbdadm adjust` for fresh resources), so a manual
#      `disconnect` is racy — the reconciler can re-`connect` the
#      peer before the 256 MiB write completes and the divergence
#      never accumulates. iptables-drop is opaque to the reconciler
#      and survives any number of adjust calls.
#
#   3. On worker-1 (the Primary side), write 256 MiB to the DRBD
#      device. These bytes never reach worker-3 — they accumulate
#      in worker-1's bitmap (against the now-unreachable peer) and
#      will be replayed once the connection comes back.
#   4. On worker-3: flush iptables. The kernel handshake
#      auto-negotiates SyncTarget (worker-3) ← SyncSource
#      (worker-1 or worker-2) and starts the bitmap-driven resync.
#   5. Within 60s observe worker-3's row walk
#      Inconsistent → SyncTarget → UpToDate. No State=Unknown,
#      no reconciler intervention.
#   6. Assert reconciler does NOT call drbdadm adjust mid-sync
#      (Bug 8 e2e validation, similar to 5.16 but the trigger is
#      auto kernel-driven, not a controller-side prop change).
#      We check by parsing the satellite pod log for "drbdadm
#      adjust" lines between the connect and the sync-completion
#      timestamps. The satellite's resource reconciler logs the
#      adjust at INFO level when it actually runs; no log lines
#      = no adjust = pass.
#   7. Verify data via md5 from worker-3 mount post-sync. We
#      compare against the md5 captured before the disconnect to
#      prove the bitmap-resume copied the right bytes (and didn't
#      silently mark un-replayed regions as UpToDate).
#   8. Cleanup via delete_rd.
#
# Failure modes guarded:
#   - State momentarily collapses to "Unknown" (observer race —
#     the row exists in linstor but its disk-state is missing
#     during the reconnect handshake)
#   - reconciler issues drbdadm adjust during the SyncTarget
#     window (Bug 8 regression on the auto-path)
#   - bitmap-resume drops bytes (md5 mismatch on the failover read)
#   - sync never starts: worker-3 stays Inconsistent forever after
#     the connect because the satellite re-rendered .res and
#     fought with the kernel's auto-negotiation

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

RD=auto-resync-test
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3
SIZE_KIB=$((1024 * 1024))            # 1 GiB volume — fits the 256 MiB diverge with margin
DIVERGE_BYTES=$((256 * 1024 * 1024)) # 256 MiB of new writes during the partition
RESYNC_DEADLINE_SECS=120             # generous: 256 MiB on the QEMU stand resyncs in <30s typically

# Track whether the iptables partition is currently in effect on $N3
# so the trap can flush it cleanly even on a partial run. Without the
# flush the next test inherits dropped DRBD traffic on this node.
PARTITION_ON=0

cleanup_partition() {
    if (( PARTITION_ON == 1 )); then
        on_node "$N3" iptables -F INPUT 2>/dev/null || true
        on_node "$N3" iptables -F OUTPUT 2>/dev/null || true
        PARTITION_ON=0
    fi
}

trap 'cleanup_partition; delete_rd "$RD"' EXIT

# wait_uptodate_3 — lib.sh's wait_uptodate is 2-peer only. The
# 5.15 scenario needs all three diskful rows UpToDate before
# we start poking the kernel; otherwise the disconnect would
# race the initial-sync the autoplace just kicked off.
wait_uptodate_3() {
    local rd=$1 deadline=$(( $(date +%s) + 240 ))
    while (( $(date +%s) < deadline )); do
        local ok=1
        for n in "$N1" "$N2" "$N3"; do
            local st
            st=$(on_node "$n" drbdsetup status "$rd" 2>/dev/null \
                 | grep "disk:" | head -1 || true)
            if [[ "$st" != *"disk:UpToDate"* ]]; then
                ok=0
                break
            fi
        done
        if (( ok == 1 )); then return 0; fi
        sleep 2
    done
    echo "FAIL: $rd never reached UpToDate on all 3 peers" >&2
    on_node "$N1" drbdsetup status "$rd" 2>/dev/null || true
    return 1
}

echo ">> apply 3-replica RD $RD (${SIZE_KIB} KiB) on $N1 + $N2 + $N3"
# Tiebreaker explicitly off — we already have 3 diskful peers,
# so a DISKLESS witness would only pollute the peer list and
# the satellite-log "adjust" search later.
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
for n in "$N1" "$N2" "$N3"; do
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

echo ">> wait up to 240s for all 3 peers UpToDate"
wait_uptodate_3 "$RD"
echo "   all 3 UpToDate"

# Capture the satellite pod names — we'll grep their logs after
# the test for any drbdadm adjust calls inside the sync window.
N3_POD=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o "jsonpath={.items[?(@.spec.nodeName==\"${N3}\")].metadata.name}")
if [[ -z "$N3_POD" ]]; then
    echo "FAIL: no satellite pod on $N3"
    exit 1
fi

# Promote $N1 to Primary, write a baseline marker that *will* be
# fully replicated before the disconnect (md5_baseline). We'll then
# write divergence bytes after the disconnect and use a different
# read range for the post-sync verify.
DEV_N1=$(device_for_rd "$RD" "$N1")
DEV_N3=$(device_for_rd "$RD" "$N3")
if [[ -z "$DEV_N1" || -z "$DEV_N3" ]]; then
    echo "FAIL: could not resolve /dev/drbdN for $RD on $N1 or $N3"
    exit 1
fi

# Take a timestamp marker for the satellite-log adjust search. We
# only care about adjust calls AFTER the disconnect — pre-disconnect
# adjusts during initial sync are expected (the reconciler renders
# .res for the freshly-applied RD).
echo ">> mark satellite log boundary on $N3 for adjust search"
LOG_BOUNDARY_TS=$(kubectl -n "$NS" exec "$N3_POD" -- date -u +%Y-%m-%dT%H:%M:%SZ)
echo "   log_boundary_ts = $LOG_BOUNDARY_TS"

# Partition $N3 from the other two peers via iptables-drop on the
# DRBD TCP port. Discover the port from $N3's rendered .res — DRBD-9
# uses a single mesh listen-port per local resource minor, identical
# on every peer for a given RD. INPUT+OUTPUT drop is required: INPUT
# alone leaves $N3's outbound keep-alives flowing and the connection
# lingers in `Connecting` instead of fully tearing down.
DRBD_PORT=$(on_node "$N3" bash -c "grep -oE 'address.*:[0-9]+' /etc/drbd.d/${RD}.res | head -1 | grep -oE '[0-9]+$'")
if [[ -z "$DRBD_PORT" ]]; then
    echo "FAIL: could not parse DRBD port from $N3's .res"
    exit 1
fi
echo ">> partition $N3 from peers (iptables drop tcp/$DRBD_PORT in+out)"
on_node "$N3" iptables -A INPUT  -p tcp --dport "$DRBD_PORT" -j DROP
on_node "$N3" iptables -A OUTPUT -p tcp --dport "$DRBD_PORT" -j DROP
PARTITION_ON=1

# Verify the kernel actually noticed: peer-connection on $N3 should
# transition out of `Connected`. The DRBD ping-timeout drives this
# at ~6s on the stand, so 30s is plenty of headroom.
echo ">> verify $N3 sees peer connections down"
deadline=$(( $(date +%s) + 30 ))
disconnected=0
while (( $(date +%s) < deadline )); do
    st=$(on_node "$N3" drbdsetup status --verbose "$RD" 2>/dev/null | tr '\n' ' ' || true)
    # connection:Connecting OR NetworkFailure OR Timeout — anything
    # that isn't Connected/Established proves the partition landed.
    if echo "$st" | grep -qE "connection:(StandAlone|Connecting|Unconnected|NetworkFailure|Timeout|BrokenPipe)"; then
        disconnected=1
        break
    fi
    sleep 1
done
if (( disconnected == 0 )); then
    echo "FAIL: $N3 connection never left Connected within 30s of iptables drop"
    on_node "$N3" drbdsetup status --verbose "$RD" || true
    exit 1
fi
echo "   $N3 partitioned from peers"

# Write 256 MiB on Primary ($N1). The bytes land on $N1 and $N2
# (still connected); $N3 misses them. write_random in lib.sh does
# the drbdadm primary dance for us, returns md5 of the *written*
# range. We need this md5 for the post-sync failover verify.
echo ">> write ${DIVERGE_BYTES} bytes on Primary ($N1, $DEV_N1) while $N3 is disconnected"
MD5_DIVERGE=$(write_random "$N1" "$DEV_N1" "$DIVERGE_BYTES")
echo "   md5_diverge = $MD5_DIVERGE"

# Heal partition. The kernel reconnect handshake now auto-negotiates:
#   - $N1/$N2 carry the newer UUID + bitmap-against-N3 → SyncSource
#   - $N3 has the older UUID + a bitmap-against-the-others →
#     SyncTarget, disk:Inconsistent
# The resync should kick off within 1-2s once TCP comes back up.
T_CONNECT=$(date +%s)
echo ">> heal partition on $N3 (iptables -F)"
cleanup_partition

# Sample the observer surface during the sync window. We want to
# see EITHER Inconsistent OR SyncTarget on $N3 (the CLI/observer
# annotates the disk state with a sync-progress percentage like
# `Inconsistent(28%)` while bitmap-resync runs; both prefixes
# match the post-Bug-29 observer fix). State=Unknown must NEVER
# appear — that signals the observer pipeline lost the row during
# the reconnect handshake.
echo ">> sample $N3 disk state during reconnect+resync (deadline ${RESYNC_DEADLINE_SECS}s)"
# State machine notes:
#   - Pre-heal: $N3 sits Secondary with disk:UpToDate locally, but
#     suspended:quorum (it's the minority side of the 1-vs-2
#     partition). peer-disk:DUnknown on both N1/N2.
#   - Heal moment: TCP handshake re-establishes, kernel UUIDs are
#     exchanged. $N3 sees its UUID is behind, voluntarily downgrades
#     local disk:UpToDate → Outdated → Inconsistent, then accepts
#     SyncTarget role and starts pulling the bitmap.
#   - Post-resync: back to disk:UpToDate, replication:Established.
#
# Because the pre-heal state is also disk:UpToDate, we must NOT
# break on the first UpToDate sample — we'd exit before the sync
# transition starts. Instead require either:
#   (a) we saw Inconsistent or SyncTarget AT LEAST ONCE,
#       AND the current state is disk:UpToDate + replication
#       != SyncTarget (i.e. genuine end-of-resync), OR
#   (b) the deadline expires.
# Sampling at 1s — the SyncTarget window on 256 MiB of dirty
# bitmap is short (often <10s on the QEMU stand).
saw_inconsistent=0
saw_synctarget=0
saw_unknown=0
reached_uptodate=0
saw_established_after_sync=0
deadline=$(( $(date +%s) + RESYNC_DEADLINE_SECS ))

while (( $(date +%s) < deadline )); do
    st=$(on_node "$N3" drbdsetup status --verbose "$RD" 2>/dev/null \
         | tr '\n' ' ' || true)
    # Match local disk state (`disk:Foo`, NOT peer-disk:Foo) by
    # looking at the first disk: occurrence — drbdsetup status
    # emits the local row first, then each peer.
    local_disk=$(echo "$st" | grep -oE 'disk:[A-Za-z]+' | head -1 || true)
    # Replication is per-peer; take whichever peer reports a
    # non-Off/non-Established value. SyncTarget on N3 means at
    # least one peer is acting as SyncSource right now. Both
    # `grep` calls can return nonzero (empty match) which under
    # `set -e` aborts the loop silently — wrap each with `|| true`
    # AND wrap the whole pipeline in `|| true` so the outer
    # assignment never propagates the failure.
    # Use awk/sed instead of grep|grep|head — single-process pipelines
    # avoid the `set -e + pipefail` trap where the inner grep finding
    # zero matches aborts the whole script silently.
    repl=$(printf '%s\n' "$st" | tr ' ' '\n' \
           | awk -F: '/^replication:/ && $2 != "Off" && $2 != "Established" {print; exit}' \
           || true)
    repl_any=$(printf '%s\n' "$st" | tr ' ' '\n' \
               | awk -F: '/^replication:/ {print; exit}' || true)
    # Count Connected peers; awk on tokenised input.
    est_peers=$(printf '%s\n' "$st" | tr ' ' '\n' \
                | awk '/^connection:Connected$/ {n++} END {print n+0}' \
                2>/dev/null || echo 0)
    echo "   [t=$(( $(date +%s) - T_CONNECT ))s] $N3: $local_disk repl=${repl:-$repl_any} connected_peers=$est_peers"
    case "$local_disk" in
        disk:Inconsistent|disk:Outdated) saw_inconsistent=1 ;;
        disk:UpToDate) reached_uptodate=1 ;;
        disk:Unknown|disk:DUnknown) saw_unknown=1 ;;
    esac
    if [[ "$repl" == "replication:SyncTarget" ]]; then
        saw_synctarget=1
    fi
    # End-of-resync: local UpToDate AND we previously observed
    # Inconsistent/SyncTarget AND no SyncTarget on the current
    # sample AND both peers Connected.
    if (( saw_inconsistent == 1 || saw_synctarget == 1 )) \
       && [[ "$local_disk" == "disk:UpToDate" ]] \
       && [[ "$repl" != "replication:SyncTarget" ]] \
       && (( est_peers >= 2 )); then
        saw_established_after_sync=1
        break
    fi
    sleep 1
done

if (( saw_unknown == 1 )); then
    echo "FAIL: $N3 disk-state surfaced Unknown during reconnect+resync"
    echo "      observer dropped the row mid-handshake"
    on_node "$N3" drbdsetup status --verbose "$RD" || true
    exit 1
fi
if (( saw_established_after_sync == 0 )); then
    echo "FAIL: $N3 did not converge to UpToDate-after-resync within ${RESYNC_DEADLINE_SECS}s"
    echo "      saw_inconsistent=$saw_inconsistent saw_synctarget=$saw_synctarget reached_uptodate=$reached_uptodate"
    on_node "$N3" drbdsetup status --verbose "$RD" || true
    exit 1
fi
if (( saw_inconsistent == 0 && saw_synctarget == 0 )); then
    # Neither Inconsistent/Outdated nor SyncTarget was observed —
    # either the sample window missed the transition (the 256 MiB
    # resync was too fast even at 1s polling) OR the row jumped
    # straight from the disconnect to UpToDate, which would mean
    # DRBD reused a GI match and skipped the bitmap-resume entirely.
    # The latter contradicts the test premise (we wrote 256 MiB on
    # the source side while $N3 was partitioned), so this is a
    # genuine fail.
    echo "FAIL: $N3 never surfaced Inconsistent/Outdated OR SyncTarget during the resync window"
    echo "      DRBD likely skipped the bitmap resume — divergence not replayed"
    on_node "$N3" drbdsetup status --verbose "$RD" || true
    exit 1
fi
echo "   resync trajectory observed:"
echo "     Inconsistent/Outdated observed = $saw_inconsistent"
echo "     SyncTarget            observed = $saw_synctarget"
echo "     UpToDate-after-resync reached  = $saw_established_after_sync"

# After reach-UpToDate, look for any drbdadm adjust call the
# satellite reconciler emitted between LOG_BOUNDARY_TS and now.
# The satellite logs at INFO level when it shells out to drbdadm;
# search the pod log for /drbdadm.*adjust/ entries with a
# timestamp >= the boundary. Any hit is a Bug 8 regression on the
# auto-path: the reconciler must defer adjust while the kernel is
# resyncing a peer.
echo ">> check satellite log on $N3 for any drbdadm adjust since $LOG_BOUNDARY_TS"
ADJUST_HITS=$(kubectl -n "$NS" logs "$N3_POD" --since-time="$LOG_BOUNDARY_TS" 2>/dev/null \
    | grep -iE 'drbdadm.*adjust|adjusting drbd resource' || true)
if [[ -n "$ADJUST_HITS" ]]; then
    echo "FAIL: satellite reconciler ran drbdadm adjust on $N3 during/after the resync"
    echo "      Bug 8 regression on the auto-recovery path:"
    echo "$ADJUST_HITS" | head -10
    exit 1
fi
echo "   no drbdadm adjust observed on $N3 during the resync window"

# Failover verify: demote $N1, promote $N3, read the same byte
# range we wrote, compare md5. If the bitmap-resume copied the
# right bytes, md5_after == md5_diverge.
echo ">> failover: demote $N1, promote $N3, re-read divergence range"
on_node "$N1" drbdadm secondary "$RD" || true
on_node "$N3" drbdadm primary "$RD"
MD5_AFTER=$(read_md5 "$N3" "$DEV_N3" "$DIVERGE_BYTES")
echo "   md5_after = $MD5_AFTER"
if [[ "$MD5_AFTER" != "$MD5_DIVERGE" ]]; then
    echo "FAIL: post-resync read mismatch on $N3"
    echo "      md5_diverge = $MD5_DIVERGE"
    echo "      md5_after   = $MD5_AFTER"
    echo "      bitmap-resume copied wrong bytes (or skipped a region)"
    exit 1
fi

echo ">> STATE-AUTO-RESYNC OK"
echo "   $N3 walked Inconsistent/SyncTarget -> UpToDate without reconciler intervention"
echo "   md5 across failover read confirms bitmap-resume integrity"
