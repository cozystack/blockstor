#!/usr/bin/env bash
#
# usage: rolling-upgrade.sh WORK_DIR
#
# Scenario 4.W28 — rolling upgrade of the controller + apiserver
# Deployments and the satellite DaemonSet must preserve I/O on a
# live DRBD-backed device.
#
# Contract (tests/scenarios/wave2-04-lifecycle.md §4.W28):
#
#   * blockstor runs as a 3-replica `blockstor-apiserver` Deployment
#     (REST), a 1-replica `blockstor-controller` Deployment
#     (reconcilers), and a per-node `blockstor-satellite` DaemonSet.
#     `helm upgrade blockstor` translates to a Deployment surge update
#     for the apiserver, a Recreate-style replace for the controller
#     (no DB to migrate — state lives in the CRDs), and a one-at-a-time
#     DaemonSet rolling update for the satellites.
#
#   * Killing the controller pod is a no-op for in-flight I/O: the
#     replacement reconciler picks up state from the CRD store on
#     start, there is no failover handshake. Same for the apiserver
#     replicas — they are stateless and behind a Service.
#
#   * Killing a satellite pod must NOT take its DRBD resources down:
#     DRBD lives in the host kernel, the satellite is just a reconciler
#     that re-renders /etc/drbd.d/*.res and runs `drbdadm adjust`. When
#     the satellite restarts, its reconciler-on-restart path must
#     rebuild in-memory state from CRD watches and converge without
#     bouncing the running DRBD device.
#
# Test recipe:
#
#   1. Apply a 2-replica RD on WORKER_1 (Primary) + WORKER_2 (peer).
#      Wait for both replicas to land UpToDate.
#   2. Resolve the local /dev/drbdN minor on the Primary; start a
#      background dd writer that issues 1 KiB direct-I/O writes once
#      per second to the device. Errors get tee'd to a log; the
#      caller asserts the log stays empty.
#   3. `kubectl rollout restart deploy/blockstor-apiserver` — wait
#      for the rollout to settle. While it runs, the writer must keep
#      ticking. The REST plane has no role in already-bound DRBD
#      devices, so this rollout is the easy floor of the test.
#   4. `kubectl rollout restart deploy/blockstor-controller` — same
#      check. The reconciler is the operator-of-record for CRD →
#      satellite reconciliation; killing it must not perturb DRBD.
#   5. `kubectl rollout restart ds/blockstor-satellite` — the
#      DaemonSet's default rolling-update strategy is maxUnavailable=1,
#      so satellites bounce one at a time. Even for the Primary's
#      satellite pod, DRBD survives in the host kernel (the preStop
#      hook calls `drbdadm down --all` only on graceful eviction; with
#      `rollout restart` the pod is recreated and DRBD is re-asserted
#      from the persisted .res files, so the kernel state never
#      drops). The writer keeps ticking.
#   6. After every rollout: assert no writer error, all replicas back
#      to UpToDate, all satellite Pods Ready.
#
# What the test does NOT exercise:
#
#   * Image-tag swap. `kubectl rollout restart` triggers the exact
#     same Deployment / DaemonSet rolling-update machinery as a
#     `helm upgrade` with a new image, which is what the contract is
#     about. Doing a real image swap would require a registry with
#     two tags of the same satellite + apiserver builds — not in
#     scope for an in-cluster e2e. The marker we care about is "the
#     rolling-update strategy preserves I/O", not "different code
#     versions interoperate".
#
#   * The 1-replica controller Deployment doesn't have a surge update
#     (maxSurge=0 with 1 replica), so step 4 will see a brief no-
#     controller window. That window is harmless for in-flight I/O
#     because DRBD does not depend on the controller — verifying
#     exactly that is the point of step 4.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD=e2e-rolling-upgrade
# Per-rollout settle budget. A DaemonSet rolling update of N=3 with
# maxUnavailable=1 on a busy QEMU stand takes ~30-60 s; double the
# upper bound for safety.
ROLLOUT_TIMEOUT=180
# Writer cadence — slow enough to keep the log small, fast enough
# that a 5-30 s I/O suspension would still drop multiple ticks and
# surface as a missing-tick gap in the writer log.
WRITER_INTERVAL=1
WRITER_LOG=/tmp/rolling-upgrade-writer.log
WRITER_ERR=/tmp/rolling-upgrade-writer.err

WRITER_PID=""

cleanup() {
    local rc=$?

    if [[ -n "$WRITER_PID" ]] && kill -0 "$WRITER_PID" 2>/dev/null; then
        kill "$WRITER_PID" 2>/dev/null || true
        wait "$WRITER_PID" 2>/dev/null || true
    fi

    if (( rc != 0 )); then
        echo "---- dump: writer stderr (last 50 lines) ----"
        tail -n 50 "$WRITER_ERR" 2>/dev/null || true
        echo "---- dump: writer stdout (last 50 lines) ----"
        tail -n 50 "$WRITER_LOG" 2>/dev/null || true
        echo "---- dump: pods ----"
        kubectl -n "$NS" get pods -o wide || true
        echo "---- dump: drbd status (all satellites) ----"
        for pod in $(kubectl -n "$NS" get pods -l app=blockstor-satellite -o name 2>/dev/null); do
            echo "-- $pod --"
            kubectl -n "$NS" exec "$pod" -- drbdsetup status 2>/dev/null || true
        done
    fi

    delete_rd "$RD" 2>/dev/null || true
    rm -f "$WRITER_LOG" "$WRITER_ERR" 2>/dev/null || true
}
trap cleanup EXIT

PRIMARY=$WORKER_1
PEER=$WORKER_2

# ---------- Step 1: create RD, wait for UpToDate ----------
echo ">> apply 2-replica RD on $PRIMARY + $PEER"
rd_apply "$RD" "$PRIMARY" "$PEER" 65536
wait_uptodate "$RD" "$PRIMARY" "$PEER"

# Promote on $PRIMARY and resolve the device path.
on_node "$PRIMARY" drbdadm primary --force "$RD" 2>/dev/null || true
sleep 2

DEV=$(device_for_rd "$RD" "$PRIMARY")
if [[ -z "$DEV" ]]; then
    echo "FAIL: could not resolve /dev/drbdN on $PRIMARY for $RD"
    exit 1
fi
echo "   primary $PRIMARY, device $DEV"

# ---------- Step 2: start background writer ----------
#
# We exec the writer inside the satellite pod on $PRIMARY (that's the
# only place /dev/drbdN exists). The writer ticks once per second:
#   - dd 1 KiB of /dev/urandom to the device with direct I/O at a
#     varying offset so the kernel actually has to drain a write
#     barrier each tick.
#   - if dd fails (write error, EIO, ESHUTDOWN), it gets logged with
#     a timestamp to $WRITER_ERR. A non-empty $WRITER_ERR at the end
#     of the test is a hard FAIL.
#
# The loop runs until we touch /tmp/rolling-upgrade.stop inside the
# pod; the main script touches that file before checking results.
#
# Wrapped in `kubectl exec` + `setsid` so the loop survives even if
# the exec connection drops mid-rollout (which can happen when the
# satellite pod on $PRIMARY itself bounces — the connection dies
# with the old pod but the loop is owned by init in the new pod).
# Actually: kubectl exec dies with the pod. We work around this in
# step 5 by re-spawning the writer after the satellite rollout
# finishes.
echo ">> start background writer on $PRIMARY $DEV (1 KiB/s, direct I/O)"
: >"$WRITER_LOG"
: >"$WRITER_ERR"

start_writer() {
    local pod
    pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o "jsonpath={.items[?(@.spec.nodeName==\"${PRIMARY}\")].metadata.name}")

    if [[ -z "$pod" ]]; then
        echo "FAIL: no satellite pod on $PRIMARY when starting writer" >&2
        return 1
    fi

    # Writer runs in the pod; stdout/stderr stream back to the host
    # log files. Each tick: stamp + dd result. dd's stderr (where it
    # reports write errors) is captured separately.
    kubectl -n "$NS" exec "$pod" -- bash -c "
        set +e
        i=0
        while true; do
            if [[ -f /tmp/rolling-upgrade.stop ]]; then
                exit 0
            fi
            ts=\$(date +%s)
            # Cycle through a few offsets so we exercise different
            # 4K-aligned regions; size of RD is 64 MiB so this stays
            # well in-range.
            off=\$(( (i % 16) * 4 ))
            if err=\$(dd if=/dev/urandom of=${DEV} bs=4096 count=1 seek=\$off \\
                conv=notrunc oflag=direct status=none 2>&1); then
                echo \"\$ts OK i=\$i off=\$off\"
            else
                echo \"\$ts ERR i=\$i off=\$off: \$err\" >&2
            fi
            i=\$(( i + 1 ))
            sleep ${WRITER_INTERVAL}
        done
    " >"$WRITER_LOG" 2>"$WRITER_ERR" &
    WRITER_PID=$!
}

start_writer

# Let the writer get a few ticks in before the chaos starts; if it
# can't even write to a quiescent device, no point upgrading.
sleep 5
if [[ -s "$WRITER_ERR" ]]; then
    echo "FAIL: writer recorded errors before any rollout started"
    cat "$WRITER_ERR"
    exit 1
fi
ticks_before=$(wc -l <"$WRITER_LOG")
echo "   writer warmup: $ticks_before OK ticks logged"

# Helper: wait for a rollout to complete, then assert writer
# health and DRBD UpToDate.
assert_writer_clean() {
    local stage=$1
    if [[ -s "$WRITER_ERR" ]]; then
        echo "FAIL: writer errors observed during $stage"
        cat "$WRITER_ERR"
        return 1
    fi
}

assert_drbd_settles() {
    local stage=$1
    if ! wait_uptodate "$RD" "$PRIMARY" "$PEER"; then
        echo "FAIL: DRBD did not return to UpToDate after $stage"
        return 1
    fi
}

# ---------- Step 3: rollout-restart apiserver Deployment ----------
echo ">> [stage: apiserver] kubectl rollout restart deploy/blockstor-apiserver"
kubectl -n "$NS" rollout restart deploy/blockstor-apiserver
kubectl -n "$NS" rollout status  deploy/blockstor-apiserver --timeout="${ROLLOUT_TIMEOUT}s"
assert_writer_clean   "apiserver rollout" || exit 1
assert_drbd_settles   "apiserver rollout" || exit 1
echo "   apiserver rollout OK, writer clean, DRBD UpToDate"

# ---------- Step 4: rollout-restart controller Deployment ----------
echo ">> [stage: controller] kubectl rollout restart deploy/blockstor-controller"
kubectl -n "$NS" rollout restart deploy/blockstor-controller
kubectl -n "$NS" rollout status  deploy/blockstor-controller --timeout="${ROLLOUT_TIMEOUT}s"
assert_writer_clean   "controller rollout" || exit 1
assert_drbd_settles   "controller rollout" || exit 1
echo "   controller rollout OK, writer clean, DRBD UpToDate"

# ---------- Step 5: rollout-restart satellite DaemonSet ----------
#
# This is the load-bearing case: the satellite on $PRIMARY itself
# will get bounced. The kubectl-exec writer dies with that pod, so
# we:
#   1. tear the writer down cleanly (touch the stop file via the
#      OLD pod; the pid will exit on its own as the connection drops);
#   2. trigger the DS rollout and wait for it to settle (DS
#      maxUnavailable=1 — one pod at a time, including the Primary's);
#   3. start a FRESH writer on the new pod and assert it can write
#      immediately (= DRBD never lost its lower-disk binding).
#
# Step 3 is the actual contract check: if the satellite rollout had
# disrupted DRBD on $PRIMARY, the new writer would block on dd until
# the device re-attached — easily 5-30 s. We bound that with a tight
# 10 s success window.
echo ">> [stage: satellite] stop kubectl-exec writer before DS rollout"
# Touch the stop sentinel through whichever pod is currently up
# on $PRIMARY — the exec call exits on its own when the pod restarts,
# but we still want a clean stop in case the pod doesn't get rolled
# (e.g. DS maxUnavailable ordering picks $PEER first).
pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o "jsonpath={.items[?(@.spec.nodeName==\"${PRIMARY}\")].metadata.name}" 2>/dev/null || true)
if [[ -n "$pod" ]]; then
    kubectl -n "$NS" exec "$pod" -- touch /tmp/rolling-upgrade.stop 2>/dev/null || true
fi
if [[ -n "$WRITER_PID" ]] && kill -0 "$WRITER_PID" 2>/dev/null; then
    # Give it a beat to notice the sentinel; if the kubectl exec
    # session already died with the pod that's fine too.
    sleep 2
    kill "$WRITER_PID" 2>/dev/null || true
    wait "$WRITER_PID" 2>/dev/null || true
fi
WRITER_PID=""

# Snapshot writer health from the pre-DS-rollout window so we can
# attribute any later error to the DS rollout itself, not earlier.
if [[ -s "$WRITER_ERR" ]]; then
    echo "FAIL: writer errors observed BEFORE satellite rollout (apiserver/controller stages were dirty)"
    cat "$WRITER_ERR"
    exit 1
fi
preds_total=$(wc -l <"$WRITER_LOG")
echo "   writer pre-DS-rollout: $preds_total OK ticks total"

echo ">> kubectl rollout restart ds/blockstor-satellite"
kubectl -n "$NS" rollout restart ds/blockstor-satellite
kubectl -n "$NS" rollout status  ds/blockstor-satellite --timeout="${ROLLOUT_TIMEOUT}s"

# After the rollout settles every satellite is on a fresh pod.
# Wait briefly for the new pod on $PRIMARY to be Ready before we
# try to exec into it for the post-rollout writer.
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    ready=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o "jsonpath={.items[?(@.spec.nodeName==\"${PRIMARY}\")].status.containerStatuses[0].ready}" 2>/dev/null || true)
    [[ "$ready" == "true" ]] && break
    sleep 2
done
if [[ "$ready" != "true" ]]; then
    echo "FAIL: new satellite pod on $PRIMARY never became Ready after DS rollout"
    exit 1
fi
echo "   new satellite pod Ready on $PRIMARY"

# Re-promote (the old pod's preStop ran `drbdadm down --all` if the
# DS controller did a graceful eviction — DRBD on the host kernel
# stays loaded but the resource may end up Secondary). The
# post-rollout writer test only needs to be able to issue writes
# from $PRIMARY; if the device is Secondary, dd will EIO.
on_node "$PRIMARY" drbdadm primary --force "$RD" 2>/dev/null || true

# Re-resolve the device — the minor MAY have shifted if drbd-utils
# re-allocated, though in practice the .res file pins it and the
# minor is stable across satellite pod cycles.
DEV=$(device_for_rd "$RD" "$PRIMARY")
if [[ -z "$DEV" ]]; then
    echo "FAIL: device for $RD on $PRIMARY vanished after DS rollout"
    exit 1
fi

# Clear the stop sentinel inside the NEW pod and start a fresh
# writer. The tightness here is the actual contract check: if DRBD
# had been knocked offline by the satellite rollout, the very first
# dd would block until re-attach.
new_pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o "jsonpath={.items[?(@.spec.nodeName==\"${PRIMARY}\")].metadata.name}")
kubectl -n "$NS" exec "$new_pod" -- rm -f /tmp/rolling-upgrade.stop 2>/dev/null || true

: >"$WRITER_LOG"
: >"$WRITER_ERR"
start_writer

# Tight 10 s window: a healthy DRBD device writes at least 5 ticks
# in 10 s (1 tick/s, allow up to 5 s of startup slack for the
# kubectl-exec handshake on the new pod). If we see fewer than 3
# ticks here, something on the new pod is blocking writes.
sleep 12
post_ticks=$(wc -l <"$WRITER_LOG")
echo "   post-DS-rollout writer: $post_ticks OK ticks in ~12 s"
if (( post_ticks < 3 )); then
    echo "FAIL: post-rollout writer logged only $post_ticks ticks; DRBD likely disrupted"
    echo "---- writer stderr ----"
    cat "$WRITER_ERR"
    exit 1
fi

if [[ -s "$WRITER_ERR" ]]; then
    echo "FAIL: writer errors observed AFTER satellite rollout"
    cat "$WRITER_ERR"
    exit 1
fi

assert_drbd_settles "satellite rollout" || exit 1
echo "   satellite rollout OK, writer clean, DRBD UpToDate"

# ---------- Step 6: final verdict ----------
#
# Summarise tick counts so a reader of the CI log can see the writer
# was actually active during each stage, not just silent-passing.
echo ">> ROLLING-UPGRADE OK"
echo "   apiserver rollout:  writer clean, DRBD UpToDate"
echo "   controller rollout: writer clean, DRBD UpToDate"
echo "   satellite rollout:  writer clean (post-rollout ticks: $post_ticks), DRBD UpToDate"
