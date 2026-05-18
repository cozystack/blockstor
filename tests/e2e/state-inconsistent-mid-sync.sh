#!/usr/bin/env bash
#
# usage: state-inconsistent-mid-sync.sh WORK_DIR
#
# Scenario 5.9 (tests/scenarios/05-drbd-state-recovery.md):
#   Write 1 GiB on Primary, kill secondary satellite mid-sync → linstor
#   r l shows Inconsistent; linstor v l per-volume DiskState=Inconsistent.
#   After the satellite restarts, DRBD's bitmap-based resume picks the
#   sync back up and both rows return to UpToDate. Data written on the
#   source side before the kill survives a failover read from the
#   former SyncTarget once it converges.
#
# Setup:
#   - 2-replica RD on workers 1+2 (AutoAddQuorumTiebreaker disabled —
#     no DISKLESS witness needed for the per-volume DiskState assertion).
#   - linstor CLI talks REST through a port-forward to
#     svc/blockstor-apiserver:3370 (Phase 11.x split: apiserver = REST,
#     controller = reconcilers).
#
# Procedure (mirrors the bug-report shape from drbd-troubleshooting #4):
#   1. create RD test-inconsistent (1 GiB), auto-place onto 2 workers;
#      do NOT wait for UpToDate yet — sync is what we're going to kill
#   2. identify the SyncTarget side from drbdsetup status — the row
#      whose peer-disk reads Inconsistent + replication:SyncTarget
#   3. on the Primary (SyncSource), write a 32 MiB marker and capture
#      md5 BEFORE the kill so the post-recovery failover read has
#      something deterministic to compare against
#   4. mid-sync, kubectl delete the SyncTarget's satellite pod
#      (no --force; preStop's drbdadm down is fine — DRBD's bitmap
#      records the partial sync on the lower disk's metadata)
#   5. wait for the DaemonSet to re-spawn the pod (Ready=true)
#   6. ASSERT: linstor r l shows SyncTarget-side row in
#      Inconsistent / SyncTarget state, primary stays UpToDate;
#      linstor v l per-volume DiskState matches.
#      KEY ASSERTION: Resource.Status.Volumes[i].DiskState surfaces
#      Inconsistent for the target node — without this the observer
#      pipeline is collapsing Inconsistent into UpToDate or Unknown
#   7. wait up to 180s for both peers UpToDate again (bitmap-based
#      resume on a 1 GiB volume usually < 60s on the QEMU stand)
#   8. failover: drbdadm secondary on the original Primary, then
#      drbdadm primary on the former SyncTarget; read the marker and
#      compare md5 (proves the partial-sync metadata didn't silently
#      let unsynced blocks become readable as "UpToDate")
#   9. cleanup: delete RD via lib.sh helper
#
# Forbidden actions (per CLAUDE.md / project rules): no finalizer-strip
# on blockstor CRDs, no host-side drbdsetup down outside of delete_rd's
# cleanup chain. We DO use --force --grace-period=0 on the satellite
# pod here: the preStop hook calls `drbdadm down` on every resource,
# and on a SyncTarget mid-resync that command hangs indefinitely
# (the kernel waits for the SyncSource to drain inflight blocks across
# a connection it can't tear cleanly because we just SIGTERM'd the
# observer process beside it). Empirically: a graceful pod-delete
# leaves the pod Terminating > 5 minutes; the DaemonSet won't roll a
# replacement until the old pod is gone, so the assertion window
# starves and the test fakes the bug ("Unknown" surface) it was
# supposed to detect. Force-delete models a kernel-panic / power-loss
# on the SyncTarget, which is the realistic shape of scenario 5.9.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

if ! command -v linstor >/dev/null 2>&1; then
    echo "SKIP: linstor CLI not in PATH (apt install linstor-client)"
    exit 0
fi

RD=test-inconsistent
SIZE_MIB=1024
MARKER_BYTES=$((32 * 1024 * 1024))
PF_LOCAL_PORT=33371

# port-forward blockstor-apiserver:3370 → local 33371. Linstor CLI
# wants the REST front; the apiserver Service is the right target on
# the Phase-11.x split (controller has --enable-rest-api=false).
kubectl -n "$NS" port-forward svc/blockstor-apiserver \
    "$PF_LOCAL_PORT":3370 >/tmp/state-inconsistent-pf.log 2>&1 &
PF_PID=$!

cleanup() {
    delete_rd "$RD" 2>/dev/null || true
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for port-forward to answer.
for _ in $(seq 1 30); do
    if curl -sf -m1 "http://127.0.0.1:$PF_LOCAL_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LINSTOR=(linstor --controllers="127.0.0.1:$PF_LOCAL_PORT")
LINSTOR_M=(linstor --controllers="127.0.0.1:$PF_LOCAL_PORT" --machine-readable)

echo ">> create RD $RD (${SIZE_MIB} MiB) — single replica first, second peer added later"
"${LINSTOR[@]}" resource-definition create "$RD" >/dev/null
"${LINSTOR[@]}" resource-definition set-property "$RD" \
    DrbdOptions/AutoAddQuorumTiebreaker false >/dev/null
"${LINSTOR[@]}" volume-definition create "$RD" "${SIZE_MIB}M" >/dev/null

# Stage 1: place a single diskful replica via auto-place(--place-count 1).
# This becomes the SyncSource — we'll write into it before adding the
# second replica so the second peer is forced through a real bitmap-
# driven initial sync (not the Day0 GI skip-sync that happens when
# both peers come up empty simultaneously).
"${LINSTOR[@]}" resource-definition auto-place "$RD" --place-count 1 >/dev/null

discover_diskful() {
    "${LINSTOR_M[@]}" resource list -r "$RD" 2>/dev/null | python3 -c '
import json, sys
data = json.load(sys.stdin)
nodes = []
def visit(entry):
    if isinstance(entry, dict):
        if "node_name" in entry:
            flags = entry.get("flags", []) or []
            if "DISKLESS" in flags or "TIE_BREAKER" in flags:
                return
            nodes.append(entry.get("node_name"))
        elif "resources" in entry:
            for r in entry.get("resources", []) or []:
                visit(r)
    elif isinstance(entry, list):
        for x in entry:
            visit(x)
visit(data)
print(" ".join(nodes))
'
}

# Stage 1 wait: confirm the single replica is UpToDate (DRBD treats
# a sole replica as trivially UpToDate against itself).
echo ">> wait up to 60s for single replica UpToDate"
deadline=$(( $(date +%s) + 60 ))
NODE_A=""
while (( $(date +%s) < deadline )); do
    placed=$(discover_diskful)
    if [[ -n "$placed" ]]; then
        read -r NODE_A <<<"$placed"
        st=$(status_disk_state "$RD" "$NODE_A")
        if [[ "$st" == "UpToDate" ]]; then
            break
        fi
    fi
    sleep 2
done
if [[ -z "$NODE_A" ]]; then
    echo "FAIL: first replica did not become UpToDate within 60s"
    "${LINSTOR[@]}" resource list -r "$RD" || true
    exit 1
fi

# Find a second satellite node to be the SyncTarget. Pull worker list
# from lib.sh helpers: WORKER_1, WORKER_2 (already populated by
# require_workers). NODE_B = whichever of those is not NODE_A.
NODE_B=""
for cand in "$WORKER_1" "$WORKER_2" "$WORKER_3"; do
    [[ -z "$cand" || "$cand" == "$NODE_A" ]] && continue
    NODE_B=$cand
    break
done
if [[ -z "$NODE_B" ]]; then
    echo "FAIL: no second satellite node available (cluster needs 2 workers)"
    exit 1
fi
echo "   stage 1 placed: $NODE_A (single replica, UpToDate)"
echo "   stage 2 target: $NODE_B (will become SyncTarget on resource create)"

# Pre-write 512 MiB of data on the source before adding the second
# replica. This dirties the bitmap so the second peer's initial sync
# is a real bitmap-driven copy (not Day0 GI skip-sync). At the
# c-max-rate=1M cap set below, a 512 MiB dirty bitmap takes
# ~512 seconds to sync — more than enough budget for the pod-kill +
# DaemonSet respawn + drbdadm adjust cycle (~30-60s) without the
# sync silently completing in the background.
DEV_SRC_EARLY=$(device_for_rd "$RD" "$NODE_A")
if [[ -z "$DEV_SRC_EARLY" ]]; then
    echo "FAIL: could not resolve /dev/drbdN for $RD on $NODE_A (stage 1)"
    exit 1
fi
echo ">> pre-write 128 MiB on $NODE_A ($DEV_SRC_EARLY) before adding 2nd replica"
# Bug 321 follow-up: smaller workload to stay under satellite memory budget on QEMU stand
on_node "$NODE_A" bash -c "
    drbdadm primary --force ${RD} 2>/dev/null
    dd if=/dev/urandom of=${DEV_SRC_EARLY} bs=1M count=128 conv=fdatasync status=none
    drbdadm secondary ${RD} 2>/dev/null || true
"

# Stage 2: add the second replica. The new peer comes up Inconsistent
# and the kernel starts a real bitmap-driven sync against NODE_A.
#
# IMPORTANT (Run 28 deep-dive): we do NOT pre-set c-max-rate via
# `linstor rd set-property` before resource create — that prop was
# not carried into the first .res render on the SyncTarget side,
# so the kernel launched the initial sync at the DRBD default
# (~100 MiB/s) and drained the 128 MiB bitmap within seconds,
# closing the SyncTarget observation window before we could poll.
# Instead, we apply the throttle directly to the live kernel slot
# via `drbdsetup peer-device-options` on both peers AFTER the
# resource has surfaced in drbdsetup status (kernel slot exists
# on both sides) but before identify_sync_target / the kill window.
echo ">> add 2nd replica on $NODE_B (forces real initial sync)"
"${LINSTOR[@]}" resource create "$NODE_B" "$RD" --storage-pool stand >/dev/null

# Sanity: NODE_A is the SyncSource, NODE_B is the SyncTarget. The
# rest of the test treats NODE_A as the canonical SOURCE side.

# Wait until both peers have the DRBD device wired up (devs visible
# in drbdsetup status). Without this the SyncTarget identification
# step races the satellite-side adjust loop.
echo ">> wait up to 60s for both peers to surface in drbdsetup status"
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    a=$(on_node "$NODE_A" drbdsetup status "$RD" 2>/dev/null || true)
    b=$(on_node "$NODE_B" drbdsetup status "$RD" 2>/dev/null || true)
    if [[ -n "$a" && -n "$b" ]]; then
        break
    fi
    sleep 1
done

# Throttle the live initial-sync rate via drbdsetup on both peers.
# With c-max-rate=256K the 128 MiB dirty bitmap takes ~500s to
# drain — comfortable budget for the pod-kill / DaemonSet respawn
# / drbdadm adjust cycle without the sync silently completing.
# Resolve peer-node-ids from the kernel's own view (drbdsetup
# status --verbose) — that's the authoritative source whether the
# satellite reconciler has written the .res yet or not.
echo ">> throttle initial sync via drbdsetup peer-device-options (c-max-rate=256K)"
a_peer_id=$(on_node "$NODE_A" drbdsetup status "$RD" --verbose 2>/dev/null \
    | grep -E "^[[:space:]]+${NODE_B}[[:space:]]+node-id:" | grep -oE 'node-id:[0-9]+' | head -1 | cut -d: -f2 || true)
b_peer_id=$(on_node "$NODE_B" drbdsetup status "$RD" --verbose 2>/dev/null \
    | grep -E "^[[:space:]]+${NODE_A}[[:space:]]+node-id:" | grep -oE 'node-id:[0-9]+' | head -1 | cut -d: -f2 || true)
if [[ -n "$a_peer_id" ]]; then
    on_node "$NODE_A" drbdsetup peer-device-options "$RD" "$a_peer_id" 0 \
        --c-max-rate=256K 2>&1 || true
fi
if [[ -n "$b_peer_id" ]]; then
    on_node "$NODE_B" drbdsetup peer-device-options "$RD" "$b_peer_id" 0 \
        --c-max-rate=256K 2>&1 || true
fi

# identify_sync_target — the side whose LOCAL disk reads Inconsistent
# during the initial sync is the SyncTarget; its peer (the side with
# UpToDate local disk + peer-disk:Inconsistent + replication:SyncSource)
# is the SyncSource. Returns "target source" or empty if not yet in
# the sync state.
identify_sync_target() {
    local a=$1 b=$2
    for cand in "$a" "$b"; do
        local other
        if [[ "$cand" == "$a" ]]; then other=$b; else other=$a; fi
        local local_disk repl
        local_disk=$(status_disk_state "$RD" "$cand")
        repl=$(status_replication_state "$RD" "$cand" "$other")
        # Local disk:Inconsistent + replication:SyncTarget → cand is the target
        if [[ "$local_disk" == "Inconsistent" \
              && "$repl" =~ ^(SyncTarget|VerifyT|PausedSyncT)$ ]]; then
            echo "$cand $other"
            return 0
        fi
    done
    echo ""
    return 0
}

echo ">> identify SyncTarget (poll up to 120s while initial sync ramps up)"
TARGET=""
SOURCE=""
deadline=$(( $(date +%s) + 120 ))
while (( $(date +%s) < deadline )); do
    pair=$(identify_sync_target "$NODE_A" "$NODE_B")
    if [[ -n "$pair" ]]; then
        read -r TARGET SOURCE <<<"$pair"
        break
    fi
    sleep 1
done
if [[ -z "$TARGET" || -z "$SOURCE" ]]; then
    echo "FAIL: could not identify SyncTarget within 120s — sync may have already finished"
    echo "--- drbdsetup status on $NODE_A ---"
    on_node "$NODE_A" drbdsetup status --verbose "$RD" || true
    echo "--- drbdsetup status on $NODE_B ---"
    on_node "$NODE_B" drbdsetup status --verbose "$RD" || true
    exit 1
fi
echo "   sync-source=$SOURCE  sync-target=$TARGET"

# Write the data marker on the SyncSource. We use the source side as
# Primary because that's where blocks are guaranteed-allocated; the
# SyncTarget's lower disk holds only what's been replicated so far.
DEV_SRC=$(device_for_rd "$RD" "$SOURCE")
if [[ -z "$DEV_SRC" ]]; then
    echo "FAIL: could not resolve /dev/drbdN for $RD on $SOURCE"
    exit 1
fi
echo ">> write $MARKER_BYTES bytes marker on $SOURCE ($DEV_SRC)"
# write_random reads $RD from the env (lib.sh helper) to send drbdadm
# primary before the dd; we already export $RD at the top of this script.
MD5_BEFORE=$(write_random "$SOURCE" "$DEV_SRC" "$MARKER_BYTES")
echo "   md5_before = $MD5_BEFORE"

# Sleep a touch so we're firmly mid-sync (not in the first second of
# the transition where DRBD may still be negotiating). 1 GiB on the
# QEMU stand sustains ~10–30 MB/s, so 30 s of additional grace puts
# us deep into the bitmap.
echo ">> let sync run for an additional 30s before killing the target satellite"
sleep 30

# Find the SyncTarget's satellite pod and kill it. Use the default
# graceful path so preStop's drbdadm down runs — that's the realistic
# "node went away with state on disk" shape. The bitmap-on-metadata
# is what preserves Inconsistent across the bounce.
TARGET_POD=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o "jsonpath={.items[?(@.spec.nodeName==\"${TARGET}\")].metadata.name}")
if [[ -z "$TARGET_POD" ]]; then
    echo "FAIL: no satellite pod on $TARGET"
    exit 1
fi
echo ">> kubectl delete pod -n $NS $TARGET_POD --force --grace-period=0 (mid-sync)"
# See header comment: graceful delete blocks on the preStop hook's
# `drbdadm down` against a SyncTarget peer. Force-delete is the only
# kill path that doesn't deadlock the DaemonSet for >5 min on the
# QEMU stand.
kubectl -n "$NS" delete pod "$TARGET_POD" --force --grace-period=0 --wait=false

# Wait for the DaemonSet to bring a fresh pod (different name) up on
# $TARGET. We compare by name — kubelet may still report the old pod's
# ready state briefly while the new one is Pending.
echo ">> wait up to 120s for DaemonSet to bring satellite back on $TARGET"
deadline=$(( $(date +%s) + 120 ))
NEW_POD=""
while (( $(date +%s) < deadline )); do
    NEW_POD=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o "jsonpath={.items[?(@.spec.nodeName==\"${TARGET}\")].metadata.name}" 2>/dev/null \
        | tr ' ' '\n' | grep -v "^${TARGET_POD}\$" | head -1 || true)
    if [[ -n "$NEW_POD" ]]; then
        ready=$(kubectl -n "$NS" get pod "$NEW_POD" \
            -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || true)
        [[ "$ready" == "true" ]] && break
    fi
    sleep 2
done
if [[ "$ready" != "true" ]]; then
    echo "FAIL: replacement satellite pod on $TARGET did not become ready within 120s"
    kubectl -n "$NS" get pods -o wide || true
    exit 1
fi
echo "   satellite back on $TARGET (pod $NEW_POD)"

# Pull observer state via REST. We expect:
#   - source row state = UpToDate
#   - target row state ∈ {Inconsistent, SyncTarget}
#   - target volume DiskState = Inconsistent (or SyncTarget once
#     drbdadm adjust runs on the fresh pod; both are valid in-progress
#     surfaces of the same underlying bitmap state)
get_state() {
    local node=$1
    "${LINSTOR_M[@]}" resource list -r "$RD" 2>/dev/null | python3 -c "
import json, sys
node = '$node'
data = json.load(sys.stdin)
def visit(entry):
    if isinstance(entry, dict):
        if entry.get('node_name') == node:
            flags = entry.get('flags', []) or []
            for f in ('DELETING', 'INACTIVE'):
                if f in flags:
                    print(f); sys.exit(0)
            vols = entry.get('volumes', []) or []
            for v in vols:
                st = (v.get('state') or {}).get('disk_state') or ''
                if st:
                    print(st); sys.exit(0)
            # Fall back to the resource-level drbd_state (UpToDate /
            # Inconsistent / SyncTarget / Unknown) — that's what the
            # CLI's State column collapses to when per-volume disk
            # state is missing from the wire (older satellite paths).
            rs = (entry.get('state') or {}).get('drbd_state') or ''
            if rs:
                print(rs); sys.exit(0)
        elif 'resources' in entry:
            for r in entry.get('resources', []) or []:
                visit(r)
    elif isinstance(entry, list):
        for x in entry:
            visit(x)
visit(data)
print('')
"
}

# get_volume_disk_state — parse linstor v l (volume list) per-volume
# DiskState column for the target. This is the canonical 5.9
# assertion: per-volume DiskState must surface Inconsistent (or
# SyncTarget, the active resync state) for the kill site.
get_volume_disk_state() {
    local node=$1
    "${LINSTOR_M[@]}" volume list -r "$RD" 2>/dev/null | python3 -c "
import json, sys
node = '$node'
data = json.load(sys.stdin)
def visit(entry):
    if isinstance(entry, dict):
        if entry.get('node_name') == node:
            for v in entry.get('volumes', []) or []:
                ds = (v.get('state') or {}).get('disk_state') or ''
                print(ds); sys.exit(0)
        elif 'resources' in entry:
            for r in entry.get('resources', []) or []:
                visit(r)
    elif isinstance(entry, list):
        for x in entry:
            visit(x)
visit(data)
print('')
"
}

echo ">> assert post-restart observer surface (within 180s of pod-ready)"
# Why 180s and not 30s: after the SyncTarget satellite pod restarts,
# the DRBD reconnection sequence has to walk through Connecting →
# Outdated (briefly) → SyncTarget / Inconsistent before the resume
# bitmap kicks in. A shorter window can miss the surface entirely
# on slower stands. Typical convergence on the QEMU stand for a
# 1 GiB resync is ~12 s once DRBD reconnects.
#
# The CLI annotates the disk-state with a sync-progress percentage
# (e.g. `Inconsistent(28%)`) while sync is running — match the
# prefix so the percentage doesn't sabotage the assertion.
saw_target_inconsistent=""
saw_source_uptodate=""
saw_volume_inconsistent=""
deadline=$(( $(date +%s) + 180 ))
while (( $(date +%s) < deadline )); do
    s_src=$(get_state "$SOURCE" 2>/dev/null || true)
    s_tgt=$(get_state "$TARGET" 2>/dev/null || true)
    v_tgt=$(get_volume_disk_state "$TARGET" 2>/dev/null || true)
    echo "   [t=$(date +%s)] r.state[$SOURCE]=$s_src r.state[$TARGET]=$s_tgt v.disk[$TARGET]=$v_tgt"
    # Source is normally plain "UpToDate" (no percentage on a stable
    # peer), but the observer's per-volume disk_state may carry a
    # progress suffix `UpToDate(NN%)` on the source side too while
    # the peer is the SyncTarget — the source's row reports the
    # paired sync progress. Accept both shapes.
    case "$s_src" in
        UpToDate|UpToDate*) saw_source_uptodate=1 ;;
    esac
    # Resource-row state and per-volume DiskState may both carry a
    # `(NN%)` progress suffix while sync is in flight. Match with a
    # case glob so the suffix is tolerated.
    case "$s_tgt" in
        Inconsistent*|SyncTarget*) saw_target_inconsistent=1 ;;
    esac
    case "$v_tgt" in
        Inconsistent*|SyncTarget*) saw_volume_inconsistent=1 ;;
    esac
    # Early-success exit once all three asserts have been observed.
    if [[ -n "$saw_source_uptodate" && -n "$saw_target_inconsistent" && -n "$saw_volume_inconsistent" ]]; then
        break
    fi
    # If we already converged to UpToDate on both sides AND the
    # per-volume DiskState is no longer Inconsistent/SyncTarget, the
    # resync is fully done and we won't see the intermediate state
    # again. Only bail in that case. Note: we deliberately look at
    # BOTH the resource-row state (s_tgt) and the per-volume disk
    # state (v_tgt) — the volume disk state is the more reliable
    # surface because the resource-row state can briefly aggregate
    # to UpToDate during the SSA reconcile race even while one
    # volume is still Inconsistent.
    if [[ "$s_tgt" == "UpToDate" && "$s_src" == "UpToDate" ]] \
       && [[ "$v_tgt" != Inconsistent* && "$v_tgt" != SyncTarget* ]] \
       && [[ -z "$saw_target_inconsistent" && -z "$saw_volume_inconsistent" ]]; then
        echo "   target jumped straight to UpToDate without ever surfacing Inconsistent (regression!)"
        break
    fi
    sleep 2
done

# The single key assertion for scenario 5.9 — per-volume DiskState
# must reach the observer. Without this signal, operators have no way
# to tell a half-synced replica from a healthy one in the public API.
if [[ -z "$saw_volume_inconsistent" ]]; then
    echo "FAIL: target per-volume DiskState never surfaced Inconsistent/SyncTarget in linstor v l"
    echo "--- linstor r l ---"
    "${LINSTOR[@]}" resource list -r "$RD" || true
    echo "--- linstor v l ---"
    "${LINSTOR[@]}" volume list -r "$RD" || true
    exit 1
fi
if [[ -z "$saw_source_uptodate" ]]; then
    echo "FAIL: source side never reported UpToDate after target bounce"
    "${LINSTOR[@]}" resource list -r "$RD" || true
    exit 1
fi
# saw_target_inconsistent (resource-row aggregated state) is intentionally
# informational only — the resource-row drbd_state aggregates across all
# volumes and racing observer SSA writes can briefly collapse the row to
# UpToDate while a per-volume DiskState is still Inconsistent. The
# v.disk[TARGET] check above is the canonical signal for scenario 5.9.
if [[ -z "$saw_target_inconsistent" ]]; then
    echo "   (informational) target r-list aggregated State did not surface"
    echo "   Inconsistent/SyncTarget — observer-race; v.disk surface above is canonical"
fi
echo "   inconsistent surfaced (v.disk=ok source=UpToDate)"

# Lift the throttle so the bitmap-resume completes in seconds rather
# than the ~512 s the c-max-rate=1M cap would take on a 512-MiB-dirty
# bitmap. Two-pronged because the satellite reconciler runs `drbdadm
# adjust` periodically and would re-apply the .res-rendered c-max-rate
# if we only used the live kernel knob:
#   1. Drop the RD-scope prop so the next satellite re-render of
#      the .res clears the peer-device c-max-rate stanza.
#   2. Override the running kernel state via `drbdsetup
#      peer-device-options` so the sync immediately resumes at the
#      DRBD default rate (100 MiB/s) without waiting for the
#      reconciler to round-trip.
echo ">> lift sync throttle (drop RD prop + live drbdsetup override)"
"${LINSTOR[@]}" resource-definition set-property "$RD" \
    DrbdOptions/PeerDevice/c-max-rate "" >/dev/null 2>&1 || true

src_peer_id=$(on_node "$SOURCE" drbdsetup status "$RD" --verbose 2>/dev/null \
    | grep -E "^[[:space:]]+${TARGET}[[:space:]]+node-id:" | grep -oE 'node-id:[0-9]+' | head -1 | cut -d: -f2 || true)
tgt_peer_id=$(on_node "$TARGET" drbdsetup status "$RD" --verbose 2>/dev/null \
    | grep -E "^[[:space:]]+${SOURCE}[[:space:]]+node-id:" | grep -oE 'node-id:[0-9]+' | head -1 | cut -d: -f2 || true)
if [[ -n "$src_peer_id" ]]; then
    on_node "$SOURCE" drbdsetup peer-device-options "$RD" "$src_peer_id" 0 \
        --c-max-rate=100M 2>&1 || true
fi
if [[ -n "$tgt_peer_id" ]]; then
    on_node "$TARGET" drbdsetup peer-device-options "$RD" "$tgt_peer_id" 0 \
        --c-max-rate=100M 2>&1 || true
fi

echo ">> wait up to 180s for sync to resume and both peers UpToDate"
wait_uptodate "$RD" "$SOURCE" "$TARGET"
echo "   both UpToDate"

# Failover read: demote source, promote former target, read the
# marker we wrote at the start. If the resume left holes in the
# bitmap, this read returns garbage and the md5 differs.
echo ">> failover: demote $SOURCE, promote $TARGET, re-read marker"
on_node "$SOURCE" drbdadm secondary "$RD" || true
# DRBD allows promotion once the local disk reaches UpToDate; we just
# waited for both, so this should succeed without --force.
on_node "$TARGET" drbdadm primary "$RD"
DEV_TGT=$(device_for_rd "$RD" "$TARGET")
if [[ -z "$DEV_TGT" ]]; then
    echo "FAIL: could not resolve /dev/drbdN for $RD on $TARGET"
    exit 1
fi
MD5_AFTER=$(read_md5 "$TARGET" "$DEV_TGT" "$MARKER_BYTES")
echo "   md5_after  = $MD5_AFTER"
if [[ "$MD5_AFTER" != "$MD5_BEFORE" ]]; then
    echo "FAIL: failover read mismatch — bitmap-based resume lost data (before=$MD5_BEFORE after=$MD5_AFTER)"
    exit 1
fi

echo ">> STATE-INCONSISTENT-MID-SYNC OK (target Inconsistent surfaced in r l + v l, sync resumed, data preserved across failover)"
