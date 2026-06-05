#!/usr/bin/env bash
#
# usage: u145-write-then-add-replica-syncs.sh WORK_DIR
#
# L6 cli-matrix cell — U145 (P0, DATA INTEGRITY).
#
# Upstream LINSTOR user report: with 1 diskful + 1 diskless(InUse), a
# `r c -s <pool> <node3> <res>` brought the NEW diskful replica up
# UpToDate IMMEDIATELY without syncing from the existing diskful — a
# silent empty replica presenting as a good copy, promotable on failover
# → data loss.
#
# For blockstor this is THE critical interaction with our skip-init-sync
# design: a brand-new replica may take the day0 GI seed-skip ONLY when no
# data-bearing diskful peer exists. Once real data has been written
# (RD.Spec.Initialized latched true), a replica added to that RD MUST be
# stamped SkipInitialSync=false by the controller and come up
# Inconsistent → SyncTarget, NEVER instant-UpToDate.
#
# This cell pins the NEGATIVE at operator level with kernel + content
# ground truth (the L1 controller/satellite seed-decision tests
# TestSkipInitSyncDataBearingPeerLatchesAndNewReplicaSyncs /
# TestResolveVolumeSeedReadsSkipInitialSyncSpecFlag cover the decision;
# only the stand observes the real DRBD transition + post-sync content):
#
#   1. Create a 2-diskful RD on a thin pool (skip-init-sync-eligible).
#   2. Bind a PVC + writer pod, write a known 64 MiB pattern, record md5.
#      The write advances the source past day0 → RD latches Initialized.
#   3. `r c <node3> <rd> -s <pool>` — add a 3rd diskful replica.
#   4. NEGATIVE assertion: the new replica must NOT be instant-UpToDate.
#      Within the first ~6s it MUST be observed Inconsistent / SyncTarget
#      (it is seeding from the data-bearing peer). An immediate UpToDate
#      with no transient sync state is the U145 bug fingerprint.
#   5. The new replica then converges UpToDate within 180s.
#   6. CONTENT assertion: the writer pod's md5 over the original region is
#      UNCHANGED — proving the new replica did not corrupt / the cluster
#      did not serve an empty copy.
#
# Pre-flight SKIPs cleanly (exit 0) when the thin pool or a CSI
# StorageClass for the existing RD is unavailable.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

linstor_cli_setup

RD=cli-matrix-u145
POOL=${POOL:-lvm-thin}
PVC_NS=${PVC_NS:-default}
PVC=u145-pvc
POD=u145-writer
MOUNT=/data
ANCHOR="$MOUNT/u145.bin"

N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

cleanup() {
    kubectl -n "$PVC_NS" delete pod "$POD" --grace-period=5 --ignore-not-found >/dev/null 2>&1 || true
    kubectl -n "$PVC_NS" delete pvc "$PVC" --ignore-not-found >/dev/null 2>&1 || true
    delete_rd "$RD"
    assert_no_orphans "$RD"
    linstor_cli_teardown
}
trap cleanup EXIT

# Pre-flight: thin pool on all three nodes (skip-init-sync is thin/ZFS
# specific; U145's data-bearing-veto must hold there).
echo ">> pre-flight: $POOL SP on $N1 + $N2 + $N3"
sp_json=$("${LCTL[@]}" --machine-readable storage-pool list --storage-pools "$POOL" 2>/dev/null || echo "[]")
have=$(jq -r --arg n1 "$N1" --arg n2 "$N2" --arg n3 "$N3" \
    '[.[]? | .[]? | select(.provider_kind != null) | .node_name] | unique
     | map(select(. == $n1 or . == $n2 or . == $n3)) | length' <<<"$sp_json" 2>/dev/null || echo 0)
if (( have < 3 )); then
    echo "SKIP: $POOL SP not on all of $N1/$N2/$N3 (got $have) — U145 fixture unavailable"
    exit 0
fi

echo ">> [U145] rd c + vd c (vol-0, 1G) + r c on $N1 + $N2 (-s $POOL)"
"${LCTL[@]}" resource-definition create "$RD" >/dev/null
"${LCTL[@]}" volume-definition create "$RD" 1G >/dev/null
"${LCTL[@]}" resource create "$N1" "$RD" --storage-pool="$POOL" >/dev/null
"${LCTL[@]}" resource create "$N2" "$RD" --storage-pool="$POOL" >/dev/null

echo ">> wait for vol-0 UpToDate on $N1 + $N2"
wait_uptodate "$RD" "$N1" "$N2"

# Bind a PVC over the existing RD + a writer pod. SKIP cleanly if the
# stand has no CSI StorageClass that targets blockstor.
echo ">> [U145] bind PVC + writer pod, write 64 MiB pattern"
if ! create_pvc_for_rd "$PVC_NS" "$PVC" "$RD" 1Gi; then
    echo "SKIP: no usable CSI StorageClass for existing RD — U145 content half unavailable"
    exit 0
fi
create_writer_pod "$PVC_NS" "$POD" "$PVC" "$MOUNT"

# Write a deterministic 64 MiB pattern and fsync, then record its md5.
# /dev/urandom would change every run; a fixed pattern keeps the md5
# baseline reproducible across reruns and human-auditable.
kubectl -n "$PVC_NS" exec "$POD" -- sh -c \
    "yes 'U145-DATA-INTEGRITY-PATTERN' | head -c 67108864 > '$ANCHOR' && sync" >/dev/null
MD5_PRE=$(pod_md5 "$PVC_NS" "$POD" "$ANCHOR")
if [[ -z "$MD5_PRE" ]]; then
    echo "FAIL (U145): could not compute md5 of written anchor — write did not land" >&2
    exit 1
fi
echo "   md5(pre-add) = $MD5_PRE"

# THE NEGATIVE: add a 3rd diskful replica AFTER real data exists. The RD
# is now Initialized, so the controller MUST stamp SkipInitialSync=false
# on the new replica → it comes up Inconsistent and SyncTargets.
echo ">> [U145] r c $N3 $RD -s $POOL (add diskful replica over written data)"
"${LCTL[@]}" resource create "$N3" "$RD" --storage-pool="$POOL" >/dev/null

# NEGATIVE assertion: the new replica must transit through a non-UpToDate
# sync state — it must NOT be instant-UpToDate. Poll fast for ~12s and
# require at least one observation of Inconsistent / SyncTarget / sync
# kernel state on $N3 before it reaches UpToDate.
echo ">> [U145] NEGATIVE: $N3 must seed (Inconsistent/SyncTarget), never instant-UpToDate"
saw_sync=false
reached_up=false
deadline=$(( $(date +%s) + 12 ))
while (( $(date +%s) < deadline )); do
    s3=$(status_disk_state "$RD" "$N3" 0)
    case "$s3" in
        Inconsistent|SyncTarget|Negotiating|Attaching|"")
            # transient seed/observe states — the replica is coming up the
            # legitimate way (empty string = not yet observed, still pre-UpToDate)
            [[ "$s3" == "Inconsistent" || "$s3" == "SyncTarget" ]] && saw_sync=true
            ;;
        UpToDate)
            reached_up=true
            break
            ;;
    esac
    # Kernel-truth cross-check: drbdsetup on $N3 reporting a sync/inconsistent
    # disk or a SyncTarget replication state also counts as "saw sync".
    if on_node "$N3" drbdsetup status "$RD" 2>/dev/null \
        | grep -Eq 'disk:Inconsistent|replication:SyncTarget'; then
        saw_sync=true
    fi
    sleep 1
done

if [[ "$reached_up" == "true" && "$saw_sync" != "true" ]]; then
    echo "FAIL (U145): new replica on $N3 reached UpToDate WITHOUT any observed" >&2
    echo "  Inconsistent/SyncTarget transition — instant-UpToDate over written data" >&2
    echo "  is the data-integrity bug (empty replica presenting as a good copy)." >&2
    on_node "$N3" drbdsetup status "$RD" 2>&1 | sed 's/^/    /' >&2 || true
    exit 1
fi

if [[ "$saw_sync" != "true" ]]; then
    echo "FAIL (U145): never observed $N3 in a seeding state within 12s — could not" >&2
    echo "  confirm the new replica went through the sync path (suspect instant-UpToDate)." >&2
    on_node "$N3" drbdsetup status "$RD" 2>&1 | sed 's/^/    /' >&2 || true
    exit 1
fi
echo "   OK: observed $N3 seeding (Inconsistent/SyncTarget) before UpToDate"

echo ">> [U145] wait all 3 replicas UpToDate (within 180s)"
wait_uptodate "$RD" "$N1" "$N3"
wait_uptodate "$RD" "$N2" "$N3"

# CONTENT assertion: the original written region must be byte-identical
# after the add+sync. If the cluster ever served the new empty replica
# as authoritative (the U145 failure), the md5 would change.
echo ">> [U145] CONTENT: md5 over original 64 MiB region unchanged post-sync"
MD5_POST=$(pod_md5 "$PVC_NS" "$POD" "$ANCHOR")
echo "   md5(post-sync) = $MD5_POST"
if [[ "$MD5_PRE" != "$MD5_POST" ]]; then
    echo "FAIL (U145): anchor md5 changed across add+sync (pre=$MD5_PRE post=$MD5_POST) — DATA LOSS" >&2
    exit 1
fi

echo ">> u145-write-then-add-replica-syncs OK (new replica seeded from data-bearing peer, never instant-UpToDate, content intact)"
