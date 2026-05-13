#!/usr/bin/env bash
#
# usage: snap-ship-cross-node.sh WORK_DIR
#
# Scenario 4.16: cross-node snapshot ship on ZFS_THIN. Exercises
# blockstor's `Reconciler.ShipSnapshot` end-to-end — the source
# satellite must run `zfs send <pool>/<rd>_00000@<snap>` and stream
# the recordset over ssh into the target satellite's `zfs recv`,
# producing a byte-perfect replica of the snapshot on a node that
# previously had NO diskful replica of the source RD.
#
# Setup:
#   - 2-replica RD `ship-src` on worker-1 + worker-2 using the
#     zfs-thin pool (ProviderKind=ZFS_THIN). 64 MiB volume.
#   - mount as Primary on worker-1, write 4 MiB of urandom marker
#     bytes, fsync, capture md5, unmount.
#   - snapshot via REST.
# Shipped restore:
#   - POST /v1/resource-definitions/ship-src/snapshot-restore-resource
#     with to_resource=ship-restored.
#   - autoplace ship-restored on worker-3 (place_count=1). The
#     autoplacer's resolveCloneSourceProviderKind constraint (Bug 15
#     fix) must pin ProviderKind to ZFS_THIN so the placer only
#     considers zfs-thin pools on candidate nodes — never falling
#     back to a dd/LVM payload on lvm-thin / stand.
#   - satellite-side ShipSnapshot picks worker-1 or worker-2 as the
#     send source (it has the snapshot locally), pipes `zfs send`
#     into ssh to worker-3 which runs `zfs recv`.
# Expected:
#   - worker-3 has a fresh diskful replica of ship-restored within
#     ~60s.
#   - reading the first 4 MiB of the restored device yields the
#     SAME md5 as the marker written on worker-1.
#   - worker-3 has a ZFS dataset `<zpool>/ship-restored_00000`
#     created by the ship-side `zfs recv`.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

RD_SRC=ship-src
RD_DST=ship-restored
SNAP=snap1
SIZE_KIB=65536           # 64 MiB
MARKER_BYTES=$((4 * 1024 * 1024))  # 4 MiB of urandom
STORPOOL=${STORPOOL:-zfs-thin}
ZPOOL=${ZPOOL:-blockstor-zfs}
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

trap 'delete_rd "$RD_DST"; delete_rd "$RD_SRC"' EXIT

# rd_apply_pool is a localised copy of lib.sh:rd_apply that lets the
# caller pin StorPoolName — lib.sh hardcodes `stand`. We need ZFS_THIN
# pools (`zfs-thin` per stand/install-pools.sh) so the source has a
# ZFS dataset to `zfs send` from. Same shape, narrower scope.
rd_apply_pool() {
    local rd=$1 primary=$2 peer=$3 pool=$4 size=${5:-65536}
    cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${rd}}
spec:
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: ${size}}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${rd}.${primary}}
spec:
  resourceDefinitionName: ${rd}
  nodeName: ${primary}
  props: {StorPoolName: ${pool}}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${rd}.${peer}}
spec:
  resourceDefinitionName: ${rd}
  nodeName: ${peer}
  props: {StorPoolName: ${pool}}
EOF
}

echo ">> apply source RD ${RD_SRC} on ${N1} + ${N2} (pool=${STORPOOL})"
rd_apply_pool "$RD_SRC" "$N1" "$N2" "$STORPOOL" "$SIZE_KIB"

RD=$RD_SRC
wait_uptodate "$RD_SRC" "$N1" "$N2"

DEV_SRC=$(device_for_rd "$RD_SRC" "$N1")
if [[ -z "$DEV_SRC" ]]; then
    echo "FAIL: could not resolve DRBD device for ${RD_SRC} on ${N1}"
    exit 1
fi

echo ">> write ${MARKER_BYTES}B urandom marker on ${N1}:${DEV_SRC}"
md5_src=$(write_random "$N1" "$DEV_SRC" "$MARKER_BYTES")
echo "   marker md5=${md5_src}"

# Demote primary cleanly so the snapshot reflects committed data.
on_node "$N1" bash -c "sync; drbdadm secondary ${RD_SRC} 2>/dev/null || true"

echo ">> verify source has a ZFS dataset on both peers"
for n in "$N1" "$N2"; do
    if ! on_node "$n" zfs list -H -o name "${ZPOOL}/${RD_SRC}_00000" >/dev/null 2>&1; then
        echo "FAIL: zfs dataset ${ZPOOL}/${RD_SRC}_00000 missing on ${n}"
        on_node "$n" zfs list 2>/dev/null || true
        exit 1
    fi
done

echo ">> snapshot ${RD_SRC}@${SNAP} via REST"
rest_post "/v1/resource-definitions/${RD_SRC}/snapshots" \
    "{\"name\":\"${SNAP}\",\"nodes\":[\"${N1}\",\"${N2}\"]}" >/dev/null

# Wait for the satellite-side ZFS snapshot to materialise. The REST
# call only stamps a Snapshot CRD; the satellite reconciler is what
# actually runs `zfs snapshot`.
echo ">> wait up to 60s for zfs snapshot on both peers"
snap_deadline=$(( $(date +%s) + 60 ))
have_snap=0
while (( $(date +%s) < snap_deadline )); do
    if on_node "$N1" zfs list -H -t snapshot -o name "${ZPOOL}/${RD_SRC}_00000@${SNAP}" >/dev/null 2>&1 \
        && on_node "$N2" zfs list -H -t snapshot -o name "${ZPOOL}/${RD_SRC}_00000@${SNAP}" >/dev/null 2>&1; then
        have_snap=1
        break
    fi
    sleep 2
done

if (( have_snap == 0 )); then
    echo "FAIL: zfs snapshot ${ZPOOL}/${RD_SRC}_00000@${SNAP} did not appear on both peers"
    on_node "$N1" zfs list -t snapshot 2>/dev/null || true
    on_node "$N2" zfs list -t snapshot 2>/dev/null || true
    exit 1
fi

echo ">> snapshot-restore into ${RD_DST}"
rest_post "/v1/resource-definitions/${RD_SRC}/snapshot-restore-resource" \
    "{\"to_resource\":\"${RD_DST}\",\"snapshot_name\":\"${SNAP}\"}" >/dev/null

# Pin placement to worker-3 only — it has NO local replica of
# ${RD_SRC} and NO local snapshot. The only way to populate it is
# for the satellite-side ShipSnapshot to stream from ${N1} or ${N2}
# over ssh + zfs send|recv. We deliberately omit ${N1}/${N2} from
# the candidate list so the placer can't take a shortcut.
echo ">> autoplace ${RD_DST} on ${N3} only (ship-only path)"
ship_t0=$(date +%s)
rest_post "/v1/resource-definitions/${RD_DST}/autoplace" \
    "{\"select_filter\":{\"place_count\":1,\"storage_pool\":\"${STORPOOL}\",\"node_name_list\":[\"${N3}\"]}}" >/dev/null

# Single-replica RD on N3 — there's no DRBD peer to reach UpToDate
# against, so wait_uptodate doesn't apply. Poll the Resource status
# until the satellite reports devicePath (provisioned + ship done).
echo ">> wait up to 180s for ${RD_DST} to provision on ${N3} via ShipSnapshot"
dst_deadline=$(( $(date +%s) + 180 ))
DEV_DST=""
while (( $(date +%s) < dst_deadline )); do
    DEV_DST=$(kubectl get resource "${RD_DST}.${N3}" \
        -o jsonpath='{.status.volumes[?(@.volumeNumber==0)].devicePath}' \
        2>/dev/null || true)
    if [[ -n "$DEV_DST" ]]; then
        break
    fi
    sleep 2
done
ship_t1=$(date +%s)
ship_elapsed=$(( ship_t1 - ship_t0 ))

if [[ -z "$DEV_DST" ]]; then
    echo "FAIL: ${RD_DST}.${N3} never reported devicePath after 180s"
    kubectl get resource "${RD_DST}.${N3}" -o yaml 2>/dev/null || true
    kubectl logs -n "$NS" deploy/blockstor-controller --tail=80 2>/dev/null || true
    exit 1
fi
echo "   provisioned: ${RD_DST}.${N3} devicePath=${DEV_DST} elapsed=${ship_elapsed}s"

# ShipSnapshot must produce a fresh ZFS dataset on N3 — that's
# literally what `zfs recv` does. If it's missing, the satellite
# took a dd / blank-create fallback path instead and the test is a
# false positive.
echo ">> verify zfs dataset ${ZPOOL}/${RD_DST}_00000 exists on ${N3}"
if ! on_node "$N3" zfs list -H -o name "${ZPOOL}/${RD_DST}_00000" >/dev/null 2>&1; then
    echo "FAIL: zfs dataset ${ZPOOL}/${RD_DST}_00000 missing on ${N3} — ship did not run"
    on_node "$N3" zfs list 2>/dev/null || true
    exit 1
fi

# Single-replica restored RD: device is whatever the satellite
# exposed (raw zvol when LayerStack drops DRBD, or /dev/drbdN when
# DRBD is kept for the clone). Either way, read the first MARKER_BYTES
# and compare md5 with the source.
echo ">> read marker bytes back from ${N3}:${DEV_DST}"
RD=$RD_DST
md5_dst=$(read_md5 "$N3" "$DEV_DST" "$MARKER_BYTES")
echo "   restored md5=${md5_dst}"

if [[ "$md5_src" != "$md5_dst" ]]; then
    echo "FAIL: shipped data differs (src=${md5_src} dst=${md5_dst})"
    exit 1
fi

echo ">> SNAP-SHIP-CROSS-NODE OK (${RD_DST} on ${N3} == ${RD_SRC}@${SNAP}; ship took ${ship_elapsed}s)"
