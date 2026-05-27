#!/usr/bin/env bash
#
# usage: zfs-clone-source-delete.sh WORK_DIR
#
# Regression for the "delete a ZFS clone SOURCE while a dependent clone
# still exists" wedge.
#
# blockstor's same-node clone is `zfs clone <pool>/<src>_00000@<snap>
# <pool>/<dst>_00000`, so the dst clone's `origin` is a snapshot UNDER
# the source dataset. ZFS refuses to `zfs destroy` a snapshot that
# still has dependent clones, so deleting the source RD made
# DeleteVolume's `zfs destroy -r <src>` fail forever with
#
#   cannot destroy '<pool>/<src>_00000': volume has dependent clones
#
# → the satellite-resource reconciler hot-loops, the source Resource
# finalizer never releases, the RD never finishes deleting, and the
# node is left degraded for later scenarios.
#
# The fix `zfs promote`s the dependent clone(s) before destroying the
# source (NOT `zfs destroy -R`, which would cascade-destroy the
# surviving clone = data loss). After promotion the origin snapshot
# reparents onto the clone, making it independent, and the source
# destroys cleanly.
#
# Expected after fix:
#   1. delete SOURCE RD          → fully deletes, no "has dependent
#                                  clones" loop, finalizers release.
#   2. dst clone survives        → its volume is still readable and the
#                                  bytes match what was written to src.
#   3. delete DST RD             → also clean (promote is a no-op when
#                                  the source has no remaining clones).
#
# ZFS pool required: same-node clone uses `zfs clone`, which only the
# ZFS provider implements. We place on the worker pair that has the
# zfs-thin pool.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD_SRC=e2e-zcsd-src
RD_DST=e2e-zcsd-dst
STORPOOL=${STORPOOL:-zfs-thin}

# The zfs-thin pool lives on worker-2 + worker-3 on the dev stand
# (worker-1 carries the ZFS `data` pool, not zfs-thin). Override via
# N1/N2 if a stand lays the pool out differently.
N1=${N1:-$WORKER_2}
N2=${N2:-$WORKER_3}

if [[ -z "$N1" || -z "$N2" ]]; then
    skip "scenario needs two workers carrying the ${STORPOOL} pool"
fi

DEL_TIMEOUT=${DEL_TIMEOUT:-90}

dump_diag() {
    echo "---- dump: resourcedefinitions / resources / snapshots ----"
    kubectl get resourcedefinitions.blockstor.cozystack.io 2>/dev/null || true
    kubectl get resources.blockstor.cozystack.io 2>/dev/null || true
    kubectl get snapshots.blockstor.cozystack.io 2>/dev/null || true
    echo "---- dump: satellite logs (grep dependent clones) ----"
    for pod in $(kubectl -n "$NS" get pods -l app=blockstor-satellite -o name 2>/dev/null); do
        kubectl -n "$NS" logs "$pod" --tail=60 2>/dev/null | grep -i "dependent clones\|DeleteResource" || true
    done
    echo "---- dump: zfs list on $N1 / $N2 ----"
    on_node "$N1" bash -c "zfs list -t all -o name,origin 2>/dev/null | grep -E 'zcsd|zfs-thin' || true" || true
    on_node "$N2" bash -c "zfs list -t all -o name,origin 2>/dev/null | grep -E 'zcsd|zfs-thin' || true" || true
}

trap 'rc=$?; (( rc != 0 )) && dump_diag; delete_rd "$RD_SRC"; delete_rd "$RD_DST"' EXIT

# rd_gone returns 0 once the RD CRD and every Resource named after it
# are absent — i.e. the satellite finalizer released and the delete
# cascade completed. Times out non-zero so the wedge is caught as a
# FAIL instead of hanging the whole batch.
rd_gone() {
    local rd=$1 timeout=$2
    local deadline=$(( $(date +%s) + timeout ))
    while (( $(date +%s) < deadline )); do
        local count
        count=$( {
            kubectl get "resourcedefinitions.blockstor.cozystack.io/${rd}" \
                --no-headers 2>/dev/null
            kubectl get resources.blockstor.cozystack.io --no-headers 2>/dev/null \
                | awk -v rd="${rd}." '$1 ~ "^"rd {print $1}'
        } | grep -cv '^$' || true )
        if [[ "$count" == "0" ]]; then
            return 0
        fi
        sleep 2
    done
    return 1
}

# Self-defending pre-test wipe so leftovers from a prior scenario don't
# get blamed on this one (and don't leave a stale zcsd-* dataset that
# trips the clone).
delete_all_rds 90 || {
    echo "FAIL: stand not idle at test start"
    kubectl get resourcedefinitions.blockstor.cozystack.io 2>/dev/null || true
    exit 1
}

echo ">> apply source RD ${RD_SRC} on ${N1}+${N2} (pool=${STORPOOL})"
rd_apply "$RD_SRC" "$N1" "$N2" 65536 "$STORPOOL"
wait_uptodate "$RD_SRC" "$N1" "$N2"

# Write known payload to the source so we can prove the clone keeps it
# after the source is gone.
DEV=$(device_for_rd "$RD_SRC" "$N1")
RD=$RD_SRC
md5_src=$(write_random "$N1" "$DEV" 262144)
echo ">> source payload md5=${md5_src}"

# Same-node clone: snapshot the source then snapshot-restore into dst.
# This is exactly the plumbing clone.sh exercises; the result is a
# `zfs clone` whose origin is a snapshot under the SOURCE dataset.
SNAP=zcsd-$(date +%s)

echo ">> take transient snapshot ${SNAP} of ${RD_SRC}"
rest_post "/v1/resource-definitions/${RD_SRC}/snapshots" \
    "{\"name\":\"${SNAP}\",\"nodes\":[\"${N1}\",\"${N2}\"]}"

echo ">> clone ${RD_SRC} → ${RD_DST}"
rest_post "/v1/resource-definitions/${RD_SRC}/snapshot-restore-resource" \
    "{\"to_resource\":\"${RD_DST}\",\"snapshot_name\":\"${SNAP}\"}"

rest_post "/v1/resource-definitions/${RD_DST}/autoplace" \
    "{\"select_filter\":{\"place_count\":2,\"storage_pool\":\"${STORPOOL}\"}}"

wait_uptodate "$RD_DST" "$N1" "$N2"
echo ">> clone ${RD_DST} is UpToDate on ${N1}+${N2}"

# --- the bug: delete the SOURCE while the clone still depends on it ---
echo ">> delete SOURCE RD ${RD_SRC} (clone ${RD_DST} still depends on it)"
delete_rd "$RD_SRC"

if ! rd_gone "$RD_SRC" "$DEL_TIMEOUT"; then
    echo "FAIL: source RD ${RD_SRC} did not finish deleting within ${DEL_TIMEOUT}s"
    echo "       (likely 'zfs destroy ... has dependent clones' hot-loop)"
    exit 1
fi
echo ">> SOURCE RD fully deleted, finalizers released"

# The surviving clone must still be UpToDate and its bytes intact —
# the promote must have reparented the data, not destroyed it.
RD=$RD_DST
wait_uptodate "$RD_DST" "$N1" "$N2"
DEV_DST=$(device_for_rd "$RD_DST" "$N1")
md5_dst=$(read_md5 "$N1" "$DEV_DST" 262144)

if [[ "$md5_src" != "$md5_dst" ]]; then
    echo "FAIL: clone data lost/changed after source delete (src=${md5_src} dst=${md5_dst})"
    exit 1
fi
echo ">> clone ${RD_DST} survived with intact data (md5=${md5_dst})"

# Reverse case: deleting the clone after the source is gone must also
# be clean (promote is a no-op — the clone owns its data now).
echo ">> delete DST RD ${RD_DST}"
delete_rd "$RD_DST"

if ! rd_gone "$RD_DST" "$DEL_TIMEOUT"; then
    echo "FAIL: clone RD ${RD_DST} did not finish deleting within ${DEL_TIMEOUT}s"
    exit 1
fi

echo ">> ZFS-CLONE-SOURCE-DELETE OK"
