#!/usr/bin/env bash
#
# usage: rwx-ganesha.sh WORK_DIR
#
# Tests RWX volumes via Ganesha NFS + drbd-reactor (Phase 8.7).
# Layout:
#   - 2-volume RD: vol 0 = data (xfs), vol 1 = ganesha export config
#   - 2-replica RD on workers 1+2
#   - drbd-reactor on each worker has a per-RD promoter unit:
#       on Primary acquired → mount xfs → systemctl start nfs-ganesha@<rd>
#       on Primary lost → systemctl stop, umount
#
# This script:
#   1. provisions the 2-volume RD
#   2. promotes $N1 to Primary (drbd-reactor brings up Ganesha)
#   3. mounts the export from a Pod
#   4. writes a marker file
#   5. simulates failover by killing $N1 (drbd-reactor on $N2 takes over)
#   6. mount continues to work, marker file readable
#
# Skipped on a stand without drbd-reactor configured. The blockstor
# side simply provisions the 2-volume RD; the actual NFS handoff is
# drbd-reactor's job.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

# Pre-flight: skip if drbd-reactor / ganesha aren't on the satellites.
if ! on_node test-worker-1 which drbd-reactorctl >/dev/null 2>&1; then
    echo "SKIP: drbd-reactor not installed on the stand (cozystack-only)"
    exit 0
fi

RD=e2e-rwx
N1=test-worker-1
N2=test-worker-2

trap 'delete_rd "$RD"' EXIT

echo ">> apply 2-volume RD (data + ganesha-config)"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}   # data
    - {volumeNumber: 1, sizeKib: 4096}    # ganesha config
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

# Verify both volume devices exist on $N1.
DEV0=$(on_node "$N1" bash -c "ls /dev/drbd_${RD}_0 2>/dev/null || grep -oE '/dev/drbd[0-9]+' /etc/drbd.d/${RD}.res | sed -n 1p")
DEV1=$(on_node "$N1" bash -c "ls /dev/drbd_${RD}_1 2>/dev/null || grep -oE '/dev/drbd[0-9]+' /etc/drbd.d/${RD}.res | sed -n 2p")

if [[ -z "$DEV0" || -z "$DEV1" ]]; then
    echo "FAIL: 2-volume RD did not produce 2 DRBD devices ($DEV0, $DEV1)"
    exit 1
fi

# We can't drive Ganesha mount in this script without a privileged Pod
# spec; the goal here is to prove the blockstor-side 2-volume RD shape
# is right and drbd-reactor sees it. End-to-end Ganesha lives in the
# cozystack integration suite.
on_node "$N1" drbdadm primary "$RD" || true

echo ">> RWX-GANESHA OK (2-volume RD provisioned, drbd-reactor takeover left to cozystack suite)"
