#!/usr/bin/env bash
#
# usage: two-volume-rd.sh WORK_DIR
#
# Tests Phase 8.7 "2-volume RDs in general". An RD with multiple
# VolumeDefinitions[] must produce one DRBD device per volume, each
# independently mountable, both replicating in the same DRBD resource
# (single TCP connection, single .res file, two volume blocks).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD=e2e-twovol
N1=test-worker-1
N2=test-worker-2

trap 'delete_rd "$RD"' EXIT

echo ">> apply 2-volume RD"
# Disable auto-tiebreaker — this test only validates per-volume
# replication on the explicit 2-replica pair, the 3rd-node witness
# would just slow initial sync and add no value here.
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    DrbdOptions/AutoAddQuorumTiebreaker: "false"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}
    - {volumeNumber: 1, sizeKib: 32768}
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

# Both volumes must show up in the rendered .res file and as distinct
# DRBD devices on the satellite.
n_devs=$(on_node "$N1" bash -c "grep -cE '/dev/drbd[0-9]+' /etc/drbd.d/${RD}.res")
if (( n_devs < 2 )); then
    echo "FAIL: 2-volume RD rendered only $n_devs devices in .res"
    on_node "$N1" cat "/etc/drbd.d/${RD}.res"
    exit 1
fi

# Independent writes per volume — md5 of vol-0 must NOT match vol-1.
DEV0=$(on_node "$N1" bash -c "grep -oE '/dev/drbd[0-9]+' /etc/drbd.d/${RD}.res | sort -u | sed -n 1p")
DEV1=$(on_node "$N1" bash -c "grep -oE '/dev/drbd[0-9]+' /etc/drbd.d/${RD}.res | sort -u | sed -n 2p")

RD=$RD
on_node "$N1" drbdadm primary "$RD" 2>/dev/null || true

md5_v0=$(on_node "$N1" bash -c "
    dd if=/dev/urandom of=${DEV0} bs=1 count=4096 status=none oflag=direct
    md5sum < <(dd if=${DEV0} bs=1 count=4096 status=none iflag=direct) | awk '{print \$1}'
")
md5_v1=$(on_node "$N1" bash -c "
    dd if=/dev/urandom of=${DEV1} bs=1 count=4096 status=none oflag=direct
    md5sum < <(dd if=${DEV1} bs=1 count=4096 status=none iflag=direct) | awk '{print \$1}'
")
on_node "$N1" drbdadm secondary "$RD" || true

if [[ "$md5_v0" == "$md5_v1" ]]; then
    echo "FAIL: vol-0 and vol-1 read identical bytes — they share the same backing device"
    exit 1
fi

# Replica side: both volumes must read identical data after a peer flip.
on_node "$N2" drbdadm primary "$RD"
md5_v0_peer=$(on_node "$N2" bash -c "md5sum < <(dd if=${DEV0} bs=1 count=4096 status=none iflag=direct) | awk '{print \$1}'")
md5_v1_peer=$(on_node "$N2" bash -c "md5sum < <(dd if=${DEV1} bs=1 count=4096 status=none iflag=direct) | awk '{print \$1}'")
on_node "$N2" drbdadm secondary "$RD"

if [[ "$md5_v0" != "$md5_v0_peer" ]]; then
    echo "FAIL: vol-0 didn't replicate (n1=$md5_v0 n2=$md5_v0_peer)"
    exit 1
fi

if [[ "$md5_v1" != "$md5_v1_peer" ]]; then
    echo "FAIL: vol-1 didn't replicate (n1=$md5_v1 n2=$md5_v1_peer)"
    exit 1
fi

echo ">> TWO-VOLUME-RD OK (independent volumes, each replicated)"
