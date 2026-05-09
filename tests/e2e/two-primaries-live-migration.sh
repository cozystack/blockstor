#!/usr/bin/env bash
#
# usage: two-primaries-live-migration.sh WORK_DIR
#
# Tests Phase 8.2 "allow-two-primaries" plumbing for live migration.
# Setup:
#   - 2-replica RD with DrbdOptions/Net/allow-two-primaries=yes
#   - $N1 Primary, write data
#   - manually promote $N2 to Primary while $N1 remains Primary
#   - drop $N1 to Secondary (the live-migration handoff)
# Expected:
#   - both can be Primary simultaneously without DRBD complaint
#   - data on $N2 reads back the same as written on $N1
#   - after handoff, $N1 cleanly drops to Secondary
#
# This pins the .res rendering path: allow-two-primaries must land
# verbatim in the net{} block of /etc/drbd.d/<rd>.res.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

RD=e2e-two-primaries
N1=test-worker-1
N2=test-worker-2

trap 'delete_rd "$RD"' EXIT

echo ">> apply RD with allow-two-primaries"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    DrbdOptions/Net/allow-two-primaries: "yes"
    # 2-replica RD is what live-migration needs; the witness on N3
    # would slow initial sync without adding test signal.
    DrbdOptions/AutoAddQuorumTiebreaker: "false"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}
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

# Sanity: rendered .res must contain "allow-two-primaries yes;"
if ! on_node "$N1" grep -q "allow-two-primaries yes" "/etc/drbd.d/${RD}.res"; then
    echo "FAIL: allow-two-primaries did not land in .res file"
    on_node "$N1" cat "/etc/drbd.d/${RD}.res" | head -40
    exit 1
fi

DEV=$(device_for_rd "$RD" "$N1")
RD=$RD
md5_n1=$(write_random "$N1" "$DEV" 262144)

echo ">> promote $N2 (live-migration step: dual-Primary)"
on_node "$N2" drbdadm primary "$RD"

# Quick sanity: both must report Primary.
n1_role=$(on_node "$N1" drbdsetup status "$RD" | grep "role:" | head -1)
n2_role=$(on_node "$N2" drbdsetup status "$RD" | grep "role:" | head -1)
if [[ "$n1_role" != *"role:Primary"* || "$n2_role" != *"role:Primary"* ]]; then
    echo "FAIL: dual-Primary did not establish (n1=$n1_role n2=$n2_role)"
    exit 1
fi

md5_n2=$(read_md5 "$N2" "$DEV" 262144)
if [[ "$md5_n1" != "$md5_n2" ]]; then
    echo "FAIL: data divergence across dual-Primary peers"
    exit 1
fi

echo ">> handoff: drop $N1 to Secondary"
on_node "$N1" drbdadm secondary "$RD"

n1_role_after=$(on_node "$N1" drbdsetup status "$RD" | grep "role:" | head -1)
if [[ "$n1_role_after" == *"role:Primary"* ]]; then
    echo "FAIL: $N1 did not drop to Secondary after handoff"
    exit 1
fi

echo ">> TWO-PRIMARIES-LIVE-MIGRATION OK"
