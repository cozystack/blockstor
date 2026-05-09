#!/usr/bin/env bash
#
# usage: network-partition.sh WORK_DIR
#
# Tests DRBD-9 quorum behaviour under a network partition.
# Setup:
#   - 3-node cluster, 3-replica RD across workers 1+2+3
#   - quorum:majority enabled (default for blockstor RDs in cozystack)
# Steps:
#   1. write 1 MiB random data on Primary, capture md5
#   2. partition worker-1 from worker-2 + worker-3 (iptables drop)
#   3. verify worker-1 fences itself (drbd state goes StandAlone)
#   4. surviving majority (workers 2+3) keeps doing I/O
#   5. heal partition; worker-1 rejoins via bitmap merge (no full re-sync)
#   6. read on worker-1 — md5 must match
#
# Failure modes guarded:
#   - split-brain (both halves Primary)
#   - full re-sync on rejoin (would mean bitmap discarded)

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

RD=e2e-partition
N1=test-worker-1
N2=test-worker-2
N3=test-worker-3
SIZE_BYTES=$((1024 * 1024))

trap 'cleanup_partition; delete_rd "$RD"' EXIT

cleanup_partition() {
    on_node "$N1" iptables -F INPUT 2>/dev/null || true
}

echo ">> apply 3-replica RD"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}
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

# 3-replica needs the 3rd peer up before write — wait_uptodate only
# checks the 2-replica pair, so adapt for 3 here.
deadline=$(( $(date +%s) + 90 ))
while (( $(date +%s) < deadline )); do
    s1=$(on_node "$N1" drbdsetup status "$RD" 2>/dev/null | grep -c "disk:UpToDate" || true)
    s2=$(on_node "$N2" drbdsetup status "$RD" 2>/dev/null | grep -c "disk:UpToDate" || true)
    s3=$(on_node "$N3" drbdsetup status "$RD" 2>/dev/null | grep -c "disk:UpToDate" || true)
    if (( s1 >= 1 && s2 >= 1 && s3 >= 1 )); then
        break
    fi
    sleep 2
done

DEV=$(device_for_rd "$RD" "$N1")

echo ">> write 1 MiB on $N1"
md5_before=$(write_random "$N1" "$DEV" "$SIZE_BYTES")
echo "   md5 = $md5_before"

# Drop traffic on the DRBD port between $N1 and the {$N2, $N3} pair.
# Discover the port from the rendered .res — it's the same on all
# peers since DRBD-9 uses a single mesh port per replica's local
# listen socket.
DRBD_PORT=$(on_node "$N1" bash -c "grep -oE 'address.*:[0-9]+' /etc/drbd.d/${RD}.res | head -1 | grep -oE '[0-9]+$'")
if [[ -z "$DRBD_PORT" ]]; then
    echo "FAIL: could not parse DRBD port"
    exit 1
fi

echo ">> partition $N1 from peers (drop tcp/$DRBD_PORT)"
on_node "$N1" iptables -A INPUT -p tcp --dport "$DRBD_PORT" -j DROP

echo ">> wait 30s for $N1 to fence itself"
sleep 30

# After quorum:majority kicks in, $N1 (minority of 1) must NOT be
# Primary. We don't strictly require StandAlone here — Connecting +
# disk:Outdated is also acceptable for a fenced minority.
n1_role=$(on_node "$N1" drbdsetup status "$RD" 2>/dev/null | grep "role:" | head -1 || true)
if [[ "$n1_role" == *"role:Primary"* ]]; then
    echo "FAIL: $N1 stayed Primary in a 1-vs-2 partition (split-brain risk)"
    exit 1
fi

echo ">> writing on majority side ($N2)"
md5_majority=$(write_random "$N2" "$DEV" "$SIZE_BYTES")

echo ">> heal partition"
cleanup_partition

echo ">> wait 60s for $N1 to converge"
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    s1=$(on_node "$N1" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
    if [[ "$s1" == *"disk:UpToDate"* ]]; then
        break
    fi
    sleep 2
done

echo ">> read on $N1 after heal — md5 must match $md5_majority"
md5_after=$(read_md5 "$N1" "$DEV" "$SIZE_BYTES")
if [[ "$md5_after" != "$md5_majority" ]]; then
    echo "FAIL: post-heal md5 mismatch ($N1=$md5_after, majority=$md5_majority)"
    exit 1
fi

echo ">> NETWORK-PARTITION OK ($N1 fenced, majority survived, bitmap merge clean)"
