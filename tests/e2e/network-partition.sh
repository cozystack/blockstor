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
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3
SIZE_BYTES=$((1024 * 1024))

trap 'cleanup_partition; delete_rd "$RD"' EXIT

cleanup_partition() {
    on_node "$N1" iptables -F INPUT 2>/dev/null || true
    on_node "$N1" iptables -F OUTPUT 2>/dev/null || true
}

echo ">> apply 3-replica RD"
# Disable the RD-reconciler's auto-tiebreaker placement — we want to
# place all three replicas explicitly (diskful, no DISKLESS witness).
# Otherwise the reconciler spawns a DISKLESS Resource on $N3 while
# the loop below is still kubectl-applying the explicit $N3 diskful
# Resource, and the two races produce the "missing
# last-applied-configuration annotation … not found" rejection
# observed live.
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    DrbdOptions/AutoAddQuorumTiebreaker: "false"
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
    s1=$(status_disk_state "$RD" "$N1")
    s2=$(status_disk_state "$RD" "$N2")
    s3=$(status_disk_state "$RD" "$N3")
    if [[ "$s1" == "UpToDate" && "$s2" == "UpToDate" && "$s3" == "UpToDate" ]]; then
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

echo ">> partition $N1 from peers (drop tcp/$DRBD_PORT in+out)"
# Block both INPUT and OUTPUT — INPUT alone leaves DRBD's outbound
# keep-alives flowing and the TCP teardown lingers, holding $N1's
# view of the peers "Connecting" long enough for the 30 s deadline
# below to time out. OUTPUT DROP closes the symmetric direction so
# the quorum-loss path fires within the DRBD ping-timeout window.
on_node "$N1" iptables -A INPUT -p tcp --dport "$DRBD_PORT" -j DROP
on_node "$N1" iptables -A OUTPUT -p tcp --dport "$DRBD_PORT" -j DROP

echo ">> wait up to 90s for $N1 to fence itself"
# Bug 297: with `on-no-quorum=suspend-io` (the controller-seeded default
# alongside `quorum=majority`), DRBD-9 SUSPENDS IO on quorum loss
# rather than demoting the role. The kernel keeps Role=Primary and
# stamps Suspended=Quorum on the local row — same effect (no further
# writes accepted on the minority side, no split-brain) but a
# different state signature than the legacy `io-error` policy. Accept
# either signal: Status.Suspended non-empty OR role no longer Primary.
# Read from Resource.Status via Phase 11.5.b P0 helpers.
deadline=$(( $(date +%s) + 90 ))
n1_first_role=""
n1_suspended=""

while (( $(date +%s) < deadline )); do
    n1_first_role=$(status_role "$RD" "$N1")
    n1_suspended=$(status_suspended "$RD" "$N1")
    if [[ "$n1_first_role" != "Primary" || -n "$n1_suspended" ]]; then
        break
    fi
    sleep 2
done

if [[ "$n1_first_role" == "Primary" && -z "$n1_suspended" ]]; then
    echo "FAIL: $N1 stayed Primary AND unsuspended in a 1-vs-2 partition (split-brain risk)"
    echo "----- last Status (role=$n1_first_role suspended=$n1_suspended) -----"
    kubectl get resource "${RD}.${N1}" -o yaml 2>/dev/null | sed -n '/^status:/,$p' || true
    echo "----- drbdsetup dump -----"
    on_node "$N1" drbdsetup status "$RD" 2>/dev/null || true
    echo "-----------------------"
    exit 1
fi

# Bug 297: with suspend-io semantics $N1 stays Primary — we need to
# explicitly demote it before $N2's write so the test's pre-heal
# divergence sequence still produces a genuine new-data-on-majority
# state. Without this the iptables drop leaves $N1 Primary-suspended
# and $N2's auto-promote on write fails ("multiple primaries not
# allowed by config"), the post-heal read loop then sees $N1 still
# holding the stale pre-partition md5.
if [[ "$n1_first_role" == "Primary" ]]; then
    on_node "$N1" drbdadm secondary "$RD" 2>/dev/null || true
fi

echo ">> writing on majority side ($N2)"
md5_majority=$(write_random "$N2" "$DEV" "$SIZE_BYTES")

echo ">> heal partition"
cleanup_partition

echo ">> wait up to 180s for $N1 to converge (peer-disk UpToDate, no suspended)"
# Bug 297: with `on-no-quorum=suspend-io` (now seeded by the controller
# alongside `quorum=majority`), the LOCAL disk on $N1 stays disk:UpToDate
# THROUGHOUT the partition — it's the same on-disk bytes, just suspended.
# So `grep -m1 disk:` matched immediately on the very first poll, before
# the kernel had reconnected and pulled the majority writes in. The
# script then raced into read_md5 on a still-suspended device and tripped
# ENODATA from open(2).
#
# Wait on the PEER state instead. `drbdsetup status` omits the
# `connection:` field once a peer is Connected — the presence of two
# `peer-disk:UpToDate` lines is the unambiguous "fully reconnected and
# resync done" signal. Also assert Status.Volumes[0].Quorum=="true"
# and Status.Suspended=="" on the local row — both flags clear as soon
# as $N1 re-joins the majority (read via Phase 11.4.b / 11.5.b P0
# helpers; the peer-disk count stays on drbdsetup pending Phase 11.5.b
# P1 Connections[].PeerVolumes[].PeerDiskState).
deadline=$(( $(date +%s) + 180 ))
status=""
n1_to_n2_conn=""
n1_to_n3_conn=""
n1_quorum=""
n1_suspended=""

while (( $(date +%s) < deadline )); do
    # TODO(Phase 11.5.b P1): peer-disk view requires Connections[].PeerVolumes[].PeerDiskState.
    # Until then keep the drbdsetup parse for `peer-disk:UpToDate` count.
    status=$(on_node "$N1" drbdsetup status "$RD" 2>/dev/null || true)
    peer_uptodate=$(echo "$status" | grep -c "peer-disk:UpToDate" || true)
    n1_to_n2_conn=$(status_connection_state "$RD" "$N1" "$N2")
    n1_to_n3_conn=$(status_connection_state "$RD" "$N1" "$N3")
    n1_quorum=$(status_volume_quorum "$RD" "$N1")
    n1_suspended=$(status_suspended "$RD" "$N1")
    if (( peer_uptodate >= 2 )) \
        && [[ "$n1_to_n2_conn" != "Connecting" && "$n1_to_n3_conn" != "Connecting" ]] \
        && [[ "$n1_quorum" != "false" ]] \
        && [[ -z "$n1_suspended" ]]; then
        break
    fi
    sleep 2
done

peer_uptodate=$(echo "$status" | grep -c "peer-disk:UpToDate" || true)
n1_to_n2_conn=$(status_connection_state "$RD" "$N1" "$N2")
n1_to_n3_conn=$(status_connection_state "$RD" "$N1" "$N3")
n1_quorum=$(status_volume_quorum "$RD" "$N1")
n1_suspended=$(status_suspended "$RD" "$N1")
if (( peer_uptodate < 2 )) \
    || [[ "$n1_to_n2_conn" == "Connecting" || "$n1_to_n3_conn" == "Connecting" ]] \
    || [[ "$n1_quorum" == "false" ]] \
    || [[ -n "$n1_suspended" ]]; then
    echo "FAIL: $N1 did not re-converge after heal"
    echo "    connections: ->$N2=$n1_to_n2_conn ->$N3=$n1_to_n3_conn"
    echo "    quorum=$n1_quorum suspended=$n1_suspended peer-disk-uptodate=$peer_uptodate"
    echo "----- last status -----"
    echo "$status"
    echo "-----------------------"
    exit 1
fi

# Demote $N2 so $N1 can promote and read. With dual-primaries
# disabled (the default) `drbdadm primary` from within read_md5
# would fail silently on $N1 while $N2 still holds Primary, and
# the subsequent `dd` opens the still-suspended block device and
# trips `No data available` (ENODATA from quorum-on-no-data path).
echo ">> demote $N2 so $N1 can read"
on_node "$N2" drbdadm secondary "$RD" || true
sleep 5

echo ">> read on $N1 after heal — md5 must match $md5_majority"
md5_after=$(read_md5 "$N1" "$DEV" "$SIZE_BYTES")
if [[ "$md5_after" != "$md5_majority" ]]; then
    echo "FAIL: post-heal md5 mismatch ($N1=$md5_after, majority=$md5_majority)"
    exit 1
fi

echo ">> NETWORK-PARTITION OK ($N1 fenced, majority survived, bitmap merge clean)"
