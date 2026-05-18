#!/usr/bin/env bash
#
# usage: replica-add-no-resync.sh WORK_DIR
#
# Tests Phase 8.1 "initial-sync skip on replica-add". Setup:
#   1. Create RD with 2 diskful replicas on $N1+$N2; let DRBD do the
#      one-time full initial-sync between the seeds.
#   2. Once both UpToDate, write 64 MiB of known-pattern data and
#      sync — this stages real bytes the new replica MUST claim to
#      have without a resync.
#   3. Add a 3rd diskful replica on $N3.
#
# Without GI seeding (the bug we're guarding against), DRBD-9 sees
# the new replica's metadata at zero GI, mismatches the peer's
# current_uuid on first connect, and triggers a full sync of the
# entire backing device — minutes for 64 MiB, hours on multi-TiB
# real workloads. With seeding (the fix this test pins), DRBD's GI
# handshake recognises the new peer as already-in-sync, the
# resource transitions to UpToDate within the polling window
# without any data ever flowing over the wire.
#
# Exit-criterion: $N3 reports UpToDate within 60s AND the resync
# byte counter on $N1/$N2 stays at zero throughout.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

RD=e2e-no-resync
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

trap 'delete_rd "$RD"' EXIT

echo ">> apply 2-replica RD on $N1+$N2"
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

# Stage real bytes on the volume. Without these the empty-volume
# resync optimisation in DRBD would mask the bug — we need a
# device that visibly differs from zeros.
echo ">> stage 32 MiB of pattern data on $N1"
on_node "$N1" bash -c "
    dev=\$(grep -oE '/dev/drbd[0-9]+' /etc/drbd.d/${RD}.res | head -1)
    drbdadm primary --force ${RD}
    dd if=/dev/urandom of=\$dev bs=1M count=32 conv=fdatasync 2>&1 | tail -1
    drbdadm secondary ${RD}
"

# Snapshot the replication-state BEFORE adding the new replica.
# DRBD's per-peer replication stays "Established" when no sync runs;
# any entry into "SyncSource"/"SyncTarget" means a resync started
# (which is what we're testing did NOT happen).
echo ">> baseline replication state on $N1"
n1_before=$(status_replication_state "$RD" "$N1" "$N2")
echo "  $N1->$N2 replication=$n1_before"

echo ">> add 3rd replica on $N3"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N3}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N3}
  props: {StorPoolName: stand}
EOF

echo ">> wait up to 60s for $N3 to reach UpToDate"
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    state=$(status_disk_state "$RD" "$N3")
    if [[ "$state" == "UpToDate" ]]; then
        break
    fi
    sleep 2
done

if [[ "$state" != "UpToDate" ]]; then
    echo "FAIL: $N3 did not reach UpToDate in 60s (state: $state)"
    echo "    DRBD likely fell through to full initial-sync — GI seeding broken"
    on_node "$N3" drbdsetup status "$RD" || true
    exit 1
fi

# Cross-check: assert no SyncSource transition happened on the
# existing peers. A full-sync would have shown up as SyncSource/
# SyncTarget on the per-peer replication state; read it from the
# observer-populated Status subresource instead of grepping drbdsetup.
echo ">> verify no resync triggered on existing peers"
for peer in "$N1" "$N2"; do
    for other in "$N1" "$N2" "$N3"; do
        [[ "$peer" == "$other" ]] && continue
        rs=$(status_replication_state "$RD" "$peer" "$other")
        if [[ "$rs" =~ ^(SyncSource|SyncTarget|PausedSyncS|PausedSyncT|StartingSyncS|StartingSyncT|VerifyS|VerifyT)$ ]]; then
            echo "FAIL: $peer->$other replication=$rs — initial-sync was NOT skipped"
            exit 1
        fi
    done
done

echo ">> NO-RESYNC OK ($N3 UpToDate within 60s, no resync on $N1/$N2)"
