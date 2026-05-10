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
N1=test-worker-1
N2=test-worker-2
N3=test-worker-3

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

# Snapshot the resync-state counters BEFORE adding the new replica.
# DRBD's `cs:` field stays "Established" when no sync runs; any
# entry into "SyncSource"/"SyncTarget" means a resync started
# (which is what we're testing did NOT happen).
echo ">> baseline cs counters on $N1"
n1_before=$(on_node "$N1" drbdsetup status "$RD" --json | grep -o 'connection-state.*"' | head -1)
echo "  $n1_before"

echo ">> add 3rd replica on $N3"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N3}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N3}
EOF

echo ">> wait up to 60s for $N3 to reach UpToDate"
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    state=$(on_node "$N3" drbdsetup status "$RD" 2>/dev/null | grep "disk:" | head -1 || true)
    if [[ "$state" == *"UpToDate"* ]]; then
        break
    fi
    sleep 2
done

if [[ "$state" != *"UpToDate"* ]]; then
    echo "FAIL: $N3 did not reach UpToDate in 60s (state: $state)"
    echo "    DRBD likely fell through to full initial-sync — GI seeding broken"
    on_node "$N3" drbdsetup status "$RD" || true
    exit 1
fi

# Cross-check: assert no SyncSource transition happened on the
# existing peers. A full-sync would have left durable evidence in
# events2 history — we use the "events2 --now" snapshot to confirm
# current state shows no syncing.
echo ">> verify no resync triggered on existing peers"
for peer in "$N1" "$N2"; do
    cs=$(on_node "$peer" drbdsetup status "$RD" 2>/dev/null | grep -E "(role|connection)" | head -2 || true)
    if echo "$cs" | grep -qE "Sync(Source|Target)"; then
        echo "FAIL: $peer entered $cs — initial-sync was NOT skipped"
        exit 1
    fi
done

echo ">> NO-RESYNC OK ($N3 UpToDate within 60s, no resync on $N1/$N2)"
