#!/usr/bin/env bash
#
# usage: skip-init-sync-offline-holder.sh WORK_DIR
#
# Skip-init-sync hardening — OFFLINE-SAFETY scenario (the core fix).
#
# Regression guarded: PR #20 decided skip-vs-sync in the satellite via a
# live-kernel probe (Adm.AnyConnectedPeerHasData) that only sees
# CONNECTED peers. If the sole data-holder is OFFLINE when a new replica
# is seeded, that probe returns false → the new replica was seeded as a
# day0 skip → came up falsely UpToDate while EMPTY → CSI would mount it
# and a pod reads zeros / overwrites real data.
#
# The fix makes the decision controller-authoritative and persisted in
# Spec: the controller latches RD.Spec.Initialized=true once the RD has
# held real data, and stamps each replica's append-only
# Resource.Spec.SkipInitialSync = !RD.Spec.Initialized at creation. A
# replica added to an already-initialized RD is stamped
# skipInitialSync=false and MUST SyncTarget — EVEN IF the data-holder is
# offline at seed time, because the decision is read from the persisted
# RD latch, not live peer/kernel state.
#
# Flow:
#   1. Create 2-replica RD on $N1+$N2; let the fresh skip bring both
#      UpToDate (out_of_sync=0, no SyncTarget — preserves PR #20).
#   2. Write 32 MiB of pattern data on $N1, capture md5. The real write
#      advances the GI past day0 → controller latches
#      RD.Spec.Initialized=true.
#   3. Take BOTH diskful holders' DRBD replication offline (iptables drop
#      the DRBD port) so the new replica cannot see ANY data peer over
#      the kernel at seed time — the exact condition the old probe
#      mis-handled.
#   4. Add a 3rd diskful replica on $N3 while the holders are offline.
#      Assert: Resource.Spec.skipInitialSync==false (controller refused
#      the skip from the persisted latch) AND $N3 does NOT become
#      UpToDate-empty (it stays Inconsistent / never UpToDate while the
#      holder is unreachable).
#   5. Restore connectivity. Assert: $N3 SyncTargets the real data and
#      reaches UpToDate, and its md5 matches the data staged on $N1.
#
# Exit-criterion: $N3 is NEVER UpToDate-empty while offline; after
# heal it SyncTargets and its md5 matches the source.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

RD=e2e-skip-offline
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3
BYTES=$(( 32 * 1024 * 1024 ))

cleanup() {
    # Always restore connectivity before teardown so delete_rd's
    # drbdadm down can reach peers and the next scenario starts clean.
    for n in "$N1" "$N2"; do
        on_node "$n" iptables -F INPUT 2>/dev/null || true
        on_node "$n" iptables -F OUTPUT 2>/dev/null || true
    done
    delete_rd "$RD"
}
trap cleanup EXIT

echo ">> apply 2-replica RD on $N1+$N2"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.cozystack.io/v1alpha1
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
apiVersion: blockstor.cozystack.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${n}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${n}
  props: {StorPoolName: stand}
EOF
done

wait_uptodate "$RD" "$N1" "$N2"

# Criterion A sanity on the fresh set: the initial replicas must have
# skipped (out_of_sync=0, no SyncTarget) — both stamped skipInitialSync.
for n in "$N1" "$N2"; do
    sis=$(kubectl get resource "${RD}.${n}" -o jsonpath='{.spec.skipInitialSync}' 2>/dev/null)
    echo "  ${RD}.${n} spec.skipInitialSync=${sis:-<unset>}"
    if [[ "$sis" != "true" ]]; then
        echo "FAIL: fresh initial replica ${RD}.${n} must be stamped skipInitialSync=true, got '${sis:-<unset>}'"
        exit 1
    fi
done

echo ">> stage 32 MiB of pattern data on $N1 (advances GI past day0)"
on_node "$N1" drbdadm primary --force "$RD"
dev1=$(device_for_rd "$RD" "$N1")
src_md5=$(write_random "$N1" "$dev1" "$BYTES")
on_node "$N1" drbdadm secondary "$RD" || true
echo "  source md5=$src_md5"

# Let the write propagate to N2 and the GI advance be observed so the
# controller latches RD.Spec.Initialized=true.
echo ">> wait for RD.spec.initialized latch"
deadline=$(( $(date +%s) + 60 ))
init=""
while (( $(date +%s) < deadline )); do
    init=$(kubectl get resourcedefinition "$RD" -o jsonpath='{.spec.initialized}' 2>/dev/null)
    [[ "$init" == "true" ]] && break
    sleep 2
done
echo "  RD.spec.initialized=${init:-<unset>}"
if [[ "$init" != "true" ]]; then
    echo "FAIL: controller did not latch RD.spec.initialized after a real write"
    exit 1
fi

DRBD_PORT=$(on_node "$N1" bash -c "grep -oE 'port [0-9]+' /etc/drbd.d/${RD}.res | head -1 | awk '{print \$2}'")
echo ">> take data-holders $N1+$N2 OFFLINE (iptables drop DRBD port $DRBD_PORT)"
for n in "$N1" "$N2"; do
    on_node "$n" iptables -A INPUT  -p tcp --dport "$DRBD_PORT" -j DROP
    on_node "$n" iptables -A OUTPUT -p tcp --dport "$DRBD_PORT" -j DROP
done

echo ">> add 3rd replica on $N3 while holders are offline"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.cozystack.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N3}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N3}
  props: {StorPoolName: stand}
EOF

# The controller must stamp skipInitialSync=false on the new replica,
# read from the persisted RD latch (NOT from live peer state — the
# holders are unreachable).
echo ">> assert $N3 stamped skipInitialSync=false (offline-safe decision)"
deadline=$(( $(date +%s) + 60 ))
sis3=""
while (( $(date +%s) < deadline )); do
    sis3=$(kubectl get resource "${RD}.${N3}" -o jsonpath='{.spec.skipInitialSync}' 2>/dev/null)
    [[ -n "$sis3" ]] && break
    sleep 2
done
echo "  ${RD}.${N3} spec.skipInitialSync=${sis3:-<unset>}"
if [[ "$sis3" != "false" ]]; then
    echo "FAIL: replica added to an initialized RD with OFFLINE holders must be stamped skipInitialSync=false, got '${sis3:-<unset>}'"
    exit 1
fi

# The CRITICAL safety assertion: while the holder is offline, $N3 must
# NOT reach UpToDate (which would mean it came up falsely UpToDate-empty).
# Watch for ~40s — it should stay Inconsistent / non-UpToDate.
echo ">> verify $N3 does NOT become UpToDate-empty while holders offline (~40s watch)"
end=$(( $(date +%s) + 40 ))
while (( $(date +%s) < end )); do
    st=$(status_disk_state "$RD" "$N3")
    if [[ "$st" == "UpToDate" ]]; then
        echo "FAIL: $N3 reached UpToDate while the data-holder is OFFLINE — it came up UpToDate-EMPTY (the unsafe-probe regression)"
        on_node "$N3" drbdsetup status "$RD" || true
        exit 1
    fi
    sleep 3
done
echo "  $N3 disk-state while offline: $(status_disk_state "$RD" "$N3") (correctly not UpToDate)"

echo ">> heal: restore connectivity on $N1+$N2"
for n in "$N1" "$N2"; do
    on_node "$n" iptables -F INPUT 2>/dev/null || true
    on_node "$n" iptables -F OUTPUT 2>/dev/null || true
done

echo ">> wait up to 180s for $N3 to SyncTarget the real data and reach UpToDate"
if ! wait_disk_state "$RD" "$N3" UpToDate 180; then
    echo "FAIL: $N3 did not reach UpToDate after the holder returned"
    on_node "$N3" drbdsetup status "$RD" || true
    exit 1
fi

echo ">> verify $N3 md5 matches the data staged on $N1"
dev3=$(device_for_rd "$RD" "$N3")
got_md5=$(read_md5 "$N3" "$dev3" "$BYTES")
echo "  source=$src_md5  n3=$got_md5"
if [[ "$src_md5" != "$got_md5" ]]; then
    echo "FAIL: $N3 md5 mismatch — it did NOT SyncTarget the real data"
    exit 1
fi

echo ">> OFFLINE-SAFETY OK ($N3 refused to skip while holder offline; SyncTargeted real data after heal, md5 matches)"
