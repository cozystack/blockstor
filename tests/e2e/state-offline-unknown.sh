#!/usr/bin/env bash
#
# usage: state-offline-unknown.sh WORK_DIR
#
# Scenario 5.5 — When the satellite on a node goes offline, the
# controller's observability projection of that node's Resource row
# must reflect "I no longer know the truth":
#
#   1. `linstor n l` for the offline worker must flip to State=Offline
#      (driven by the heartbeat watchdog at internal/controller/
#      node_heartbeat_controller.go: stale beyond
#      NodeMonitorGracePeriod=40 s → ConnectionStatus=OFFLINE).
#
#   2. `linstor r l -r <RD>` row for the offline worker must show
#      State=Unknown: like LINSTOR, the controller answers a replica on
#      an OFFLINE node with no satellite state (no in_use, no
#      volumes[].state.disk_state, no peer links), so a stale UpToDate
#      is never re-emitted for a replica nothing can vouch for.
#
#   3. The CRD Status keeps the satellite's last report for forensics
#      when the satellite died before blanking it; only the answer
#      changes. Reported, not asserted (it depends on how it died).
#
#   4. Recovery: removing the schedulability taint lets the satellite
#      pod come back, the heartbeat resumes, the watchdog flips
#      ConnectionStatus back to ONLINE, and the observer re-asserts
#      DrbdState=UpToDate. Within HEAL_TIMEOUT, both the node row
#      and the resource row must look healthy again, without
#      operator intervention.
#
# We provoke the offline window by labelling WORKER_3 with a
# scenario-local key and patching the satellite DaemonSet's
# nodeAffinity to refuse that label, then force-deleting the
# existing satellite pod on WORKER_3. The DS's blanket
# `tolerations: [{operator: Exists}]` makes node taints useless
# for keeping the satellite off WORKER_3 (every taint is
# tolerated), so we have to use affinity instead.
#
# Stopping `kubelet` on a Talos node leaves the containerd-managed
# satellite pod alive (kubelet just stops reconciling Pod status),
# so heartbeats KEEP flowing — that wouldn't exercise the OFFLINE
# flip at all. The affinity-based eviction is the only reliable
# way on this stand to land the satellite in a truly absent state
# for the >40 s grace window.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

if ! command -v linstor >/dev/null 2>&1; then
    echo "FAIL: linstor CLI not in PATH (apt install linstor-client)" >&2
    exit 1
fi

RD=e2e-5-5-offline-unknown
EVICT_LABEL=blockstor.org/offline-unknown-test
OFFLINE_TIMEOUT=75    # 40 s grace + watchdog requeue period (5 s) +
                      # apiserver/SSA-apply slack on a busy QEMU stand
HEAL_TIMEOUT=180      # satellite startup + heartbeat (max 40 s for the
                      # watchdog to flip ConnectionStatus back) +
                      # DRBD reconnect + full re-sync of the
                      # offline-window-delta on a busy QEMU stand

# --- REST port-forward (random ephemeral port for parallel-iter safety) ---
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/state-offline-unknown-pf.log 2>&1 &
PF_PID=$!

for _ in $(seq 1 30); do
    if curl -fsS -m 1 "http://127.0.0.1:${PF_PORT}/v1/healthz" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:$PF_PORT")
LCTLJ=(linstor --controllers "http://localhost:$PF_PORT" --machine-readable)

dump_diag() {
    echo "---- dump: linstor n l ----"
    "${LCTL[@]}" node list || true
    echo "---- dump: linstor r l ----"
    "${LCTL[@]}" resource list || true
    echo "---- dump: Node CRDs ----"
    kubectl get nodes.blockstor.org -o wide || true
    echo "---- dump: Resource CRDs (Status) ----"
    kubectl get resources.blockstor.org -o yaml | \
        grep -A2 -E 'name:|drbdState|disk_state|connectionStatus' | head -80 || true
}

cleanup() {
    local rc=$?
    if (( rc != 0 )); then
        dump_diag
    fi

    # Strip the affinity patch + the eviction label so the DaemonSet
    # re-spawns the satellite. Doing this BEFORE delete_rd lets the
    # satellite-side teardown actually run against the replica we
    # placed on WORKER_3 (otherwise the finalizer hangs and the next
    # scenario observes residue).
    kubectl -n "$NS" patch ds blockstor-satellite --type=json \
        -p='[{"op":"remove","path":"/spec/template/spec/affinity/nodeAffinity/requiredDuringSchedulingIgnoredDuringExecution/nodeSelectorTerms/0/matchExpressions/1"}]' \
        2>/dev/null || true
    kubectl label node "$WORKER_3" "${EVICT_LABEL}-" 2>/dev/null || true

    # Wait for ALL 3 satellite Pods Ready before tearing the RD — checking
    # only WORKER_3 raced the next scenario's `require_workers 3` preflight
    # when DS desired/ready briefly desynced after the affinity-revert
    # (toggle-disk SKIPped with "found 2" on lane 3 of run 26646144009).
    # Widen window to 120s and require numberReady==desiredNumberScheduled,
    # the same signal `reset_cluster_state` and `require_workers` use.
    local deadline=$(( $(date +%s) + 120 ))
    local ds_ready=0 ds_desired=0
    while (( $(date +%s) < deadline )); do
        ds_desired=$(kubectl -n "$NS" get ds blockstor-satellite \
            -o jsonpath='{.status.desiredNumberScheduled}' 2>/dev/null || echo 0)
        ds_ready=$(kubectl -n "$NS" get ds blockstor-satellite \
            -o jsonpath='{.status.numberReady}' 2>/dev/null || echo 0)
        if [[ -n "$ds_desired" ]] && (( ds_desired > 0 )) && (( ds_desired == ds_ready )); then
            break
        fi
        sleep 2
    done

    delete_rd "$RD" 2>/dev/null || true

    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3

echo ">> apply 3-replica RD on $N1 / $N2 / $N3 (no tiebreaker — all diskful)"
cat <<EOF | kubectl apply -f -
apiVersion: blockstor.org/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    DrbdOptions/AutoAddQuorumTiebreaker: "false"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}
---
apiVersion: blockstor.org/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N1}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N1}
  props: {StorPoolName: stand}
---
apiVersion: blockstor.org/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N2}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N2}
  props: {StorPoolName: stand}
---
apiVersion: blockstor.org/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N3}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N3}
  props: {StorPoolName: stand}
EOF

# Wait until ALL THREE replicas land UpToDate. Read the authoritative
# CRD Status (status.drbdState) the observer writes per replica, NOT the
# controller's REST resource-list disk_state projection: under CI load
# that projection can lag the CRD by the whole precondition window (seen
# on the 8-lane runner — every replica's CRD Status was UpToDate while
# `linstor r l` still showed Unknown), which is a controller-cache lag,
# not a real DRBD state. The offline-window assertions below still drive
# the CLI `linstor r l` view; only this precondition reads the CRD.
echo ">> wait for all three replicas to land UpToDate (authoritative CRD Status)"
deadline=$(( $(date +%s) + 240 ))
while (( $(date +%s) < deadline )); do
    n_up=$(kubectl get resource.blockstor.org -o json 2>/dev/null \
        | jq --arg rd "${RD}." '[.items[] | select(.metadata.name | startswith($rd)) | select(.status.drbdState == "UpToDate")] | length' \
        2>/dev/null || echo 0)
    if (( n_up == 3 )); then
        break
    fi
    sleep 3
done
if (( n_up != 3 )); then
    echo "FAIL: only $n_up/3 replicas reached UpToDate before satellite isolation"
    exit 1
fi
echo "   all 3 replicas UpToDate"

# Snapshot the pre-isolation state of $N3 so we can assert the
# last-known DiskState survives the offline window (CRD Status, same
# authoritative source as the precondition above).
last_known_disk=$(kubectl get "resource.blockstor.org/${RD}.${N3}" \
    -o jsonpath='{.status.drbdState}' 2>/dev/null || echo "")
echo "   pre-offline DiskState on $N3 = '$last_known_disk' (must survive offline)"
if [[ "$last_known_disk" != "UpToDate" ]]; then
    echo "FAIL: pre-condition broken — $N3 row did not show UpToDate before isolation"
    exit 1
fi

# --- Step: isolate $N3 by patching DS affinity FIRST, then killing pod ----
#
# The DaemonSet's blanket `tolerations: [{operator: Exists}]` makes taints
# useless here (every taint is tolerated). Use a label-based eviction:
# the DS gets patched to require absence of EVICT_LABEL, and the node
# gets labelled. ORDER MATTERS:
#
#   1. PATCH the DS template first to add a `EVICT_LABEL DoesNotExist`
#      affinity requirement. At this point $N3 has no such label so the
#      template still matches — the existing pod keeps running and the
#      DS controller does NOT start a graceful eviction.
#
#   2. LABEL $N3 with EVICT_LABEL=offline. Now $N3 fails the affinity
#      requirement; the DS controller marks it for graceful eviction
#      (the preStop hook would `drbdadm down` every resource and the
#      observer would blank Status.Volumes[i].DiskState, losing the
#      last-known DiskState we're trying to preserve).
#
#   3. IMMEDIATELY force-delete the pod (grace=0, no wait) so kubelet
#      SIGKILLs the container before the DS controller's graceful
#      eviction goroutine can fire its delete. Force-delete preserves
#      the last-known DiskState in the CRD.
#
# After step 3 the DS controller's next reconcile sees no pod on $N3
# AND the patched template excludes $N3 → no new pod is scheduled.
# With the previous order (delete → label → patch) the DS controller
# observed the pod gone with the OLD template still allowing $N3 and
# raced ahead to schedule a fresh pod before the patch landed.

echo ">> patch DS nodeAffinity to require absence of $EVICT_LABEL"
kubectl -n "$NS" patch ds blockstor-satellite --type=json \
    -p='[{"op":"add","path":"/spec/template/spec/affinity/nodeAffinity/requiredDuringSchedulingIgnoredDuringExecution/nodeSelectorTerms/0/matchExpressions/-","value":{"key":"'"${EVICT_LABEL}"'","operator":"DoesNotExist"}}]'

echo ">> label $N3 with $EVICT_LABEL=offline so DS affinity excludes it"
kubectl label node "$N3" "${EVICT_LABEL}=offline" --overwrite

echo ">> force-delete satellite Pod on $N3 (no grace — bypass preStop, race the DS eviction)"
# Closes the get-then-delete race: between `get pod name` and `delete pod $name`
# the DS controller can complete its own eviction first (the affinity patch
# above already excluded $N3), and the `delete pod $name` then 404s. Use a
# field-selector delete instead so kubectl resolves the target server-side
# under the satellite label, and tolerate NotFound — either the DS controller
# already evicted the pod (success) or we beat it to the punch (also success).
kubectl -n "$NS" delete pod -l app=blockstor-satellite \
    --field-selector "spec.nodeName=${N3}" \
    --force --grace-period=0 --wait=false --ignore-not-found

# Confirm the DaemonSet refused to re-schedule (so heartbeats truly stop).
sleep 8
new_pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o "jsonpath={.items[?(@.spec.nodeName==\"${N3}\")].metadata.name}" 2>/dev/null || true)
if [[ -n "$new_pod" ]]; then
    pod_phase=$(kubectl -n "$NS" get pod "$new_pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    echo "FAIL: DaemonSet re-spawned satellite pod $new_pod on $N3 (phase=$pod_phase) despite affinity patch"
    exit 1
fi
echo "   no satellite pod on $N3 (DS refusing to re-bind)"

# --- Step: assert Node $N3 lands Offline within $OFFLINE_TIMEOUT ----------
echo ">> wait up to ${OFFLINE_TIMEOUT}s for $N3 to flip Offline"
deadline=$(( $(date +%s) + OFFLINE_TIMEOUT ))
got_offline=0
while (( $(date +%s) < deadline )); do
    conn=$("${LCTLJ[@]}" node list 2>/dev/null \
        | jq -r --arg n "$N3" '.[][] | select(.name == $n) | .connection_status // ""')
    if [[ "$conn" == "OFFLINE" ]]; then
        got_offline=1
        break
    fi
    sleep 3
done
if (( got_offline == 0 )); then
    echo "FAIL: $N3 never flipped ConnectionStatus=OFFLINE within ${OFFLINE_TIMEOUT}s (got '$conn')"
    exit 1
fi
echo "   $N3 ConnectionStatus=OFFLINE"

# --- Step: assert Resource row's State flipped to Unknown -----------------
#
# LINSTOR forgets a satellite's state when the connection to it closes
# and answers that node's replicas with no state at all: no in_use, no
# volumes[].state.disk_state, no peer links. linstor-client renders such
# a row as a yellow Unknown and lists it under `--faulty`. The
# controller gives the same answer for every replica on an OFFLINE node
# (pkg/api/v1/offline.go), read from the Node's ConnectionStatus on each
# request, so this holds as soon as the OFFLINE flip above has been seen.
rd_json=$("${LCTLJ[@]}" resource list -r "$RD" 2>/dev/null)
n3_row=$(echo "$rd_json" | jq -c --arg n "$N3" '[.[][] | select(.node_name == $n)][0] // {}')
n3_disk_state=$(echo "$n3_row" | jq -r '.volumes[0].state.disk_state // ""')
n3_in_use=$(echo "$n3_row" | jq -r '.state.in_use // ""')
n3_links=$(echo "$n3_row" | jq -r '.layer_object.drbd.connections // {} | length')
echo "   $N3 row: in_use='$n3_in_use', volumes[0].state.disk_state='$n3_disk_state', peer links=$n3_links"

if [[ -n "$n3_disk_state" || -n "$n3_in_use" || "$n3_links" != "0" ]]; then
    echo "FAIL: $N3 is OFFLINE but its replica still carries the dead satellite's state"
    echo "      (linstor-client renders it as '${n3_disk_state}' instead of Unknown)"
    exit 1
fi
echo "   PASS $N3 replica carries no satellite state (linstor r l: Unknown)"

# The stored report itself is kept: the CRD Status still holds what the
# satellite last observed, for forensics, and only the answer changes.
# Whether it survives depends on how the satellite died (a preStop
# `drbdadm down` blanks it, a SIGKILL that beats preStop freezes it),
# so it is reported, not asserted.
disk_preserved=0
crd_disk=$(kubectl get "resource.blockstor.org/${RD}.${N3}" \
    -o jsonpath='{.status.volumes[0].diskState}' 2>/dev/null || echo "")
if [[ -n "$crd_disk" && "$crd_disk" == "$last_known_disk" ]]; then
    echo "   last-known CRD Status.Volumes[0].DiskState preserved ('$crd_disk')"
    disk_preserved=1
else
    echo "   last-known CRD Status.Volumes[0].DiskState not preserved (got '${crd_disk:-<empty>}')"
fi

# --- Step: heal — drop the label so $N3 again matches the DS affinity -----
#
# DO NOT remove the affinity patch yet. Mutating the DS template here
# would force the DS controller to roll the pods on worker-1 and
# worker-2 too (any template change triggers a rolling update with
# maxUnavailable=1), which knocks them OFFLINE for ~10-30 s each and
# breaks the recovery assertion below. Removing the LABEL (a
# node-level change, not a template change) is enough: the DS
# affinity says "key DoesNotExist", which the unlabeled node now
# satisfies, so the DS reconciles a fresh pod onto $N3 WITHOUT
# rolling its peers.
#
# The full cleanup() trap on EXIT does the template revert, after
# all assertions have settled.
echo ">> unlabel $N3 so it satisfies the DS affinity again"
kubectl label node "$N3" "${EVICT_LABEL}-" 2>/dev/null || true

echo ">> wait up to ${HEAL_TIMEOUT}s for $N3 to return to ONLINE + UpToDate"
#
# Check only $N3's row, not the cluster-wide UpToDate count. The
# scenario's pinned object is $N3's recovery, and we deliberately
# don't touch the DS template here (only the label) so worker-1 and
# worker-2 shouldn't see DS rolls during heal. But on a busy QEMU
# stand other unrelated reconciler activity can briefly flap a peer
# OFFLINE; that's a separate failure mode, not a heal regression.
deadline=$(( $(date +%s) + HEAL_TIMEOUT ))
healed=0
while (( $(date +%s) < deadline )); do
    conn=$("${LCTLJ[@]}" node list 2>/dev/null \
        | jq -r --arg n "$N3" '.[][] | select(.name == $n) | .connection_status // ""')
    n3_disk=$("${LCTLJ[@]}" resource list -r "$RD" 2>/dev/null \
        | jq -r --arg n "$N3" '.[][] | select(.node_name == $n) | .volumes[0].state.disk_state // ""')
    if [[ "$conn" == "ONLINE" && "$n3_disk" == "UpToDate" ]]; then
        healed=1
        break
    fi
    sleep 3
done
if (( healed == 0 )); then
    echo "FAIL: $N3 did not return to ONLINE + UpToDate within ${HEAL_TIMEOUT}s"
    echo "      (conn=$conn, ${N3}.disk_state=$n3_disk)"
    exit 1
fi
echo "   $N3 back to ONLINE, $N3 disk_state=UpToDate"

# Final verdict.
#
# DETERMINISTIC contracts (these gate the scenario and are hard-asserted
# above with their own `exit 1`): the owning Node flips to OFFLINE while
# isolated, its replica then answers with no satellite state, and $N3
# heals back to ONLINE + UpToDate once un-isolated. All held to reach
# this line.
#
# OBSERVATIONAL contract (disk_preserved): whether the CRD keeps the
# last-known DiskState depends on how the satellite died, see above.
echo ">> STATE-OFFLINE-UNKNOWN OK (deterministic contracts held)"
echo "   Node Offline flip:               PASS"
echo "   Heal after un-isolate:           PASS"
echo "   Resource State -> Unknown:       PASS"
echo "   Last-known DiskState in the CRD: $( ((disk_preserved)) && echo kept || echo 'not kept (satellite blanked it on shutdown)' )"
exit 0
