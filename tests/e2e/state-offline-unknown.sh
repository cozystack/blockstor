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
#      State=Unknown — the observer can no longer prove anything
#      about that replica's DRBD state, so the wire-side projection
#      must not silently re-emit a stale UpToDate.
#
#   3. The last-known per-volume DiskState must NOT be wiped to
#      empty — operators need it for forensics ("the last time the
#      satellite was talking, this replica was UpToDate / Inconsistent
#      / Failed / ..."). The CRD Status.Volumes[*].DiskState is the
#      source of truth for the disk-state column; only the
#      resource-level State should collapse to Unknown.
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
    echo "SKIP: linstor CLI not in PATH"
    exit 0
fi

RD=e2e-5-5-offline-unknown
EVICT_LABEL=blockstor.io/offline-unknown-test
OFFLINE_TIMEOUT=75    # 40 s grace + watchdog requeue period (5 s) +
                      # apiserver/SSA-apply slack on a busy QEMU stand
HEAL_TIMEOUT=180      # satellite startup + heartbeat (max 40 s for the
                      # watchdog to flip ConnectionStatus back) +
                      # DRBD reconnect + full re-sync of the
                      # offline-window-delta on a busy QEMU stand

# --- REST port-forward (random ephemeral port for parallel-iter safety) ---
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward svc/blockstor-controller "$PF_PORT":3370 \
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
    kubectl get nodes.blockstor.io.blockstor.io -o wide || true
    echo "---- dump: Resource CRDs (Status) ----"
    kubectl get resources.blockstor.io.blockstor.io -o yaml | \
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

    # Wait briefly for satellite Pod readiness before tearing the RD —
    # gives delete_rd's per-pod drbdsetup-down clean shot at WORKER_3.
    local deadline=$(( $(date +%s) + 60 ))
    while (( $(date +%s) < deadline )); do
        local ready
        ready=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
            -o "jsonpath={.items[?(@.spec.nodeName==\"${WORKER_3}\")].status.containerStatuses[0].ready}" 2>/dev/null || true)
        [[ "$ready" == "true" ]] && break
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
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: ResourceDefinition
metadata: {name: ${RD}}
spec:
  props:
    DrbdOptions/AutoAddQuorumTiebreaker: "false"
  volumeDefinitions:
    - {volumeNumber: 0, sizeKib: 65536}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N1}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N1}
  props: {StorPoolName: stand}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N2}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N2}
  props: {StorPoolName: stand}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Resource
metadata: {name: ${RD}.${N3}}
spec:
  resourceDefinitionName: ${RD}
  nodeName: ${N3}
  props: {StorPoolName: stand}
EOF

# Wait until ALL THREE replicas land UpToDate. wait_uptodate as-is only
# checks two peers, so spin our own 3-way poll using the REST view (the
# observer already projects DiskState per replica into the CRD Status).
echo ">> wait for all three replicas to land UpToDate"
deadline=$(( $(date +%s) + 240 ))
while (( $(date +%s) < deadline )); do
    n_up=$("${LCTLJ[@]}" resource list -r "$RD" 2>/dev/null \
        | jq '[.[][] | select(.volumes[]?.state.disk_state == "UpToDate")] | length' \
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

# Snapshot the pre-isolation state of $N3's row so we can assert the
# last-known DiskState survives the offline window.
last_known_disk=$("${LCTLJ[@]}" resource list -r "$RD" 2>/dev/null \
    | jq -r --arg n "$N3" '.[][] | select(.node_name == $n) | .volumes[0].state.disk_state // ""')
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
sat_pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o "jsonpath={.items[?(@.spec.nodeName==\"${N3}\")].metadata.name}")
if [[ -n "$sat_pod" ]]; then
    kubectl -n "$NS" delete pod "$sat_pod" --force --grace-period=0 --wait=false
fi

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
# The Python CLI's `linstor r l` State column reads
# `volumes[*].state.disk_state` (per-volume) AND the resource-level
# `state.drbd_state`. The expectation from the scenario brief is that
# the resource-level State collapses to Unknown when the satellite is
# offline, while the last-known per-volume DiskState is preserved.
#
# Probe both: parse the REST JSON for the offline node's row.
rd_json=$("${LCTLJ[@]}" resource list -r "$RD" 2>/dev/null)
n3_drbd_state=$(echo "$rd_json" \
    | jq -r --arg n "$N3" '.[][] | select(.node_name == $n) | .state.drbd_state // ""')
n3_disk_state=$(echo "$rd_json" \
    | jq -r --arg n "$N3" '.[][] | select(.node_name == $n) | .volumes[0].state.disk_state // ""')

echo "   $N3 row: state.drbd_state='$n3_drbd_state', volumes[0].state.disk_state='$n3_disk_state'"

# Also check what the Python CLI renders for the State column — that's
# the user-visible answer, derived from `volumes[].state.disk_state`
# (via VolumeCommands.volume_state_cell in linstor_client/commands/
# vlm_cmds.py). When that field is empty, the CLI shows "Unknown"
# (yellow); when it has a value, the CLI shows that value verbatim.
cli_state=$("${LCTLJ[@]}" resource list -r "$RD" 2>/dev/null \
    | jq -r --arg n "$N3" '.[][] | select(.node_name == $n) | .volumes[0].state.disk_state // ""')
echo "   $N3 CLI-rendered State (from volumes[0].state.disk_state) = '${cli_state:-Unknown}'"

# Architecture observation pinned by this scenario (open issue):
#
# The blockstor controller has NO projection layer that translates
# "owning satellite is OFFLINE" → resource-level State=Unknown. Both
# state.drbd_state and volumes[*].state.disk_state are echoed straight
# from the CRD Status, which the satellite owns via SSA. When the
# satellite dies:
#   - if its preStop hook had a chance to `drbdadm down` first,
#     observer events2 frames empty out volumes[].state.disk_state →
#     CLI renders State=Unknown (yellow), and operators LOSE the
#     last-known disk-state snapshot;
#   - if SIGKILL beats preStop (force --grace-period=0), the CRD
#     retains its last-known DiskState=UpToDate → CLI renders
#     State=UpToDate (green) for a replica the controller can no
#     longer prove anything about.
#
# The brief's contract — "State=Unknown AND last-known DiskState
# preserved" — requires both at once, which is architecturally
# impossible with the current single-field projection. The fix
# would be a controller-side projection that writes
# Status.DrbdState=Unknown (resource-level, separate from
# Volumes[].DiskState) whenever the owning Node's ConnectionStatus
# is OFFLINE, so the wire shape carries both Unknown (resource
# state) AND UpToDate (last-known disk state) at once.

state_flipped=0
disk_preserved=0

case "$n3_drbd_state" in
    "" | "Unknown" )
        echo "   PASS resource-level state.drbd_state landed Unknown ('$n3_drbd_state')"
        state_flipped=1
        ;;
    * )
        echo "   FAIL state.drbd_state still '$n3_drbd_state' after OFFLINE flip"
        echo "        (no controller-side projection of OFFLINE → Unknown on the"
        echo "        Resource CRD; field stays at the satellite's last write)"
        ;;
esac

if [[ -n "$n3_disk_state" && "$n3_disk_state" == "$last_known_disk" ]]; then
    echo "   PASS last-known volumes[0].DiskState preserved ('$n3_disk_state')"
    disk_preserved=1
else
    echo "   FAIL last-known volumes[0].DiskState was lost (got '${n3_disk_state:-<empty>}'," \
         "expected '$last_known_disk')"
    echo "        (observer's events2 frames during pod shutdown blanked the field"
    echo "        before SIGKILL arrived)"
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

# Final verdict — the scenario brief requires BOTH:
#   (a) resource-level State=Unknown after OFFLINE flip
#   (b) last-known per-volume DiskState preserved
# Both have to hold simultaneously for a green pass. If either side
# fails, surface the architecture gap loudly so the regression is
# visible in the CI log.
if (( state_flipped == 1 && disk_preserved == 1 )); then
    echo ">> STATE-OFFLINE-UNKNOWN OK"
    echo "   Node Offline flip: PASS"
    echo "   Resource State -> Unknown: PASS"
    echo "   Last-known DiskState preserved: PASS"
    echo "   Heal after un-isolate: PASS"
    exit 0
fi

echo ">> STATE-OFFLINE-UNKNOWN FAIL (open issue)"
echo "   Node Offline flip:                       PASS"
echo "   Resource State -> Unknown:               $( ((state_flipped))  && echo PASS || echo FAIL )"
echo "   Last-known DiskState preserved:          $( ((disk_preserved)) && echo PASS || echo FAIL )"
echo "   Heal after un-isolate:                   PASS"
echo "   Gap: blockstor has no controller-side projection that flips"
echo "        Resource.Status.DrbdState=Unknown when the owning Node's"
echo "        ConnectionStatus is OFFLINE. The CLI's State column reads"
echo "        volumes[].state.disk_state directly, so satisfying both"
echo "        contracts simultaneously requires a new projection layer."
exit 1
