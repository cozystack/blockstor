#!/usr/bin/env bash
#
# usage: recovery-deleting-convert.sh WORK_DIR
#
# Scenario 5.13 — DELETING stuck → convert + toggle-disk path
# (recovery-skill A2 / Method 2).
#
# Goal: validate the SKILL-documented recipe for unwedging a Resource
# whose parent satellite is unreachable. In upstream LINSTOR this
# materialises as a stuck "DELETING" copy on a node that is no longer
# answering; the controller cannot finish `r d` because the satellite
# never ACKs the FlagDelete clear. The recipe is:
#
#   linstor rd sp <rd> DrbdOptions/Resource/quorum off
#   linstor r td <node> <rd> --diskless        # convert DELETING → Diskless
#   <wait for FlagDelete to clear>
#   linstor r d <node> <rd>                     # retry; CRD finally goes
#   linstor rd sp <rd> DrbdOptions/Resource/quorum majority
#
# In the blockstor world the analog of "DELETING stuck" is a Resource
# CRD with a deletionTimestamp whose SatelliteResourceFinalizer cannot
# be stripped because no satellite pod is alive on the node. Toggling
# the replica to DISKLESS rewrites the satellite's DeleteResource
# request — the StoragePool field goes empty and the satellite skips
# the lower-layer provider.DeleteVolume + drbdadm-down loop, so when
# the satellite eventually returns (or the operator re-targets the
# delete) the finalizer-strip path is the only remaining work and it
# succeeds without diskful teardown. That is the Method 2 contract.
#
# Setup:
#   - 3-replica RD `stuck-rd` on workers 1/2/3, 64 MiB, autoplace
#     disabled (we want exactly those three peers — no auto-witness
#     race muddying the quorum prop assertions).
#   - Workers 1+2 reach UpToDate, then we kill worker-3's satellite
#     for the duration of the recipe.
#
# Steps:
#   1. Patch the satellite DaemonSet with a nodeSelector that EXCLUDES
#      worker-3 so the existing pod terminates and no replacement is
#      ever scheduled. `kubectl delete pod` alone would race the
#      DaemonSet controller and a new pod would land within seconds —
#      we need worker-3 to genuinely look "gone" for the rest of the
#      recipe.
#   2. `kubectl delete resource stuck-rd.<worker-3>` (analog of
#      `linstor r d worker-3 stuck-rd`). The CRD acquires a
#      deletionTimestamp; the satellite-side finalizer survives
#      because nobody is running on worker-3.
#   3. Apply Method 2 via REST (linstor-cli would route through the
#      same handlers, but we hit the API directly so the test stays
#      independent of CLI version skew on the stand):
#        - PUT /v1/resource-definitions/stuck-rd  with
#          override_props.DrbdOptions/Resource/quorum=off — drops the
#          parent RD's quorum policy so the surviving 2-of-3 cluster
#          keeps writing (regression guard for diskful path).
#        - PUT /v1/.../resources/<worker-3>/toggle-disk/diskless —
#          flips Spec.Flags["DISKLESS"] on the stuck replica.
#   4. Un-patch the DaemonSet so worker-3's satellite respawns. Its
#      handleDelete picks up the deletionTimestamp + DISKLESS flag,
#      DeleteResource sees StoragePool=="" and skips the diskful
#      teardown, finalizer strips, CRD vanishes.
#   5. Restore quorum=majority (Method 2 housekeeping). The RD now
#      has only 2 replicas left and the quorum policy reverts to the
#      diskful default.
#   6. Workers 1+2 must stay UpToDate throughout — verified at three
#      sample points (after delete, after toggle-disk, after CRD is
#      gone). A drop to Outdated/Inconsistent here means the recipe
#      itself interrupted the data path and the SKILL doc is wrong
#      for blockstor's reconciler model.
#
# Regression guards:
#   - The respawned satellite on worker-3 MUST NOT crash or surface an
#     "actionable error" trying to re-register under the same node
#     name (Bug 20 in the upstream LINSTOR catalog; in blockstor this
#     would surface as the satellite's startup probe failing or its
#     log emitting `actionable` / `register.*conflict` lines). Tail
#     the satellite log post-respawn for ~10 s; any actionable-class
#     match fails the test.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

RD=stuck-rd
N1=$WORKER_1
N2=$WORKER_2
N3=$WORKER_3
POOL=${STORPOOL:-stand}

# Cluster-local nodeSelector key: applying this label only to
# workers 1+2 (not 3) is what evicts the satellite from worker-3
# without touching anything else. Cleanup path puts the label back
# on every worker and drops the nodeSelector so the DaemonSet's
# default placement returns.
PIN_LABEL=blockstor.io/test-5-13=keep

restore_daemonset_and_label() {
    # Best-effort rollback in EXIT trap. Don't fail the trap if these
    # error — we still want delete_rd to run.
    kubectl -n "$NS" patch daemonset blockstor-satellite \
        --type=json -p='[{"op":"remove","path":"/spec/template/spec/nodeSelector/blockstor.io~1test-5-13"}]' \
        2>/dev/null || true
    for n in "$N1" "$N2" "$N3"; do
        kubectl label node "$n" "blockstor.io/test-5-13-" --overwrite 2>/dev/null || true
    done
    kubectl -n "$NS" rollout status daemonset blockstor-satellite --timeout=60s 2>/dev/null || true
}

cleanup() {
    restore_daemonset_and_label
    delete_rd "$RD"
}
trap cleanup EXIT

# ---------------------------------------------------------------------
# Phase 0: helpers — REST port-forward + state probes
# ---------------------------------------------------------------------

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward svc/blockstor-controller "${PF_PORT}":3370 \
    >/tmp/recovery-deleting-convert-pf.log 2>&1 &
PF_PID=$!

cleanup_with_pf() {
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
    cleanup
}
trap cleanup_with_pf EXIT

for _ in $(seq 1 30); do
    if curl -fsS -m 1 "http://127.0.0.1:${PF_PORT}/v1/healthz" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

disk_state() {
    status_disk_state "$RD" "$1"
}

assert_uptodate_12() {
    local tag=$1 s1 s2 deadline
    # Poll up to 15 s — Resource.Status can transiently report a
    # different state when the satellite reconciler is mid-`drbdadm
    # adjust` (the device is briefly absent from the kernel list
    # during the reconfigure window). A flaky single-shot read here
    # would mask the real regression we care about, which is a
    # *sustained* drop out of UpToDate on the diskful peers.
    deadline=$(( $(date +%s) + 15 ))
    while (( $(date +%s) < deadline )); do
        s1=$(disk_state "$N1")
        s2=$(disk_state "$N2")
        if [[ "$s1" == "UpToDate" && "$s2" == "UpToDate" ]]; then
            echo "   [$tag] N1+N2 UpToDate OK"
            return 0
        fi
        sleep 1
    done
    echo "FAIL[${tag}]: N1=$s1 / N2=$s2 — diskful peers must stay UpToDate"
    return 1
}

# ---------------------------------------------------------------------
# Phase 1: apply 3-replica RD, wait UpToDate
# ---------------------------------------------------------------------

echo ">> apply 3-replica RD ${RD} on ${N1}/${N2}/${N3} (autoplace off)"
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
  props:
    StorPoolName: "${POOL}"
EOF
done

# Reuse wait_uptodate for the (N1,N2) pair; check N3 separately so
# the 180 s budget is shared but per-peer status is explicit.
wait_uptodate "$RD" "$N1" "$N2"
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    if [[ "$(disk_state "$N3")" == *"UpToDate"* ]]; then
        break
    fi
    sleep 2
done
if [[ "$(disk_state "$N3")" != *"UpToDate"* ]]; then
    echo "FAIL: N3 not UpToDate before scenario starts"
    exit 1
fi
echo "   all 3 peers UpToDate"

# ---------------------------------------------------------------------
# Phase 2: stop worker-3's satellite (forcefully, until the recipe
# is fully applied). Patch DS with nodeSelector pinning to N1+N2,
# and only label those two so the existing pod on N3 is evicted and
# no replacement schedules.
# ---------------------------------------------------------------------

echo ">> stop satellite on ${N3} via DaemonSet nodeSelector pin"
kubectl label node "$N1" "$PIN_LABEL" --overwrite >/dev/null
kubectl label node "$N2" "$PIN_LABEL" --overwrite >/dev/null
kubectl label node "$N3" blockstor.io/test-5-13- --overwrite 2>/dev/null || true
kubectl -n "$NS" patch daemonset blockstor-satellite --type=merge -p '
spec:
  template:
    spec:
      nodeSelector:
        blockstor.io/test-5-13: keep
'

# Wait for worker-3's satellite pod to actually be gone — DS reconcile
# is fast but not instant; without this poll the next steps would race
# the pod's last reconcile loop.
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    pod_n3=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o "jsonpath={.items[?(@.spec.nodeName==\"${N3}\")].metadata.name}" 2>/dev/null || true)
    if [[ -z "$pod_n3" ]]; then
        break
    fi
    sleep 2
done
if [[ -n "$pod_n3" ]]; then
    echo "FAIL: satellite pod on ${N3} survived the DS pin (pod=${pod_n3})"
    exit 1
fi
echo "   satellite on ${N3} is gone"

# ---------------------------------------------------------------------
# Phase 3: delete the worker-3 replica. The CRD picks up a
# deletionTimestamp but the finalizer can't be stripped (no satellite
# on N3 to do so). Diskful peers must still be UpToDate.
# ---------------------------------------------------------------------

echo ">> delete Resource ${RD}.${N3} (will get stuck in 'DELETING')"
# Don't --wait — the delete will never finish on its own; we'll drive
# it home with the toggle-disk recipe below.
kubectl delete --wait=false "resource.blockstor.io.blockstor.io/${RD}.${N3}"

# Confirm it landed in the "stuck deleting" state, not "actually
# gone". A successful immediate delete here would mean blockstor isn't
# stamping the satellite finalizer at all — that's a separate bug, but
# either way the scenario can't proceed without a stuck CRD to recover.
sleep 5
dt=$(kubectl get "resource.blockstor.io.blockstor.io/${RD}.${N3}" \
    -o jsonpath='{.metadata.deletionTimestamp}' 2>/dev/null || true)
finalizers=$(kubectl get "resource.blockstor.io.blockstor.io/${RD}.${N3}" \
    -o jsonpath='{.metadata.finalizers}' 2>/dev/null || true)
echo "   N3 replica deletionTimestamp=${dt:-<missing>}  finalizers=${finalizers:-<none>}"
if [[ -z "$dt" ]]; then
    echo "FAIL: ${RD}.${N3} has no deletionTimestamp — delete may have completed unexpectedly"
    exit 1
fi
if [[ "$finalizers" != *"satellite-resource"* ]]; then
    echo "FAIL: ${RD}.${N3} missing satellite-resource finalizer; recipe cannot validate"
    exit 1
fi

assert_uptodate_12 "after delete"

# ---------------------------------------------------------------------
# Phase 4: Method 2 — RD quorum off + toggle-disk → diskless
# ---------------------------------------------------------------------

echo ">> apply Method 2 step 1: rd sp ${RD} DrbdOptions/Resource/quorum off"
curl -fsS -X PUT \
    -H 'Content-Type: application/json' \
    "http://127.0.0.1:${PF_PORT}/v1/resource-definitions/${RD}" \
    -d '{"override_props":{"DrbdOptions/Resource/quorum":"off"}}' >/dev/null

# Read it back so a silently-dropped patch (e.g. the PUT routing
# regression that bit linstor-cli.sh) fails the test loudly here
# instead of much later in the recipe.
got=$(curl -fsS "http://127.0.0.1:${PF_PORT}/v1/resource-definitions/${RD}" \
    | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("props",{}).get("DrbdOptions/Resource/quorum",""))')
if [[ "$got" != "off" ]]; then
    echo "FAIL: quorum prop not stamped on RD (got '${got}')"
    exit 1
fi
echo "   quorum=off stamped on RD"

echo ">> apply Method 2 step 2: r td ${N3} ${RD} --diskless (convert DELETING → Diskless)"
http_code=$(curl -s -o /tmp/rdc-td-out -w '%{http_code}' -X PUT \
    "http://127.0.0.1:${PF_PORT}/v1/resource-definitions/${RD}/resources/${N3}/toggle-disk/diskless")
echo "   toggle-disk/diskless HTTP ${http_code}"
if [[ "$http_code" != "200" ]]; then
    echo "FAIL: r td --diskless against a deletionTimestamp'd Resource returned ${http_code}"
    cat /tmp/rdc-td-out 2>/dev/null || true
    echo
    exit 1
fi

# The handler updates Spec.Flags["DISKLESS"]. Verify by reading the
# CRD — Spec must show the flag, regardless of whether the object is
# mid-deletion.
post_flags=$(kubectl get "resource.blockstor.io.blockstor.io/${RD}.${N3}" \
    -o jsonpath='{.spec.flags}' 2>/dev/null || true)
echo "   ${RD}.${N3} spec.flags after toggle-disk = ${post_flags}"
if [[ "$post_flags" != *"DISKLESS"* ]]; then
    echo "FAIL: DISKLESS flag not stamped on Spec — toggle-disk recipe broken for DELETING resources"
    exit 1
fi

assert_uptodate_12 "after toggle-disk"

# ---------------------------------------------------------------------
# Phase 5: restore satellite on N3 and watch the stub disappear
# ---------------------------------------------------------------------

echo ">> restore satellite on ${N3} (un-pin DaemonSet)"
kubectl -n "$NS" patch daemonset blockstor-satellite \
    --type=json -p='[{"op":"remove","path":"/spec/template/spec/nodeSelector/blockstor.io~1test-5-13"}]'

# Wait for the new pod to come Ready. 90 s is generous for QEMU image
# pull-cache on the stand; bumping to 120 s if the cluster is cold.
deadline=$(( $(date +%s) + 120 ))
sat_pod_n3=""
while (( $(date +%s) < deadline )); do
    sat_pod_n3=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o "jsonpath={.items[?(@.spec.nodeName==\"${N3}\")].metadata.name}" 2>/dev/null || true)
    if [[ -n "$sat_pod_n3" ]]; then
        phase=$(kubectl -n "$NS" get pod "$sat_pod_n3" -o jsonpath='{.status.phase}' 2>/dev/null || true)
        ready=$(kubectl -n "$NS" get pod "$sat_pod_n3" -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || true)
        if [[ "$phase" == "Running" && "$ready" == "true" ]]; then
            break
        fi
    fi
    sleep 2
done
if [[ -z "$sat_pod_n3" ]]; then
    echo "FAIL: satellite on ${N3} never respawned after DS unpin"
    exit 1
fi
echo "   satellite ${sat_pod_n3} on ${N3} is Running+Ready"

# Mark the satellite-respawn time so the log scan below is bounded
# to the post-respawn window.
respawn_ts=$(date +%s)

# Now the finalizer should clear: handleDelete fires, sees DISKLESS,
# DeleteResource with empty StoragePool skips the diskful path,
# satellite strips the finalizer, kube-apiserver finalises.
echo ">> wait up to 60s for ${RD}.${N3} CRD to vanish"
deadline=$(( $(date +%s) + 60 ))
gone=false
while (( $(date +%s) < deadline )); do
    if ! kubectl get "resource.blockstor.io.blockstor.io/${RD}.${N3}" >/dev/null 2>&1; then
        gone=true
        break
    fi
    sleep 2
done
if [[ "$gone" != "true" ]]; then
    echo "FAIL: ${RD}.${N3} CRD still present after satellite respawn + toggle-disk recipe"
    kubectl get "resource.blockstor.io.blockstor.io/${RD}.${N3}" -o yaml 2>/dev/null || true
    exit 1
fi
echo "   ${RD}.${N3} CRD is gone — recipe converged"

# ---------------------------------------------------------------------
# Phase 6: post-recovery housekeeping
# ---------------------------------------------------------------------

echo ">> restore RD quorum policy (rd sp ${RD} ... quorum majority)"
curl -fsS -X PUT \
    -H 'Content-Type: application/json' \
    "http://127.0.0.1:${PF_PORT}/v1/resource-definitions/${RD}" \
    -d '{"override_props":{"DrbdOptions/Resource/quorum":"majority"}}' >/dev/null

assert_uptodate_12 "after CRD gone"

# Bug 20 actionable-error scan: the respawned satellite must not be
# stuck trying to re-register under the same node name. Surface any
# `actionable` / `register.*conflict` log line emitted since respawn.
echo ">> scan ${sat_pod_n3} log for Bug 20 actionable-class errors"
elapsed=$(( $(date +%s) - respawn_ts ))
# Tail the entire post-respawn window so a fast bring-up doesn't miss
# anything; 30 s minimum so we cover steady-state registration.
if (( elapsed < 30 )); then
    sleep $(( 30 - elapsed ))
fi
log_since=$(( $(date +%s) - respawn_ts ))
actionable_hits=$(kubectl -n "$NS" logs "$sat_pod_n3" --since="${log_since}s" 2>/dev/null \
    | grep -iE 'actionable|register.*conflict|already.*registered' \
    | grep -vi 'no.*actionable' | wc -l || true)
if (( actionable_hits > 0 )); then
    echo "FAIL: satellite ${sat_pod_n3} surfaced ${actionable_hits} Bug-20-class log line(s):"
    kubectl -n "$NS" logs "$sat_pod_n3" --since="${log_since}s" 2>/dev/null \
        | grep -iE 'actionable|register.*conflict|already.*registered' \
        | grep -vi 'no.*actionable' | head -10
    exit 1
fi
echo "   no Bug-20 actionable-class lines in ${sat_pod_n3} log"

echo ">> RECOVERY-DELETING-CONVERT OK (N3 stub removed via Method 2; N1+N2 stayed UpToDate)"
