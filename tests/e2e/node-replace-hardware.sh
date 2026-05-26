#!/usr/bin/env bash
#
# usage: node-replace-hardware.sh WORK_DIR
#
# Scenario 5.W11 — Permanent node failure: replace with new hardware.
# Source: drbd-troubleshooting §"Dealing with permanent node failure"
# (lines 192-216), wave2-05 §5.W11.
#
# Recipe under test (operator-driven, 4 steps):
#
#   1. `linstor node lost <old>` cascade-deletes the dead satellite +
#      its storage-pool registrations + every replica row on it
#      (wave2 4.W04 is closed → cascade is a documented side-effect).
#   2. Install new hardware (fresh OS + LINSTOR satellite). Modelled
#      here by re-asserting the Node CRD via REST `POST /v1/nodes`
#      — the same path `linstor node create <new> <ip>` hits.
#   3. `linstor sp create <new> stand` re-registers the per-node
#      backing pool. Without this the autoplacer skips the node:
#      Node row exists but no candidate pool. Modelled via REST
#      `POST /v1/nodes/{node}/storage-pools`.
#   4. `linstor r c <rd> --auto-place` re-spawns the missing replica
#      onto the freshly-installed node so the RD returns to its
#      requested replica count. Modelled via REST
#      `POST /v1/resource-definitions/{rd}/autoplace`.
#
# What this script pins (recipe-contract test):
#
#   - Each of the 4 REST calls returns 200/201 in sequence; the
#     order is load-bearing — sp create on a missing node 404s,
#     autoplace with no eligible pool returns 0 candidates.
#   - After step 4, the RD's diskful replica count is back to 2
#     (one survivor on $WORKER_1, one fresh on $WORKER_3) and
#     both peers reach UpToDate within wait_uptodate's 180 s.
#   - The recipe is reconciler-quiet: blockstor must not undo
#     any of the 4 operator steps mid-sequence.
#
# Out of scope: kernel-level metadata wipe, fresh-disk size check
# (DRBD refuses smaller disks) — that's a satellite-side concern
# the recipe assumes was done before step 2. This script is the
# command-contract guard, not a full bare-metal swap simulation.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

RD=test-replace-hw
# The DaemonSet blanket-tolerates every taint (`operator: Exists`), so
# a NoSchedule taint cannot keep the satellite Pod off $WORKER_3. Use a
# label-based eviction the way state-offline-unknown.sh does: patch the
# DS to require absence of $EVICT_LABEL, then label the node.
EVICT_LABEL=blockstor.io/replace-hw-test
POOL_NAME=stand

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward deploy/blockstor-apiserver "$PF_PORT":3370 \
    >/tmp/node-replace-hw-pf.log 2>&1 &
PF_PID=$!

dump_diag() {
    echo "---- dump: GET /v1/nodes ----"
    curl -fsS "http://localhost:$PF_PORT/v1/nodes" 2>/dev/null | jq . || true
    echo "---- dump: GET /v1/view/storage-pools ----"
    curl -fsS "http://localhost:$PF_PORT/v1/view/storage-pools" 2>/dev/null | jq . || true
    echo "---- dump: GET /v1/view/resources ----"
    curl -fsS "http://localhost:$PF_PORT/v1/view/resources?resource=$RD" 2>/dev/null | jq . || true
    echo "---- dump: kubectl get pods -n $NS ----"
    kubectl get pods -n "$NS" -o wide || true
}

cleanup() {
    local rc=$?
    if (( rc != 0 )); then
        dump_diag
    fi

    # Strip the affinity patch + the eviction label so the DaemonSet
    # re-spawns the satellite. Doing this before the pool re-apply +
    # delete_rd lets the satellite-side teardown actually run against
    # any replica we placed on WORKER_3 (otherwise the finalizer hangs
    # and the next scenario observes residue).
    kubectl -n "$NS" patch ds blockstor-satellite --type=json \
        -p='[{"op":"remove","path":"/spec/template/spec/affinity/nodeAffinity/requiredDuringSchedulingIgnoredDuringExecution/nodeSelectorTerms/0/matchExpressions/1"}]' \
        2>/dev/null || true
    kubectl label node "$WORKER_3" "${EVICT_LABEL}-" 2>/dev/null || true

    # Re-bootstrap the WORKER_3 Node CRD + ALL per-node pools the stand
    # provisions — mirrors node-lost.sh's cleanup so the next test in
    # the batch sees a usable 3-node cluster. `n lost` cascade-deletes
    # EVERY per-node StoragePool CRD (stand/lvm-thin/zfs-thin), so all
    # three shapes must be reasserted, not just `stand`. Reference for
    # the exact CRD shapes: stand/install-pools.sh +
    # stand/blockstor-storagepools.yaml. Re-applying only `stand` left
    # WORKER_3 without lvm-thin/zfs-thin and cascaded into the downstream
    # tests (Run 54 trigger).
    #
    # ORDER MATTERS: re-apply the pools BEFORE delete_rd so the
    # satellite-side teardown of any replica we placed on WORKER_3 runs
    # against re-registered pools instead of hanging on a missing pool.
    local ip
    ip=$(kubectl get node "$WORKER_3" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null || true)
    if [[ -n "$ip" ]]; then
        cat <<EOF | kubectl apply -f - 2>/dev/null || true
apiVersion: blockstor.cozystack.io/v1alpha1
kind: Node
metadata: {name: $WORKER_3}
spec:
  type: SATELLITE
  netInterfaces:
    - {name: default, address: $ip}
---
apiVersion: blockstor.cozystack.io/v1alpha1
kind: StoragePool
metadata: {name: ${POOL_NAME}.${WORKER_3}}
spec:
  nodeName: $WORKER_3
  poolName: $POOL_NAME
  providerKind: FILE_THIN
  props:
    StorDriver/FileDir: /var/lib/blockstor-pool
---
apiVersion: blockstor.cozystack.io/v1alpha1
kind: StoragePool
metadata: {name: lvm-thin.${WORKER_3}}
spec:
  nodeName: $WORKER_3
  poolName: lvm-thin
  providerKind: LVM_THIN
  props:
    StorDriver/LvmVg: blockstor-lvm
    StorDriver/ThinPool: thin
---
apiVersion: blockstor.cozystack.io/v1alpha1
kind: StoragePool
metadata: {name: zfs-thin.${WORKER_3}}
spec:
  nodeName: $WORKER_3
  poolName: zfs-thin
  providerKind: ZFS_THIN
  props:
    StorDriver/ZPoolThin: blockstor-zfs
EOF
    fi

    # Now tear down the test RD. With the pools re-registered above the
    # satellite finalizer can complete against a live pool registration.
    delete_rd "$RD" 2>/dev/null || true

    # Wait briefly for the satellite Pod to come back so the next
    # test in the batch sees a Ready WORKER_3.
    local deadline=$(( $(date +%s) + 60 ))
    while (( $(date +%s) < deadline )); do
        ready=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
            -o "jsonpath={.items[?(@.spec.nodeName==\"${WORKER_3}\")].status.containerStatuses[0].ready}" 2>/dev/null || true)
        [[ "$ready" == "true" ]] && break
        sleep 2
    done

    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 20); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTLJ=()
if command -v linstor >/dev/null 2>&1; then
    LCTLJ=(linstor --controllers "http://localhost:$PF_PORT" --machine-readable)
fi

# Resolve WORKER_3's IP up-front; we need it both for `node create`
# (step 2) and for the cleanup-trap fallback.
W3_IP=$(kubectl get node "$WORKER_3" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
if [[ -z "$W3_IP" ]]; then
    echo "FAIL: could not resolve $WORKER_3 InternalIP" >&2
    exit 1
fi

echo ">> seed $RD: 2 diskful replicas on $WORKER_1 + $WORKER_3 (one will die)"
curl -fsS -XPOST -H'Content-Type: application/json' \
    "http://localhost:$PF_PORT/v1/resource-definitions" \
    -d "{\"resource_definition\":{\"name\":\"$RD\"}}" >/dev/null
curl -fsS -XPOST -H'Content-Type: application/json' \
    "http://localhost:$PF_PORT/v1/resource-definitions/$RD/volume-definitions" \
    -d '{"volume_definition":{"size_kib":102400}}' >/dev/null
# Disable auto-tiebreaker so the placer's gap-fill in step 4 lands
# a true diskful replica, not a DISKLESS witness on a survivor.
curl -fsS -XPUT -H'Content-Type: application/json' \
    "http://localhost:$PF_PORT/v1/resource-definitions/$RD" \
    -d '{"override_props":{"DrbdOptions/AutoAddQuorumTiebreaker":"false"}}' >/dev/null
curl -fsS -XPOST -H'Content-Type: application/json' \
    "http://localhost:$PF_PORT/v1/resource-definitions/$RD/resources" \
    -d "[{\"resource\":{\"node_name\":\"$WORKER_1\",\"props\":{\"StorPoolName\":\"$POOL_NAME\"}}}]" >/dev/null
curl -fsS -XPOST -H'Content-Type: application/json' \
    "http://localhost:$PF_PORT/v1/resource-definitions/$RD/resources" \
    -d "[{\"resource\":{\"node_name\":\"$WORKER_3\",\"props\":{\"StorPoolName\":\"$POOL_NAME\"}}}]" >/dev/null

wait_uptodate "$RD" "$WORKER_1" "$WORKER_3"

echo ">> simulate permanent failure: isolate $WORKER_3 so the DS stops re-spawning the satellite"
# ORDER MATTERS (copied from state-offline-unknown.sh §"isolate $N3"):
#   1. PATCH the DS template FIRST so the affinity gate exists before
#      the eviction label appears. At this point $WORKER_3 has no
#      label so the existing pod keeps running.
#   2. LABEL $WORKER_3 — DS controller now marks it for eviction.
#   3. IMMEDIATELY force-delete the pod (grace=0) so kubelet SIGKILLs
#      the container before the DS controller's graceful eviction
#      goroutine fires its own delete. Force-delete bypasses the
#      preStop hook, which is what we want: a "permanent hardware
#      failure" leaves no time for an orderly drbdadm down.
echo ">> patch DS nodeAffinity to require absence of $EVICT_LABEL"
kubectl -n "$NS" patch ds blockstor-satellite --type=json \
    -p='[{"op":"add","path":"/spec/template/spec/affinity/nodeAffinity/requiredDuringSchedulingIgnoredDuringExecution/nodeSelectorTerms/0/matchExpressions/-","value":{"key":"'"${EVICT_LABEL}"'","operator":"DoesNotExist"}}]'

echo ">> label $WORKER_3 with $EVICT_LABEL=offline so DS affinity excludes it"
kubectl label node "$WORKER_3" "${EVICT_LABEL}=offline" --overwrite

echo ">> force-delete satellite Pod on $WORKER_3 (no grace — bypass preStop, race the DS eviction)"
sat_pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o "jsonpath={.items[?(@.spec.nodeName==\"${WORKER_3}\")].metadata.name}")
if [[ -n "$sat_pod" ]]; then
    kubectl -n "$NS" delete pod "$sat_pod" --force --grace-period=0 --wait=false
fi

# Confirm the DS really refused to re-spawn the pod — otherwise
# heartbeats keep flowing and the controller's watchdog will never
# flip ConnectionStatus=OFFLINE, and `n lost` 409s in step 1 with
# "satellite is still reporting as ONLINE".
sleep 8
new_pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o "jsonpath={.items[?(@.spec.nodeName==\"${WORKER_3}\")].metadata.name}" 2>/dev/null || true)
if [[ -n "$new_pod" ]]; then
    pod_phase=$(kubectl -n "$NS" get pod -n "$NS" "$new_pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    echo "FAIL: DaemonSet re-spawned satellite pod $new_pod on $WORKER_3 (phase=$pod_phase) despite affinity patch"
    exit 1
fi

# Wait for the controller's heartbeat watchdog to flip $WORKER_3 to
# ConnectionStatus=OFFLINE. `linstor n lost` is gated by this — it
# refuses with HTTP 409 ("Node cannot be lost while its satellite is
# still reporting as ONLINE") as long as the controller believes the
# satellite is reachable. Budget: 40 s heartbeat grace + 5 s watchdog
# requeue + apiserver/SSA slack on a busy QEMU stand.
OFFLINE_TIMEOUT=75
echo ">> wait up to ${OFFLINE_TIMEOUT}s for $WORKER_3 ConnectionStatus=OFFLINE"
deadline=$(( $(date +%s) + OFFLINE_TIMEOUT ))
got_offline=0
conn=""
while (( $(date +%s) < deadline )); do
    conn=$(curl -fsS "http://localhost:$PF_PORT/v1/nodes" 2>/dev/null \
        | jq -r --arg n "$WORKER_3" '.[] | select(.name == $n) | .connection_status // ""')
    if [[ "$conn" == "OFFLINE" ]]; then
        got_offline=1
        break
    fi
    sleep 3
done
if (( got_offline == 0 )); then
    echo "FAIL: $WORKER_3 never flipped ConnectionStatus=OFFLINE within ${OFFLINE_TIMEOUT}s (got '$conn')"
    echo "      \`linstor n lost\` will 409 in step 1 — see drbd-troubleshooting docs."
    exit 1
fi
echo "   $WORKER_3 ConnectionStatus=OFFLINE — safe to call \`n lost\`"

# ===========================================================
# STEP 1: linstor node lost <old>
# ===========================================================
echo ">> STEP 1: DELETE /v1/nodes/$WORKER_3/lost (cascade-delete old satellite)"
http1=$(curl -sS -o /tmp/replace-hw-1.out -w '%{http_code}' \
    -XDELETE "http://localhost:$PF_PORT/v1/nodes/$WORKER_3/lost")
if [[ "$http1" != "200" ]]; then
    echo "FAIL: step 1 (node lost) returned http=$http1 body=$(cat /tmp/replace-hw-1.out)"
    exit 1
fi
# Sanity: the Node CRD must be gone now — the recipe's premise
# is that step 2 re-creates it from a clean slate, not patches an
# existing row.
if kubectl get "nodes.blockstor.cozystack.io/${WORKER_3}" >/dev/null 2>&1; then
    echo "FAIL: Node CRD $WORKER_3 still present after lost"
    exit 1
fi

# ===========================================================
# STEP 2: linstor node create <new> <ip>
#         (= install new hardware + register satellite)
# ===========================================================
echo ">> STEP 2: POST /v1/nodes (register replacement hardware as $WORKER_3 @ $W3_IP)"
http2=$(curl -sS -o /tmp/replace-hw-2.out -w '%{http_code}' \
    -XPOST -H'Content-Type: application/json' \
    "http://localhost:$PF_PORT/v1/nodes" \
    -d "{\"name\":\"$WORKER_3\",\"type\":\"SATELLITE\",\"net_interfaces\":[{\"name\":\"default\",\"address\":\"$W3_IP\"}]}")
if [[ "$http2" != "201" ]]; then
    echo "FAIL: step 2 (node create) returned http=$http2 body=$(cat /tmp/replace-hw-2.out)"
    exit 1
fi
# Cross-check: the autoCreate'd DfltDisklessStorPool fires here too
# (audit row #3), so /v1/view/storage-pools?nodes=$WORKER_3 must
# already list at least the diskless pool.
diskless_present=$(curl -fsS "http://localhost:$PF_PORT/v1/view/storage-pools?nodes=$WORKER_3" \
    | jq -r '[.[] | select(.provider_kind == "DISKLESS")] | length')
if (( diskless_present < 1 )); then
    echo "FAIL: step 2 did not auto-create DfltDisklessStorPool on $WORKER_3"
    exit 1
fi

# ===========================================================
# STEP 3: linstor sp create <new> stand
#         (= register the backing storage pool the autoplacer needs)
# ===========================================================
echo ">> STEP 3: POST /v1/nodes/$WORKER_3/storage-pools (re-register $POOL_NAME on new hw)"
http3=$(curl -sS -o /tmp/replace-hw-3.out -w '%{http_code}' \
    -XPOST -H'Content-Type: application/json' \
    "http://localhost:$PF_PORT/v1/nodes/$WORKER_3/storage-pools" \
    -d "{\"storage_pool_name\":\"$POOL_NAME\",\"node_name\":\"$WORKER_3\",\"provider_kind\":\"FILE_THIN\",\"props\":{\"StorDriver/FileDir\":\"/var/lib/blockstor-pool\"}}")
if [[ "$http3" != "201" ]]; then
    echo "FAIL: step 3 (sp create) returned http=$http3 body=$(cat /tmp/replace-hw-3.out)"
    exit 1
fi
# Sanity: the pool must now appear in the per-node view; without
# it the autoplacer in step 4 has no candidate to land on.
sp_present=$(curl -fsS "http://localhost:$PF_PORT/v1/view/storage-pools?nodes=$WORKER_3" \
    | jq -r --arg p "$POOL_NAME" '[.[] | select(.storage_pool_name == $p)] | length')
if (( sp_present < 1 )); then
    echo "FAIL: step 3 did not surface $POOL_NAME on $WORKER_3"
    exit 1
fi

# ===========================================================
# STEP 4: linstor r c <rd> --auto-place
#         (= re-spawn the missing replica on the fresh hardware)
# ===========================================================
# Un-isolate the kube-node so the satellite DS can rebind — without
# this the autoplace's selection is fine but the kernel-side
# resource-create on $WORKER_3 has no satellite to apply onto.
kubectl -n "$NS" patch ds blockstor-satellite --type=json \
    -p='[{"op":"remove","path":"/spec/template/spec/affinity/nodeAffinity/requiredDuringSchedulingIgnoredDuringExecution/nodeSelectorTerms/0/matchExpressions/1"}]' \
    2>/dev/null || true
kubectl label node "$WORKER_3" "${EVICT_LABEL}-" 2>/dev/null || true

# Wait briefly for the satellite Pod to come back; without a Ready
# satellite the kernel-side resource-create on $WORKER_3 has nothing
# to apply onto and the new replica never reaches UpToDate.
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
    ready=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
        -o "jsonpath={.items[?(@.spec.nodeName==\"${WORKER_3}\")].status.containerStatuses[0].ready}" 2>/dev/null || true)
    [[ "$ready" == "true" ]] && break
    sleep 2
done

echo ">> STEP 4: POST /v1/resource-definitions/$RD/autoplace (refill to 2 diskful)"
# Pin node_name_list to {WORKER_1 (survivor), WORKER_3 (replacement)}.
# Without it the autoplacer is free to pick WORKER_2 — its `stand` pool
# is score-identical to WORKER_3's, and the NodeName tie-break favours
# WORKER_2 — but the assertions below (wait_uptodate on W1+W3, on_w3==1)
# require the refilled replica on WORKER_3. This makes placement
# deterministic without changing the lost→create→sp→autoplace recipe.
http4=$(curl -sS -o /tmp/replace-hw-4.out -w '%{http_code}' \
    -XPOST -H'Content-Type: application/json' \
    "http://localhost:$PF_PORT/v1/resource-definitions/$RD/autoplace" \
    -d "{\"select_filter\":{\"place_count\":2,\"storage_pool\":\"$POOL_NAME\",\"node_name_list\":[\"$WORKER_1\",\"$WORKER_3\"]}}")
if [[ "$http4" != "200" && "$http4" != "201" ]]; then
    echo "FAIL: step 4 (autoplace) returned http=$http4 body=$(cat /tmp/replace-hw-4.out)"
    exit 1
fi

echo ">> wait up to 180s for the new replica on $WORKER_3 to be UpToDate"
wait_uptodate "$RD" "$WORKER_1" "$WORKER_3"

# Final assert: exactly 2 diskful replicas (one of which is on $WORKER_3
# — the freshly-installed hardware). Anything else means the recipe
# converged to the wrong topology.
diskful_count=$(curl -fsS "http://localhost:$PF_PORT/v1/view/resources?resource=$RD" \
    | jq -r '[.[] | select((.flags // []) | index("DISKLESS") | not)] | length')
if (( diskful_count != 2 )); then
    echo "FAIL: expected 2 diskful replicas after recipe, got $diskful_count"
    exit 1
fi

on_w3=$(curl -fsS "http://localhost:$PF_PORT/v1/view/resources?resource=$RD" \
    | jq -r --arg w3 "$WORKER_3" \
    '[.[] | select(.node_name == $w3) | select((.flags // []) | index("DISKLESS") | not)] | length')
if (( on_w3 != 1 )); then
    echo "FAIL: replacement replica did not land on $WORKER_3 (got $on_w3)"
    exit 1
fi

echo ">> NODE-REPLACE-HARDWARE OK (lost → create → sp create → autoplace recipe converges)"
