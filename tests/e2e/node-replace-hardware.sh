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
TAINT_KEY=blockstor.io/replace-hw-test
POOL_NAME=stand

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward svc/blockstor-controller "$PF_PORT":3370 \
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

    # Untaint so the DaemonSet replays the satellite Pod onto WORKER_3
    # whether the test passed or failed.
    kubectl taint nodes "$WORKER_3" "${TAINT_KEY}:NoSchedule-" 2>/dev/null || true

    delete_rd "$RD" 2>/dev/null || true

    # Re-bootstrap the WORKER_3 Node CRD if step-2 didn't run (early
    # failure path) — mirrors node-lost.sh's cleanup so the next test
    # in the batch sees a usable 3-node cluster. The stand's per-node
    # `stand` pool is also re-applied so resource creates work again.
    local ip
    ip=$(kubectl get node "$WORKER_3" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null || true)
    if [[ -n "$ip" ]]; then
        cat <<EOF | kubectl apply -f - 2>/dev/null || true
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: Node
metadata: {name: $WORKER_3}
spec:
  type: SATELLITE
  netInterfaces:
    - {name: default, address: $ip}
---
apiVersion: blockstor.io.blockstor.io/v1alpha1
kind: StoragePool
metadata: {name: ${POOL_NAME}.${WORKER_3}}
spec:
  nodeName: $WORKER_3
  poolName: $POOL_NAME
  providerKind: FILE_THIN
  props:
    StorDriver/FileDir: /var/lib/blockstor-pool
EOF
    fi

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
    -d "[{\"name\":\"$RD\",\"node_name\":\"$WORKER_1\",\"props\":{\"StorPoolName\":\"$POOL_NAME\"}}]" >/dev/null
curl -fsS -XPOST -H'Content-Type: application/json' \
    "http://localhost:$PF_PORT/v1/resource-definitions/$RD/resources" \
    -d "[{\"name\":\"$RD\",\"node_name\":\"$WORKER_3\",\"props\":{\"StorPoolName\":\"$POOL_NAME\"}}]" >/dev/null

wait_uptodate "$RD" "$WORKER_1" "$WORKER_3"

echo ">> taint $WORKER_3 to keep the DaemonSet from re-spawning the satellite"
kubectl taint nodes "$WORKER_3" "${TAINT_KEY}=true:NoSchedule" --overwrite

echo ">> simulate permanent failure: evict satellite Pod on $WORKER_3"
sat_pod=$(kubectl -n "$NS" get pods -l app=blockstor-satellite \
    -o "jsonpath={.items[?(@.spec.nodeName==\"${WORKER_3}\")].metadata.name}")
if [[ -n "$sat_pod" ]]; then
    kubectl -n "$NS" delete pod "$sat_pod" --force --grace-period=0 --wait=false
fi
sleep 5

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
if kubectl get "nodes.blockstor.io.blockstor.io/${WORKER_3}" >/dev/null 2>&1; then
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
# Untaint the kube-node so the satellite DS can rebind — without
# this the autoplace's selection is fine but the kernel-side
# resource-create on $WORKER_3 has no satellite to apply onto.
kubectl taint nodes "$WORKER_3" "${TAINT_KEY}:NoSchedule-" 2>/dev/null || true

echo ">> STEP 4: POST /v1/resource-definitions/$RD/autoplace (refill to 2 diskful)"
http4=$(curl -sS -o /tmp/replace-hw-4.out -w '%{http_code}' \
    -XPOST -H'Content-Type: application/json' \
    "http://localhost:$PF_PORT/v1/resource-definitions/$RD/autoplace" \
    -d "{\"select_filter\":{\"place_count\":2,\"storage_pool\":\"$POOL_NAME\"}}")
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
