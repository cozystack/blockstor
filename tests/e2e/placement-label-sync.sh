#!/usr/bin/env bash
#
# usage: placement-label-sync.sh WORK_DIR
#
# Scenario 2.13 — k8s node labels → blockstor Node `Aux/<label-key>`
# props sync, then RG-level `--replicas-on-same Aux/<key>` actually
# constrains placement to nodes that share the same label value.
#
# Why this matters (operator-perceived):
#   Without the NodeLabelSyncReconciler, a StorageClass that requests
#   `replicasOnSame: "topology.kubernetes.io/zone=z1"` matches no
#   blockstor Node and the autoplacer silently picks topology-blind
#   replicas. Operator reports "the StorageClass setting doesn't work".
#
# Test plan:
#   1. Label live k8s nodes via `kubectl label`:
#        worker-1 + worker-2 → topology.kubernetes.io/zone=zone-a
#        worker-3            → topology.kubernetes.io/zone=zone-b
#   2. Wait up to 30s for the NodeLabelSyncReconciler to mirror the
#      labels into `Node.Spec.Props["Aux/topology.kubernetes.io/zone"]`
#      on the matching blockstor Node CRD. Record the elapsed seconds.
#   3. Cross-check on the LINSTOR-CLI surface:
#        `linstor node list-properties worker-1` shows the prop.
#   4. Create an RG `rg-zoned` with
#        --replicas-on-same Aux/topology.kubernetes.io/zone
#        --place-count 2
#      (Only `zone-a` has ≥2 nodes; the placer must pick the zone-a
#      pair worker-1 + worker-2 and never land a replica on worker-3.)
#   5. Spawn an RD from the RG: `linstor rg spawn rg-zoned myresource 64M`.
#      Within the autoplace budget, both replicas must land on the
#      worker-1 + worker-2 pair. worker-3 carrying a diskful replica
#      is a HARD FAIL (placer ignored the zone tuple).
#   6. Cleanup: delete RD + RG, strip the test labels.
#
# Pre-flight: require 3 satellite workers; require `linstor` CLI on
# the stand host (same as linstor-cli.sh).

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 3

if ! command -v linstor >/dev/null 2>&1; then
    echo "SKIP: linstor CLI not in PATH (apt install linstor-client)"
    exit 0
fi

ZONE_KEY="topology.kubernetes.io/zone"
AUX_KEY="Aux/${ZONE_KEY}"
ZONE_A="zone-a"
ZONE_B="zone-b"

# Sync budget. The reconciler watches corev1.Node with a
# LabelChangedPredicate; the satellite-side Aux prop should land via
# SSA within a couple of seconds on a quiet cluster. 30s is the
# scenario-2.13 contract — anything longer is a bug or a queue stall.
SYNC_TIMEOUT=30

# Autoplace budget. Placer creates 2 Resource CRDs + waits for
# satellites to bring up DRBD. 90s mirrors node-restore.sh's
# replacement-spawn budget — the slow path is satellite bring-up,
# not the placer decision itself.
PLACE_TIMEOUT=90

RG=rg-zoned
RD=myresource
SIZE_BYTES=$((64 * 1024 * 1024))  # 64 MiB

PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n "$NS" port-forward svc/blockstor-controller "$PF_PORT":3370 \
    >/tmp/placement-label-sync-pf.log 2>&1 &
PF_PID=$!

# Track which workers we labelled so cleanup hits the right nodes
# even if we exit between label + assert.
LABELED_NODES=("$WORKER_1" "$WORKER_2" "$WORKER_3")

cleanup() {
    local rc=$?

    # Best-effort: delete spawned RD and the RG so the next iter run
    # starts clean. delete_rd handles the satellite-side teardown
    # (drbdsetup down + .res / marker cleanup).
    delete_rd "$RD" 2>/dev/null || true
    "${LCTL[@]}" resource-group delete "$RG" 2>/dev/null || true

    # Strip the topology zone label we added — never leave test-only
    # labels on a shared stand. The trailing "-" is kubectl's syntax
    # for "remove this label" (NOT compatible with --overwrite, which
    # the previous version mistakenly passed and which made kubectl
    # silently no-op on the remove). Each node is stripped
    # independently so a transient apiserver error on one doesn't
    # leak the label on the others.
    for n in "${LABELED_NODES[@]}"; do
        kubectl label node "$n" "${ZONE_KEY}-" >/dev/null 2>&1 || true
    done

    if (( rc != 0 )); then
        echo "---- dump: kubectl get node.blockstor.io.blockstor.io -o yaml | grep -E 'name:|Aux/topology' ----"
        kubectl get nodes.blockstor.io.blockstor.io -o yaml 2>/dev/null \
            | grep -E '^[[:space:]]*(name:|Aux/topology)' | head -40 || true
        echo "---- dump: linstor resource list -r $RD ----"
        "${LCTL[@]}" resource list -r "$RD" 2>/dev/null || true
    fi

    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for the port-forward to actually answer (same idiom as
# observability-linstor-node-bridge.sh — a flat sleep races under
# parallel-iter load).
for _ in $(seq 1 30); do
    if curl -fsS -m 1 "http://127.0.0.1:${PF_PORT}/v1/healthz" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL=(linstor --controllers "http://localhost:$PF_PORT")
LCTLJ=(linstor --controllers "http://localhost:$PF_PORT" --machine-readable)

# --- Sanity: starting state must be clean (no leftover zone label) ---------

echo ">> sanity: strip any pre-existing $ZONE_KEY label before the test starts"
for n in "${LABELED_NODES[@]}"; do
    kubectl label node "$n" "${ZONE_KEY}-" >/dev/null 2>&1 || true
done

# --- Step 1: label nodes ----------------------------------------------------

echo ">> label $WORKER_1 and $WORKER_2 with ${ZONE_KEY}=${ZONE_A}"
kubectl label node "$WORKER_1" "${ZONE_KEY}=${ZONE_A}" --overwrite >/dev/null
kubectl label node "$WORKER_2" "${ZONE_KEY}=${ZONE_A}" --overwrite >/dev/null

echo ">> label $WORKER_3 with ${ZONE_KEY}=${ZONE_B}"
kubectl label node "$WORKER_3" "${ZONE_KEY}=${ZONE_B}" --overwrite >/dev/null

LABEL_TS_EPOCH=$(date +%s)
echo "   labels applied at epoch $LABEL_TS_EPOCH"

# --- Step 2: wait for NodeLabelSyncReconciler to mirror into Aux props -----

echo ">> wait up to ${SYNC_TIMEOUT}s for ${AUX_KEY} to land on every blockstor Node CRD"
deadline=$(( LABEL_TS_EPOCH + SYNC_TIMEOUT ))
sync_ok=0
SYNC_ELAPSED=""
last_seen=""

# Expected (k8s-node-name -> expected aux value) tuples.
declare -A WANT
WANT["$WORKER_1"]="$ZONE_A"
WANT["$WORKER_2"]="$ZONE_A"
WANT["$WORKER_3"]="$ZONE_B"

while (( $(date +%s) < deadline )); do
    all_match=1
    last_seen=""
    for n in "${LABELED_NODES[@]}"; do
        got=$(kubectl get "nodes.blockstor.io.blockstor.io/${n}" \
            -o jsonpath="{.spec.props.${AUX_KEY//./\\.}}" 2>/dev/null || true)
        last_seen+="$n=${got:-<none>} "
        if [[ "$got" != "${WANT[$n]}" ]]; then
            all_match=0
        fi
    done
    if (( all_match == 1 )); then
        SYNC_ELAPSED=$(( $(date +%s) - LABEL_TS_EPOCH ))
        sync_ok=1
        break
    fi
    sleep 1
done

if (( sync_ok != 1 )); then
    echo "FAIL: ${AUX_KEY} did not propagate to all blockstor Node CRDs within ${SYNC_TIMEOUT}s"
    echo "  last_seen: $last_seen"
    exit 1
fi

echo "   NodeLabelSyncReconciler propagated ${AUX_KEY} in ${SYNC_ELAPSED}s"
echo "$SYNC_ELAPSED" > "$WORK_DIR/.2.13-sync-seconds"

# --- Step 3: cross-check via `linstor node list-properties` ----------------

# `node list-properties` is what the operator actually runs. The
# machine-readable variant lets us assert without regex-parsing the
# table. Shape: [[{"key":"Aux/...","value":"zone-a"}, ...]]
echo ">> linstor node list-properties $WORKER_1 must show ${AUX_KEY}=${ZONE_A}"
props_json=$("${LCTLJ[@]}" node list-properties "$WORKER_1" 2>/dev/null || true)
seen=$(echo "$props_json" | jq -r --arg k "$AUX_KEY" \
    '.[]?[]? | select(.key == $k) | .value' 2>/dev/null | head -1)
if [[ "$seen" != "$ZONE_A" ]]; then
    echo "FAIL: linstor CLI surface missing $AUX_KEY=$ZONE_A on $WORKER_1 (got '${seen:-<empty>}')"
    echo "  raw props_json:"
    echo "$props_json"
    exit 1
fi

# --- Step 4: create RG with --replicas-on-same on the synced Aux key -------

echo ">> create RG $RG with --replicas-on-same $AUX_KEY --place-count 2"
"${LCTL[@]}" resource-group create "$RG" \
    --place-count 2 \
    --storage-pool stand \
    --replicas-on-same "$AUX_KEY" >/dev/null

# --- Step 5: spawn RD from the RG, assert both replicas in zone-a ----------

echo ">> linstor rg spawn $RG $RD ${SIZE_BYTES}B"
"${LCTL[@]}" resource-group spawn-resources "$RG" "$RD" "${SIZE_BYTES}B" >/dev/null

echo ">> wait up to ${PLACE_TIMEOUT}s for 2 diskful replicas to land"
deadline=$(( $(date +%s) + PLACE_TIMEOUT ))
placement_ok=0
PLACED_ON=""
while (( $(date +%s) < deadline )); do
    # Diskful replicas (exclude DISKLESS / TIE_BREAKER) for this RD.
    PLACED_ON=$("${LCTLJ[@]}" resource list -r "$RD" 2>/dev/null \
        | jq -r '.[]?[]? | select((.flags // []) | index("DISKLESS") | not) | .node_name' \
        | sort -u || true)
    diskful_count=$(echo "$PLACED_ON" | grep -cv '^$' || true)
    if (( diskful_count >= 2 )); then
        placement_ok=1
        break
    fi
    sleep 2
done

if (( placement_ok != 1 )); then
    echo "FAIL: only $diskful_count diskful replicas after ${PLACE_TIMEOUT}s"
    "${LCTLJ[@]}" resource list -r "$RD" || true
    exit 1
fi

echo "   diskful replicas landed on:"
echo "$PLACED_ON" | sed 's/^/     /'

# --- Step 6: enforce the zone-tuple invariant ------------------------------

# Any diskful replica on $WORKER_3 means the placer ignored
# replicas-on-same — that's the bug 2.13 is meant to catch.
if echo "$PLACED_ON" | grep -qx "$WORKER_3"; then
    echo "FAIL: diskful replica on $WORKER_3 (zone-b) — placer ignored ${AUX_KEY} constraint"
    "${LCTLJ[@]}" resource list -r "$RD" || true
    exit 1
fi

# All landed replicas must share the same zone tuple. With place-count=2
# and only zone-a holding ≥2 candidate nodes, the only valid placement
# is the worker-1 + worker-2 pair.
unique_zones=$(echo "$PLACED_ON" | while read -r n; do
    [[ -z "$n" ]] && continue
    kubectl get "nodes.blockstor.io.blockstor.io/${n}" \
        -o jsonpath="{.spec.props.${AUX_KEY//./\\.}}" 2>/dev/null
    echo
done | sort -u | grep -cv '^$' || true)

if [[ "$unique_zones" != "1" ]]; then
    echo "FAIL: replicas span $unique_zones zone-tuples (replicas-on-same broken)"
    echo "$PLACED_ON" | while read -r n; do
        [[ -z "$n" ]] && continue
        zv=$(kubectl get "nodes.blockstor.io.blockstor.io/${n}" \
            -o jsonpath="{.spec.props.${AUX_KEY//./\\.}}" 2>/dev/null)
        echo "    $n -> $AUX_KEY=$zv"
    done
    exit 1
fi

# Stamp the observed pair so the batch driver report can quote it.
echo "$PLACED_ON" | tr '\n' ',' | sed 's/,$//' > "$WORK_DIR/.2.13-placed-on"

echo ">> PLACEMENT-LABEL-SYNC OK"
echo "   label-sync to Aux props: ${SYNC_ELAPSED}s (budget ${SYNC_TIMEOUT}s)"
echo "   replicas in single zone-tuple: $PLACED_ON (no replica on $WORKER_3)"
