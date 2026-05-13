#!/usr/bin/env bash
#
# usage: observability-three-way.sh WORK_DIR
#
# Scenario 7.9 (tests/scenarios/07-quorum-observability.md):
#   PVC ↔ Resource CRD ↔ DRBD device three-way match. Bind a PVC,
#   then assert the three observability layers agree on:
#     - resource name (PV.spec.csi.volumeHandle == LINSTOR RD name ==
#       `.res` `resource <name>` block)
#     - DRBD port (LINSTOR `r l` Port column == `.res` `address` port)
#     - DRBD minor (LINSTOR `v l` MinorNr column == `.res`
#       `volume 0 { ... minor M; }` == /dev/drbdN)
#
# Failure modes guarded:
#   - PVC Bound but Resource CRD not stamped → bound-too-early bug
#   - LINSTOR shows UpToDate but `.res` missing → satellite reconciler
#     didn't render
#   - DRBD minor in `.res` ≠ kernel → minor allocator desync (Phase 8.1)

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

if ! command -v linstor >/dev/null 2>&1; then
    echo "SKIP: linstor CLI not in PATH (apt install linstor-client)"
    exit 0
fi
if ! command -v jq >/dev/null 2>&1; then
    echo "SKIP: jq not in PATH"
    exit 0
fi

PVC=three-way-test
SC=e2e-three-way-sc

# port-forward blockstor-controller:3370 → random local port for `linstor` CLI.
PF_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
kubectl -n blockstor-system port-forward svc/blockstor-controller "$PF_PORT":3370 \
    >/tmp/three-way-pf.log 2>&1 &
PF_PID=$!

cleanup() {
    kubectl delete pvc "$PVC" --ignore-not-found --wait=false 2>/dev/null || true
    kubectl delete sc "$SC" --ignore-not-found 2>/dev/null || true
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for port-forward.
for _ in $(seq 1 30); do
    if curl -sf -m1 "http://localhost:$PF_PORT/v1/nodes" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

LCTL_M=(linstor --controllers "http://localhost:$PF_PORT" --machine-readable)

# Point piraeus-operator's bundled linstor-csi at blockstor's apiserver
# so the StorageClass we create below resolves `storagePool: stand`
# against blockstor's pool registry (and not piraeus's, whose pool
# name is `pool`). The official piraeus knob is
# LinstorCluster.spec.externalController.url; setting it makes the
# operator (a) skip its in-cluster linstor-controller Deployment and
# (b) re-render the linstor-csi controller+node manifests with
# LS_CONTROLLERS pointing at the URL we provide. This is the correct
# fix per the Phase 7.9 scenario design: the test exercises
# blockstor's three-way observability invariant (PVC ↔ Resource CRD
# ↔ .res), so the CSI provisioning request MUST land on blockstor.
BLOCKSTOR_URL="http://blockstor-apiserver.blockstor-system.svc:3370"
CUR_URL=$(kubectl get linstorcluster linstorcluster \
    -o jsonpath='{.spec.externalController.url}' 2>/dev/null || true)
if [[ "$CUR_URL" != "$BLOCKSTOR_URL" ]]; then
    echo ">> wire linstor-csi at $BLOCKSTOR_URL via LinstorCluster.spec.externalController"
    kubectl patch linstorcluster linstorcluster --type merge \
        -p "{\"spec\":{\"externalController\":{\"url\":\"$BLOCKSTOR_URL\"}}}"

    echo ">> wait up to 180s for linstor-csi-controller to roll with new LS_CONTROLLERS"
    deadline=$(( $(date +%s) + 180 ))
    while (( $(date +%s) < deadline )); do
        env_val=$(kubectl -n piraeus-datastore get deploy linstor-csi-controller \
            -o jsonpath='{.spec.template.spec.containers[?(@.name=="linstor-csi")].env[?(@.name=="LS_CONTROLLERS")].value}' \
            2>/dev/null || true)
        if [[ "$env_val" == "$BLOCKSTOR_URL" ]]; then
            break
        fi
        sleep 3
    done
    if [[ "$env_val" != "$BLOCKSTOR_URL" ]]; then
        echo "FAIL: linstor-csi-controller LS_CONTROLLERS never reconciled to $BLOCKSTOR_URL (got '$env_val')"
        kubectl -n piraeus-datastore get deploy linstor-csi-controller -o yaml | grep -A2 LS_CONTROLLERS || true
        exit 1
    fi
    kubectl -n piraeus-datastore rollout status deploy/linstor-csi-controller --timeout=120s
    kubectl -n piraeus-datastore rollout status ds/linstor-csi-node --timeout=120s
else
    echo ">> linstor-csi already wired at $BLOCKSTOR_URL"
fi

# Create a dedicated StorageClass against linstor-csi so the test
# isn't sensitive to whatever default SC the stand was provisioned
# with. With the externalController.url patch above, linstor-csi now
# resolves `storagePool: stand` against blockstor (whose stand pool
# is FILE_THIN on every worker).
echo ">> create StorageClass $SC (linstor-csi, pool=stand, 2 replicas)"
cat <<EOF | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: {name: $SC}
provisioner: linstor.csi.linbit.com
parameters:
  linstor.csi.linbit.com/storagePool: stand
  linstor.csi.linbit.com/placementCount: "2"
  csi.storage.k8s.io/fstype: ext4
allowVolumeExpansion: true
volumeBindingMode: Immediate
reclaimPolicy: Delete
EOF

echo ">> apply PVC $PVC (100Mi, RWO)"
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: $PVC}
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 100Mi
  storageClassName: $SC
EOF

echo ">> wait up to 120s for PVC Bound"
deadline=$(( $(date +%s) + 120 ))
phase=""
while (( $(date +%s) < deadline )); do
    phase=$(kubectl get pvc "$PVC" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    [[ "$phase" == "Bound" ]] && break
    sleep 3
done
if [[ "$phase" != "Bound" ]]; then
    echo "FAIL: PVC $PVC stuck in phase=$phase"
    kubectl describe pvc "$PVC" || true
    exit 1
fi

PV_NAME=$(kubectl get pvc "$PVC" -o jsonpath='{.spec.volumeName}')
VOL_HANDLE=$(kubectl get pv "$PV_NAME" -o jsonpath='{.spec.csi.volumeHandle}')
echo "   PV=$PV_NAME volumeHandle=$VOL_HANDLE"

# In linstor-csi the RD name == volumeHandle (PV-prefixed) by default.
RD="$VOL_HANDLE"

# Make sure LINSTOR sees the RD AND its replicas have a DRBD port
# assigned before we extract metadata. A fresh-bound PVC can race the
# satellite's first-resource-render: the RD CRD exists but the
# tcp_ports list is still null until the satellite stamps the port.
echo ">> wait up to 120s for LINSTOR RD $RD + tcp_ports populated"
deadline=$(( $(date +%s) + 120 ))
seen=""
while (( $(date +%s) < deadline )); do
    rd_seen=$("${LCTL_M[@]}" resource-definition list -r "$RD" 2>/dev/null \
        | jq -r --arg n "$RD" '[.. | objects | select(.name? == $n)] | length')
    port_seen=$("${LCTL_M[@]}" resource list -r "$RD" 2>/dev/null \
        | jq -r '[.[0][]? | .layer_object.drbd.tcp_ports[]? | numbers] | length')
    if [[ "${rd_seen:-0}" -gt 0 && "${port_seen:-0}" -gt 0 ]]; then
        seen=1
        break
    fi
    sleep 2
done
if [[ -z "$seen" ]]; then
    echo "FAIL: RD $RD never showed up with tcp_ports via linstor"
    "${LCTL_M[@]}" resource-definition list -r "$RD" || true
    "${LCTL_M[@]}" resource list -r "$RD" || true
    exit 1
fi

# ---- LINSTOR view ----
# `linstor --machine-readable r l -r <rd>` returns [[ {res-entry}, ... ]].
# Each res-entry carries:
#   .node_name                                — replica node
#   .flags[]                                  — DISKLESS / TIE_BREAKER (absent on diskful)
#   .layer_object.drbd.tcp_ports[0]           — DRBD TCP port (this is the
#                                                blockstor-exposed shape;
#                                                upstream linstor exposes
#                                                .layer_object.drbd.port and
#                                                rsc_dfn_port — we accept either)
#   .layer_object.drbd.drbd_volumes[].device_path — /dev/drbdN, minor encoded as N
#   .volumes[].device_path                    — same /dev/drbdN, redundant view
#
# Per Phase 8.1 the port + minor are RD-scoped, so picking the first
# diskful entry is sufficient; we still cross-check every replica's
# .res file in the loop below.
RES_JSON=$("${LCTL_M[@]}" resource list -r "$RD")
VOL_JSON=$("${LCTL_M[@]}" volume list -r "$RD")

# Diskful replicas only — TIE_BREAKER / DISKLESS entries have no backing
# storage and no .res-rendered minor on the satellite. Use --arg DK so jq
# parses the array filter correctly (previous `has("flags") | not or ...`
# parsed as `(has(...) | not) or ...` which on flag-less objects evaluated
# to boolean and then crashed on `.flags // []`).
DISKFUL_FILTER='[ .[0][] | select((.flags // []) | index("DISKLESS") | not) ]'
DISKFUL_JSON=$(echo "$RES_JSON" | jq "$DISKFUL_FILTER")

mapfile -t NODES_LINSTOR < <(echo "$DISKFUL_JSON" \
    | jq -r '.[].node_name' | sort -u)

if (( ${#NODES_LINSTOR[@]} == 0 )); then
    echo "FAIL: no diskful replicas in linstor view (got only DISKLESS/TIE_BREAKER)"
    echo "$RES_JSON"
    exit 1
fi

# Port — try blockstor's tcp_ports[0] first, fall back to upstream shapes.
PORT_LINSTOR=$(echo "$DISKFUL_JSON" | jq -r '
    [ .[] | .layer_object.drbd.tcp_ports[]? ] | first // empty
')
if [[ -z "$PORT_LINSTOR" || "$PORT_LINSTOR" == "null" ]]; then
    PORT_LINSTOR=$(echo "$DISKFUL_JSON" | jq -r '
        [ .[] | .. | objects | (.port // .tcp_port // empty) | numbers ] | first // empty
    ')
fi
if [[ -z "$PORT_LINSTOR" || "$PORT_LINSTOR" == "null" ]]; then
    RD_JSON=$("${LCTL_M[@]}" resource-definition list -r "$RD")
    PORT_LINSTOR=$(echo "$RD_JSON" | jq -r '
        [.. | objects | (.rsc_dfn_port // .port // .tcp_port // empty) | numbers]
        | first // empty
    ')
fi

# Minor — blockstor doesn't surface a minor_nr field; derive it from the
# device_path /dev/drbdN exposed under layer_object.drbd.drbd_volumes[]
# or volumes[]. Accept upstream-linstor's .minor_nr / .minor when present.
MINOR_LINSTOR=$(echo "$VOL_JSON" | jq -r '
    [.. | objects | (.minor_nr // .minor // empty) | numbers] | first // empty
')
if [[ -z "$MINOR_LINSTOR" || "$MINOR_LINSTOR" == "null" ]]; then
    DEV_PATH=$(echo "$DISKFUL_JSON" | jq -r '
        [ .[] | (.. | objects | .device_path? // empty) | strings
          | select(test("^/dev/drbd[0-9]+$")) ] | first // empty
    ')
    if [[ -n "$DEV_PATH" ]]; then
        MINOR_LINSTOR=${DEV_PATH#/dev/drbd}
    fi
fi

echo "   LINSTOR view: name=$RD port=$PORT_LINSTOR minor=$MINOR_LINSTOR nodes=${NODES_LINSTOR[*]}"

if [[ -z "$PORT_LINSTOR" || -z "$MINOR_LINSTOR" ]]; then
    echo "FAIL: could not extract port/minor from LINSTOR JSON"
    echo "--- resource list ---"; echo "$RES_JSON"
    echo "--- volume list ---"; echo "$VOL_JSON"
    exit 1
fi

# ---- .res file view on every diskful satellite ----
fail=0
for node in "${NODES_LINSTOR[@]}"; do
    echo ">> [$node] inspect /etc/drbd.d/${RD}.res"
    RES_CONTENT=$(on_node "$node" bash -c "cat /etc/drbd.d/${RD}.res 2>/dev/null \
        || cat /var/lib/linstor.d/${RD}.res 2>/dev/null \
        || true")
    if [[ -z "$RES_CONTENT" ]]; then
        echo "FAIL: no .res file for $RD on $node"
        fail=1
        continue
    fi

    # resource <name> { ... } — must match.
    RES_NAME=$(echo "$RES_CONTENT" | grep -oE '^resource[[:space:]]+[A-Za-z0-9._-]+' \
        | head -1 | awk '{print $2}')
    if [[ "$RES_NAME" != "$RD" ]]; then
        echo "FAIL: [$node] .res resource block '$RES_NAME' != RD '$RD'"
        fail=1
    fi

    # address ... :PORT — must match LINSTOR port.
    RES_PORT=$(echo "$RES_CONTENT" \
        | grep -oE 'address[^;]*:[0-9]+' \
        | head -1 \
        | grep -oE '[0-9]+$')
    if [[ "$RES_PORT" != "$PORT_LINSTOR" ]]; then
        echo "FAIL: [$node] .res port '$RES_PORT' != LINSTOR port '$PORT_LINSTOR'"
        fail=1
    fi

    # volume 0 { device /dev/drbdN minor N; } — minor must match.
    RES_MINOR=$(echo "$RES_CONTENT" \
        | grep -oE 'minor[[:space:]]+[0-9]+' \
        | head -1 \
        | awk '{print $2}')
    if [[ "$RES_MINOR" != "$MINOR_LINSTOR" ]]; then
        echo "FAIL: [$node] .res minor '$RES_MINOR' != LINSTOR minor '$MINOR_LINSTOR'"
        fail=1
    fi

    # Cross-check kernel: /dev/drbd<MINOR> must exist on this node.
    if ! on_node "$node" bash -c "test -b /dev/drbd${MINOR_LINSTOR}"; then
        echo "FAIL: [$node] /dev/drbd${MINOR_LINSTOR} not present in kernel"
        fail=1
    fi

    echo "   [$node] OK: name=$RES_NAME port=$RES_PORT minor=$RES_MINOR /dev/drbd${MINOR_LINSTOR} present"
done

if (( fail != 0 )); then
    echo "--- diagnostic dump ---"
    echo "PV: $PV_NAME  volumeHandle: $VOL_HANDLE"
    echo "LINSTOR port=$PORT_LINSTOR minor=$MINOR_LINSTOR"
    echo "--- resource list ---"; echo "$RES_JSON" | jq -c . 2>/dev/null || echo "$RES_JSON"
    echo "--- volume list ---";   echo "$VOL_JSON" | jq -c . 2>/dev/null || echo "$VOL_JSON"
    exit 1
fi

echo ">> OBSERVABILITY-THREE-WAY OK (PVC ↔ LINSTOR ↔ .res agree on name=$RD port=$PORT_LINSTOR minor=$MINOR_LINSTOR)"
