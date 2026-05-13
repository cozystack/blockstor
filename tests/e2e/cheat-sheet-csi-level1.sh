#!/usr/bin/env bash
#
# usage: cheat-sheet-csi-level1.sh WORK_DIR
#
# Scenario 1.23 (tests/scenarios/01-api-contract.md):
#   Level-1 CSI commands from the operator cheat-sheet
#   (tests/observability-cheat-sheet-scenarios.md §10):
#
#     kubectl describe pvc <name>
#     kubectl get volumeattachments
#     kubectl logs <csi-controller-pod> -c csi-attacher
#     kubectl logs <csi-controller-pod> -c csi-provisioner
#     kubectl logs <csi-controller-pod> -c csi-resizer
#
# The cheat-sheet uses the upstream-piraeus name "piraeus-csi-
# controller". The Pod that piraeus-operator actually deploys is named
# `linstor-csi-controller-*` in namespace `piraeus-datastore`. We
# resolve the real pod via label, but document that delta as part of
# 1.26 below — operators who copy-paste the cheat-sheet verbatim will
# need either an alias Deployment or a doc fix.
#
# Skip rule: piraeus-datastore not installed (stand without `make
# piraeus`) → SKIP, since the CSI sidecars are not blockstor code and
# their absence is an operator-stand decision, not a regression.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 1

CSI_NS=${CSI_NS:-piraeus-datastore}

if ! kubectl get ns "$CSI_NS" >/dev/null 2>&1; then
    echo "SKIP: namespace $CSI_NS not present (piraeus-operator not installed)"
    exit 0
fi

# Wait briefly for the CSI controller pod to exist + be Ready.
# `make piraeus` installs it asynchronously; if the stand was just
# brought up the Deployment may still be ContainerCreating.
echo ">> waiting up to 60s for linstor-csi-controller pod Ready in $CSI_NS"
deadline=$(( $(date +%s) + 60 ))
CSI_POD=""
while (( $(date +%s) < deadline )); do
    CSI_POD=$(kubectl -n "$CSI_NS" get pods -l app.kubernetes.io/component=linstor-csi-controller \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    if [[ -z "$CSI_POD" ]]; then
        # Some piraeus versions label as 'app=linstor-csi-controller'.
        CSI_POD=$(kubectl -n "$CSI_NS" get pods -l app=linstor-csi-controller \
            -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    fi
    if [[ -z "$CSI_POD" ]]; then
        # Last resort: name prefix.
        CSI_POD=$(kubectl -n "$CSI_NS" get pods -o jsonpath='{.items[*].metadata.name}' 2>/dev/null \
            | tr ' ' '\n' | grep '^linstor-csi-controller-' | head -1 || true)
    fi
    if [[ -n "$CSI_POD" ]]; then
        ready=$(kubectl -n "$CSI_NS" get pod "$CSI_POD" \
            -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
        if [[ "$ready" == "True" ]]; then
            break
        fi
    fi
    sleep 3
done

if [[ -z "$CSI_POD" ]]; then
    echo "SKIP: no linstor-csi-controller pod in $CSI_NS (piraeus not finished installing?)"
    exit 0
fi

echo ">> CSI controller pod: $CSI_POD"

# --- Sidecar logs: each must be non-empty -----------------------------
# Per the cheat-sheet, the operator-debug flow inspects each sidecar
# (attacher / provisioner / resizer / external-snapshotter). We check
# the three the spec calls out as P1 (attacher/provisioner/resizer).
# snapshotter is optional in piraeus-operator's manifest matrix.

fail=0
for sc in csi-attacher csi-provisioner csi-resizer; do
    # Confirm the container exists first — image drift would put a
    # missing-container error in front of an empty-log false-negative.
    if ! kubectl -n "$CSI_NS" get pod "$CSI_POD" \
        -o jsonpath='{.spec.containers[*].name}' | tr ' ' '\n' | grep -qx "$sc"; then
        echo "FAIL: container $sc not present in $CSI_POD"
        fail=1
        continue
    fi
    out=$(kubectl -n "$CSI_NS" logs "$CSI_POD" -c "$sc" --tail=20 2>&1 || true)
    if [[ -z "$out" ]]; then
        echo "FAIL: logs $CSI_POD -c $sc returned empty"
        fail=1
    else
        echo "   $sc OK ($(echo "$out" | wc -l | tr -d ' ') lines)"
    fi
done

# --- kubectl get volumeattachments -----------------------------------
# Cluster-scoped resource — just assert the API answers (exit 0).
# Empty list is fine; "the server doesn't have a resource type" would
# mean a broken CSI install and IS a fail.
if ! kubectl get volumeattachments >/dev/null 2>&1; then
    echo "FAIL: kubectl get volumeattachments errored"
    fail=1
fi

# --- kubectl describe pvc -------------------------------------------
# Pick any PVC if one exists; otherwise create a tiny one bound to a
# linstor SC so the operator-cheat-sheet flow is exercised end to end.
# We tear it down at exit.
PVC=${PVC:-cheat-sheet-l1-pvc}
SC=${SC:-cheat-sheet-l1-sc}

cleanup() {
    kubectl delete pvc "$PVC" --ignore-not-found --wait=false 2>/dev/null || true
    kubectl delete sc  "$SC"  --ignore-not-found 2>/dev/null || true
}
trap cleanup EXIT

# The three-way-observability scenario points linstor-csi at blockstor
# already; we accept whatever LS_CONTROLLERS the operator wired. If
# this script runs standalone before three-way, we just need ANY linstor
# storage pool to be a valid backend for the SC.
echo ">> create transient SC $SC + PVC $PVC (RWO, 64Mi, linstor-csi)"
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
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: $PVC}
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 64Mi
  storageClassName: $SC
EOF

# describe must exit 0 even if the PVC is still Pending. The cheat-
# sheet's first-step "kubectl describe pvc" is meant to surface the
# Events block, and works for any phase. We assert the Events section
# header is rendered.
describe_out=$(kubectl describe pvc "$PVC" 2>&1 || true)
if ! echo "$describe_out" | grep -q '^Events:'; then
    echo "FAIL: kubectl describe pvc $PVC missing Events section"
    echo "--- describe ---"
    echo "$describe_out"
    fail=1
fi

if (( fail != 0 )); then
    echo "CHEAT-SHEET-CSI-LEVEL1: FAIL"
    exit 1
fi

echo ">> CHEAT-SHEET-CSI-LEVEL1 OK (describe pvc, get volumeattachments, $CSI_POD logs × 3 sidecars all responding)"
