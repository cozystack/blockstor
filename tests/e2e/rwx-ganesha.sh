#!/usr/bin/env bash
#
# usage: rwx-ganesha.sh WORK_DIR
#
# RWX validation through linstor-csi: claim a PVC with
# accessModes=ReadWriteMany against a piraeus-backed StorageClass,
# mount it from two Pods on different worker nodes, and verify a
# marker written on one is readable on the other.
#
# linstor-csi auto-publishes an RWX PVC over its NFS-Ganesha sidecar
# (the `linstor-csi-nfs-server` DaemonSet piraeus-operator ships) —
# the dev stand has those Pods Running after `make piraeus`, so this
# test only needs to drive the upper-layer Pod / PVC plumbing. No
# drbd-reactor or hand-rolled Ganesha config required.
#
# This is technically a piraeus-stack smoke (the underlying volume
# is provisioned by Java LINSTOR), kept in tests/e2e/ because it
# exercises the same RWX surface blockstor will expose through its
# LINSTOR-compatible REST API in a later phase.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

# RWX-Ganesha publishes via piraeus' linstor-csi NFS-Ganesha path.
# That requires the full piraeus HA stack healthy:
#   linstor-affinity-controller, linstor-csi-controller,
#   linstor-csi-node, ha-controller, operator (and ganesha-server
#   exports). Skip if any required component isn't Running on the
#   stand — the test would just time out on pod-Ready otherwise.
required=(linstor-affinity-controller linstor-csi-controller linstor-csi-node ha-controller)
missing=""
for c in "${required[@]}"; do
    if ! kubectl get pods -A --no-headers 2>/dev/null | grep -E "${c}.*Running" >/dev/null; then
        missing="$missing $c"
    fi
done
if [[ -n "$missing" ]]; then
    echo "SKIP: piraeus components not Running:$missing"
    exit 0
fi

SC=e2e-rwx-sc
PVC=e2e-rwx
P1=e2e-rwx-pod-1
P2=e2e-rwx-pod-2

cleanup() {
    kubectl delete pod "$P1" "$P2" --ignore-not-found --wait=false 2>/dev/null || true
    kubectl delete pvc "$PVC" --ignore-not-found 2>/dev/null || true
    kubectl delete sc "$SC" --ignore-not-found 2>/dev/null || true
}
trap cleanup EXIT

echo ">> StorageClass against piraeus' linstor-csi (pool=pool, replicas=2)"
cat <<EOF | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: {name: $SC}
provisioner: linstor.csi.linbit.com
parameters:
  linstor.csi.linbit.com/storagePool: pool
  linstor.csi.linbit.com/placementCount: "2"
  csi.storage.k8s.io/fstype: ext4
allowVolumeExpansion: true
volumeBindingMode: Immediate
reclaimPolicy: Delete
EOF

echo ">> PVC 128Mi accessModes=ReadWriteMany"
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: $PVC}
spec:
  accessModes: [ReadWriteMany]
  storageClassName: $SC
  resources:
    requests: {storage: 128Mi}
EOF

echo ">> wait for PVC Bound (90s)"
deadline=$(( $(date +%s) + 90 ))
phase=""
while (( $(date +%s) < deadline )); do
    phase=$(kubectl get pvc "$PVC" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    [[ "$phase" == "Bound" ]] && break
    sleep 3
done

if [[ "$phase" != "Bound" ]]; then
    echo "FAIL: PVC never Bound (phase=$phase)"
    kubectl describe pvc "$PVC" | tail -20
    exit 1
fi

echo ">> two Pods on $WORKER_1 + $WORKER_2 mount the PVC"
# PodSecurity: the test namespace runs with PSA `restricted:latest`
# enforcement. Run 28 deep-dive showed the 600s timeout was not NFS
# slowness — it was PSA blocking pod admission outright (no
# securityContext → violated runAsNonRoot / allowPrivilegeEscalation /
# capabilities / seccompProfile). Set the full restricted-baseline
# securityContext at both Pod and Container scope and the pods admit
# immediately; 300s is plenty of headroom for NFS-Ganesha publish.
for spec in "$P1:$WORKER_1" "$P2:$WORKER_2"; do
    name=${spec%:*}
    node=${spec#*:}
    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata: {name: $name}
spec:
  nodeName: $node
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 65532
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: w
      image: alpine:3
      command: ["sleep", "300"]
      securityContext:
        allowPrivilegeEscalation: false
        capabilities:
          drop: [ALL]
      volumeMounts:
        - {name: data, mountPath: /data}
  volumes:
    - name: data
      persistentVolumeClaim: {claimName: $PVC}
EOF
done

echo ">> wait both Pods Ready (300s)"
kubectl wait --for=condition=Ready --timeout=300s pod/"$P1" pod/"$P2"

MARK="rwx-$(date +%s)-$$"
echo ">> write marker '$MARK' from $P1"
kubectl exec "$P1" -- sh -c "echo $MARK > /data/marker && sync"

echo ">> read marker from $P2"
got=$(kubectl exec "$P2" -- cat /data/marker)
if [[ "$got" != "$MARK" ]]; then
    echo "FAIL: marker mismatch — got '$got', want '$MARK'"
    exit 1
fi

echo ">> RWX-GANESHA OK (marker round-tripped between $P1 on $WORKER_1 and $P2 on $WORKER_2)"
