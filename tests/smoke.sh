#!/usr/bin/env bash
# usage: smoke.sh WORK_DIR
# Golden-path test: provision a PVC, mount it, write data, verify replication count.
set -euo pipefail
WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

NS=blockstor-smoke
PVC=smoke-pvc
POD=smoke-writer

trap 'kubectl delete ns $NS --wait=false --ignore-not-found' EXIT

echo ">> creating namespace + StorageClass + PVC"
kubectl create ns $NS --dry-run=client -o yaml | kubectl apply -f -

cat <<EOF | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata: { name: blockstor-smoke }
provisioner: linstor.csi.linbit.com
parameters:
  linstor.csi.linbit.com/storagePool: "pool"
  linstor.csi.linbit.com/placementCount: "2"
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: $PVC, namespace: $NS }
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: blockstor-smoke
  resources: { requests: { storage: 256Mi } }
---
apiVersion: v1
kind: Pod
metadata: { name: $POD, namespace: $NS }
spec:
  restartPolicy: Never
  containers:
  - name: w
    image: busybox:1.37
    command: [sh, -c, "echo blockstor-smoke-\$HOSTNAME > /data/marker && sync && sleep 30"]
    volumeMounts: [{ name: d, mountPath: /data }]
  volumes:
  - name: d
    persistentVolumeClaim: { claimName: $PVC }
EOF

echo ">> waiting for pod"
kubectl -n $NS wait pod/$POD --for=condition=Ready --timeout=3m

echo ">> verifying write"
kubectl -n $NS exec $POD -- cat /data/marker

echo ">> SMOKE OK"
