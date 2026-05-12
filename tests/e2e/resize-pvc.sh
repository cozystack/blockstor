#!/usr/bin/env bash
#
# usage: resize-pvc.sh WORK_DIR
#
# Phase 8.2 follow-up — PLAN.md: "Volume resize end-to-end with a
# real PVC: write checksum, grow via REST, verify checksum +
# filesystem sees the new size."
#
# Builds on resize-plain.sh (block-level write/grow/md5) by going
# through the k8s CSI surface: PVC + filesystem + Pod-level verification.
#
# Steps:
#   1. StorageClass against linstor-csi with allowVolumeExpansion +
#      ext4 fstype.
#   2. 128 MiB PVC; one Pod on WORKER_1 mounts it at /data.
#   3. Write a 32 MiB random file to /data/blob, compute md5,
#      compute pre-grow filesystem `df -k /data` Used.
#   4. Patch PVC spec.resources.requests.storage to 256 MiB.
#   5. Wait for both:
#      - PVC.status.capacity reports >= 256 MiB
#      - `df -k /data` reports an Available delta of >= the grow
#        amount (filesystem actually grew, not just the block device)
#   6. Verify md5 of /data/blob round-trips intact across the grow.
#
# Triggers piraeus-csi's `NodeExpandVolume` flow which calls into
# blockstor's `PUT /v1/resource-definitions/{rd}/volume-definitions/{vn}`
# size-bump + the satellite-side `lvextend|zfs set volsize` + `drbdadm
# resize` chain. Filesystem grow is `resize2fs` issued by piraeus-csi
# in the node-publish stage after the block device grew.

set -euo pipefail

WORK_DIR=${1:?work_dir required}
export KUBECONFIG="$WORK_DIR/kubeconfig"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

require_workers 2

SC=e2e-resize-pvc-sc
PVC=e2e-resize-pvc
POD=e2e-resize-pvc-pod
SIZE_INITIAL=128Mi
SIZE_GROWN=256Mi

cleanup() {
    kubectl delete pod "$POD" --ignore-not-found --wait=false 2>/dev/null || true
    kubectl delete pvc "$PVC" --ignore-not-found 2>/dev/null || true
    kubectl delete sc "$SC" --ignore-not-found 2>/dev/null || true
}
trap cleanup EXIT

echo ">> StorageClass (allowVolumeExpansion, fstype=ext4, placement=2)"
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

echo ">> PVC $SIZE_INITIAL"
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: $PVC}
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: $SC
  resources:
    requests: {storage: $SIZE_INITIAL}
EOF

echo ">> wait PVC Bound (90s)"
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

echo ">> Pod on $WORKER_1 mounts the PVC"
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata: {name: $POD}
spec:
  nodeName: $WORKER_1
  restartPolicy: Never
  containers:
    - name: w
      image: alpine:3
      command: ["sleep", "600"]
      volumeMounts:
        - {name: data, mountPath: /data}
  volumes:
    - name: data
      persistentVolumeClaim: {claimName: $PVC}
EOF

kubectl wait --for=condition=Ready --timeout=120s pod/"$POD"

echo ">> write 32 MiB random blob and capture md5 + filesystem size"
kubectl exec "$POD" -- sh -c "dd if=/dev/urandom of=/data/blob bs=1M count=32 status=none && sync"
md5_pre=$(kubectl exec "$POD" -- sh -c "md5sum /data/blob | cut -d' ' -f1")
fs_size_pre_kb=$(kubectl exec "$POD" -- sh -c "df -k /data | awk 'NR==2{print \$2}'")
echo "   pre-grow: md5=$md5_pre filesystem_size_kb=$fs_size_pre_kb"

echo ">> patch PVC.spec.resources.requests.storage → $SIZE_GROWN"
kubectl patch pvc "$PVC" --type=merge -p "{\"spec\":{\"resources\":{\"requests\":{\"storage\":\"$SIZE_GROWN\"}}}}"

# csi-resizer reconciles into PV.spec.capacity, then NodeExpand resizes
# the filesystem in-Pod. Both need to finish before df reflects the new
# size. PVC.status.capacity flips when both legs land. Allow 90 s.
echo ">> wait PVC.status.capacity == $SIZE_GROWN (90s)"
deadline=$(( $(date +%s) + 90 ))
got=""
while (( $(date +%s) < deadline )); do
    got=$(kubectl get pvc "$PVC" -o jsonpath='{.status.capacity.storage}' 2>/dev/null || true)
    [[ "$got" == "$SIZE_GROWN" ]] && break
    sleep 3
done

if [[ "$got" != "$SIZE_GROWN" ]]; then
    echo "FAIL: PVC.status.capacity never reached $SIZE_GROWN (got=$got)"
    kubectl describe pvc "$PVC" | tail -30
    exit 1
fi

echo ">> verify filesystem-level grow (df sees larger size)"
fs_size_post_kb=$(kubectl exec "$POD" -- sh -c "df -k /data | awk 'NR==2{print \$2}'")
echo "   post-grow: filesystem_size_kb=$fs_size_post_kb"

# Expect the filesystem to see at least 1.5× of the pre-grow size
# (we doubled the request 128→256 MiB; filesystem overhead trims a
# bit so a strict ×2 isn't reliable). 1.5× catches the "did not
# resize at all" case unambiguously.
threshold=$(( fs_size_pre_kb * 3 / 2 ))
if (( fs_size_post_kb < threshold )); then
    echo "FAIL: filesystem did not grow — post=$fs_size_post_kb pre=$fs_size_pre_kb threshold=$threshold"
    exit 1
fi

echo ">> verify md5 of blob survived the grow"
md5_post=$(kubectl exec "$POD" -- sh -c "md5sum /data/blob | cut -d' ' -f1")
if [[ "$md5_pre" != "$md5_post" ]]; then
    echo "FAIL: blob md5 changed across resize — pre=$md5_pre post=$md5_post"
    exit 1
fi

echo ">> RESIZE-PVC OK (PVC $SIZE_INITIAL → $SIZE_GROWN; fs grew $fs_size_pre_kb→$fs_size_post_kb KiB; blob md5 intact)"
