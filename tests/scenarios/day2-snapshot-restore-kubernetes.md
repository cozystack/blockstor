# day2-snapshot-restore-kubernetes

## Scenario

Restore a CSI VolumeSnapshot into a new PVC (replacing or alongside the original) in Kubernetes.

## Steps

1. Scale down the deployment using the original PVC: `kubectl scale deploy/<name> --replicas=0` and `kubectl rollout status deploy/<name>`.
2. Delete the original PVC (the VolumeSnapshot remains): `kubectl delete pvc/data-volume`.
3. Recreate the PVC referencing the snapshot:
```
kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data-volume
spec:
  storageClassName: linbit-sds-storage
  resources:
    requests:
      storage: 1Gi
  dataSource:
    apiGroup: snapshot.storage.k8s.io
    kind: VolumeSnapshot
    name: data-volume-snapshot-1
  accessModes:
    - ReadWriteOnce
EOF
```
4. Scale the deployment back up: `kubectl scale deploy/<name> --replicas=1`.

## Expected outcome

- A new PVC is bound to a new LINSTOR-backed PV whose initial data matches the snapshot.
- The pod restarts and reads the restored data.

## Validations

- `kubectl get pvc data-volume` shows `STATUS=Bound`.
- `kubectl exec <pod> -- md5sum /mountpoint/file` matches the pre-snapshot value.

## Doc reference

linstor-kubernetes.adoc: `===== Restoring a snapshot` (lines 2338-2386).

## Notes

- The new PVC's size must be >= the snapshot size.
- The snapshot must already be `readyToUse=true`. If not, the PVC binds will fail with a "snapshot not ready" event.
