# day2-snapshot-create-kubernetes

## Scenario

Take a CSI snapshot of a LINSTOR-backed PVC in Kubernetes.

## Steps

1. Verify the CSI snapshot CRDs and controller are present:
```
kubectl api-resources --api-group=snapshot.storage.k8s.io -oname
kubectl get pods -A | grep snapshot-controller
```
2. Create a `VolumeSnapshotClass` referencing `linstor.csi.linbit.com`:
```
kubectl apply -f - <<EOF
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: linbit-sds-snapshots
driver: linstor.csi.linbit.com
deletionPolicy: Delete
EOF
```
3. Create a `VolumeSnapshot` referencing the PVC:
```
kubectl apply -f - <<EOF
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: data-volume-snapshot-1
spec:
  volumeSnapshotClassName: linbit-sds-snapshots
  source:
    persistentVolumeClaimName: data-volume
EOF
```
4. Wait: `kubectl wait volumesnapshot --for=jsonpath='{.status.readyToUse}'=true data-volume-snapshot-1`.

## Expected outcome

- A `VolumeSnapshot` resource is created and reaches `readyToUse=true`.
- A matching LINSTOR snapshot is visible from the controller.

## Validations

- `kubectl get volumesnapshot data-volume-snapshot-1` shows `READYTOUSE=true`.
- `kubectl -n linbit-sds exec deploy/linstor-controller -- linstor snapshot list` shows a snapshot with `State=Successful` for the PVC's underlying RD.

## Doc reference

linstor-kubernetes.adoc: `==== Working with snapshots` (lines 2214-2335) and `===== Creating a snapshot` (lines 2270-2302).

## Notes

- Backing storage pool MUST support snapshots (LVM_THIN, ZFS, ZFS_THIN, FILE_THIN with reflink).
- `deletionPolicy: Delete` cascades a LINSTOR snapshot delete when the `VolumeSnapshot` is removed.
- See `day2-snapshot-restore-kubernetes.md` for restoring.
