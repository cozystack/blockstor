# day2-backup-restore-s3-kubernetes

## Scenario

Register and restore an existing S3 backup as a Kubernetes VolumeSnapshot so a PVC can be created from it.

## Steps

1. Configure the remote in LINSTOR: `linstor remote create s3 backup-remote s3.us-west-1.amazonaws.com snapshot-bucket us-west-1 access-key secret-key`.
2. List available backups: `linstor backup list backup-remote`.
3. Create a `VolumeSnapshotContent` referencing the S3 snapshot ID:
```
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotContent
metadata:
  name: restored-snap-content-from-s3
spec:
  deletionPolicy: Delete
  driver: linstor.csi.linbit.com
  source:
    snapshotHandle: <snapshot-id>
  volumeSnapshotClassName: linstor-csi-snapshot-class-s3
  volumeSnapshotRef:
    apiVersion: snapshot.storage.k8s.io/v1
    kind: VolumeSnapshot
    name: example-backup-from-s3
    namespace: project
```
4. Create a matching `VolumeSnapshot`:
```
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: example-backup-from-s3
  namespace: project
spec:
  source:
    volumeSnapshotContentName: restored-snap-content-from-s3
  volumeSnapshotClassName: linstor-csi-snapshot-class-s3
```
5. Wait for the snapshot to become `READYTOUSE=true`, then create a PVC referencing it as `dataSource`.

## Expected outcome

- The Kubernetes snapshot pair becomes ready.
- A PVC using the snapshot as `dataSource` is bound and the new volume contains the restored data.

## Validations

- `kubectl get volumesnapshot example-backup-from-s3 -n project` shows `READYTOUSE=true`.
- A PVC referencing it binds successfully.

## Doc reference

linstor-kubernetes.adoc: `===== Restoring from remote snapshots` (lines 2434-2484).

## Notes

- The `linstor-csi-snapshot-class-s3` VolumeSnapshotClass and the S3 access secret must already exist (see linstor-kubernetes.adoc lines 2388-2433).
- For a LUKS-layered backup, you must set the source-cluster passphrase on the remote first.
