# day2-k8s-rwx-pvc

## Scenario

Provision a ReadWriteMany PVC backed by LINSTOR (uses NFS export + DRBD Reactor under the hood).

## Steps

1. Confirm the LINSTOR cluster has >= 3 nodes for DRBD quorum (recommended).
2. Use (or create) a StorageClass whose `autoPlace` >= 2.
3. Create the PVC:
```
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: demo-rwx-pvc-0
spec:
  storageClassName: linstor-csi-lvm-thin-r3
  accessModes:
  - ReadWriteMany
  resources:
    requests:
      storage: 5Gi
```
4. Reference the PVC from multiple Pods simultaneously and confirm they all mount.

## Expected outcome

- PVC is bound; behind the scenes LINSTOR provisions a DRBD resource and DRBD Reactor manages an HA NFS export.
- Multiple pods can read/write concurrently via NFS.

## Validations

- `kubectl get pvc demo-rwx-pvc-0` shows `STATUS=Bound`, `ACCESS MODES=RWX`.
- Two pods on different nodes can simultaneously write to the mounted PVC.

## Doc reference

linstor-kubernetes.adoc: `=== ReadWriteMany volume access` (lines 2635-2696).

## Notes

- Performance is lower than ReadWriteOnce due to NFS.
- Do NOT set `mountOpts` in the storage class - the operator picks the right ones for the NFS export.
- For sub-3-node clusters: explicitly set `DrbdOptions/Resource/quorum: majority` in the SC; otherwise quorum defaults to `off` and split-brain risk increases.
