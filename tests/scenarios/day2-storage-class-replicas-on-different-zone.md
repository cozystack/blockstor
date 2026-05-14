# day2-storage-class-replicas-on-different-zone

## Scenario

Configure a Kubernetes StorageClass that spreads replicas across availability zones for HA across data centres.

## Steps

1. Ensure each node has a `topology.kubernetes.io/zone` label.
2. Define the StorageClass:
```
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: linstor-storage
provisioner: linstor.csi.linbit.com
volumeBindingMode: WaitForFirstConsumer
parameters:
  linstor.csi.linbit.com/storagePool: linstor-pool
  linstor.csi.linbit.com/placementCount: "2"
  linstor.csi.linbit.com/allowRemoteVolumeAccess: |
    - fromSame:
      - topology.kubernetes.io/zone
  linstor.csi.linbit.com/replicasOnDifferent: topology.kubernetes.io/zone
```
3. Apply and create a PVC referencing this class.

## Expected outcome

- Each replica is placed in a different zone.
- The PVC is accessible only from nodes in the same zone as one of its replicas.

## Validations

- `linstor r l --resource <rsc>` shows replicas on nodes whose `Aux/topology.kubernetes.io/zone` values are distinct.
- `kubectl get pv <pv> -o yaml | grep -A20 nodeAffinity` shows node selector restricting to the replica zones.

## Doc reference

linstor-kubernetes.adoc: `===== Multi-zonal homogeneous clusters` (lines 2538-2574).

## Notes

- The CSI driver propagates the Kubernetes node labels to LINSTOR aux properties (`Aux/<label>`), so `--replicas-on-different topology.kubernetes.io/zone` works.
- For multi-region clusters, use `replicasOnSame: topology.kubernetes.io/region` to keep all replicas within ONE region (separate failure domain).
