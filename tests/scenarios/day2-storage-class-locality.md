# day2-storage-class-locality

## Scenario

Optimise pod-to-data locality: place one replica on the same node as the first consuming pod, and forbid remote access from any node lacking a local replica.

## Steps

1. Define StorageClass with `volumeBindingMode: WaitForFirstConsumer` and `allowRemoteVolumeAccess: "false"`:
```
parameters:
  linstor.csi.linbit.com/storagePool: linstor-pool
  linstor.csi.linbit.com/placementCount: "2"
  linstor.csi.linbit.com/allowRemoteVolumeAccess: "false"
```
2. (Optional) Deploy the LINSTOR Affinity Controller (`day2-k8s-affinity-controller-deploy.md`) so node affinity stays consistent with replica placement after maintenance.

## Expected outcome

- The CSI driver waits for the first pod, learns its node, and places one replica there.
- Pods on nodes without a replica cannot bind the volume.

## Validations

- After the first pod schedules, `linstor r l --resource <rsc>` shows one replica on the same node.
- A pod scheduled to a node without a replica stays `Pending` with a volume affinity error.

## Doc reference

linstor-kubernetes.adoc: `=== Volume locality optimization` (lines 2720-2729) and `===== Single-zone homogeneous clusters` (lines 2508-2537).

## Notes

- Avoid `allowRemoteVolumeAccess: true` if you need strict locality - it lets remote-access pods bind without a local replica.
- Combine with the Affinity Controller to keep PV node affinity in sync with replica moves.
