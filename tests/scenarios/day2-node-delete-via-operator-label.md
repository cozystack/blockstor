# day2-node-delete-via-operator-label

## Scenario

Let the LINSTOR Operator (v2) evacuate and remove a Kubernetes node automatically by labelling it for deletion and updating the `linstorcluster` node-affinity.

## Steps

1. Label the node: `kubectl label nodes <node-name> marked-for-deletion=` (note the trailing `=`).
2. Edit the cluster CR: `kubectl edit linstorclusters linstorcluster` and set:
```
spec:
  nodeAffinity:
    nodeSelectorTerms:
    - matchExpressions:
      - key: marked-for-deletion
        operator: DoesNotExist
```
3. Wait for the Operator to evacuate the corresponding `LinstorSatellite`: `kubectl wait linstorsatellite/<node> --for=condition=EvacuationCompleted`.
4. Delete the Kubernetes node: `kubectl delete node <node-name>`.

## Expected outcome

- The LINSTOR satellite DaemonSet stops scheduling its pod on the labelled node.
- Internally, the Operator performs `linstor node evacuate <node>` for the satellite.
- After step 3, `linstor resource list --nodes <node>` is empty.
- After step 4, the node is gone from `kubectl get nodes` and from `linstor node list`.

## Validations

- `kubectl describe linstorsatellite/<node> | grep -i evacuation` shows the in-progress / completed condition.
- `linstor node list | grep <node>` returns nothing after final delete.
- All PVCs that had replicas on the node have their replica count restored on survivors.

## Doc reference

linstor-kubernetes.adoc: `=== Deleting a LINSTOR node in Kubernetes` (lines 2976-3026).

## Notes

- The Operator finishes evacuation only when LINSTOR can place replacement replicas; if no candidate exists, the `EvacuationCompleted` condition never goes true. Add a node or reduce `--place-count` first.
- The Operator-driven flow is preferred over manual `kubectl drain` + `linstor node evacuate` because it sequences the satellite teardown after evacuation completes.
