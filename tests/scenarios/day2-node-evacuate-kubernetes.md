# day2-node-evacuate-kubernetes

## Scenario

Evacuate a LINSTOR satellite node that is part of a Kubernetes cluster, moving both Kubernetes workloads and LINSTOR resources off it before retirement.

## Steps

1. Cordon the node so Kubernetes stops scheduling new pods onto it: `kubectl cordon <node-name>`.
2. Drain existing pods while ignoring DaemonSets (LINSTOR satellite is a DaemonSet): `kubectl drain --ignore-daemonsets <node-name>`.
3. Verify cluster workloads continue to run on the remaining nodes.
4. Evacuate LINSTOR resources from the node by following the regular `day2-node-evacuate` procedure: `linstor node evacuate <node-name>`.
5. Wait for `linstor node list` to show the node as `EVACUATE` and for all resources to finish syncing onto replacement nodes.
6. Delete the LINSTOR node: `linstor node delete <node-name>`.

## Expected outcome

- Pods that were running on the drained node are running on other Kubernetes nodes.
- PVs backed by LINSTOR resources that had replicas on the drained node still have the desired replica count, with all replicas now on survivors.
- The DaemonSet pod for the LINSTOR satellite on the drained node terminates once the LINSTOR node is removed.

## Validations

- `kubectl get nodes <node-name>` shows `SchedulingDisabled`.
- `kubectl get pods --all-namespaces --field-selector spec.nodeName=<node-name>` returns only DaemonSet pods (or nothing after final delete).
- `linstor resource list --nodes <node-name>` is empty before `node delete` is run.

## Doc reference

linstor-kubernetes.adoc: `=== Evacuating a node in Kubernetes` (lines 2949-2974).

## Notes

- For multi-node evacuations, set `AutoplaceTarget=false` on each target node first - see `day2-node-evacuate-multiple.md`.
- If a workload uses a PVC that has replicas only on the node being drained, the pod will not start until LINSTOR finishes placing a replica elsewhere - tune the order: evacuate-then-drain if you cannot tolerate the pod pause.
