# day2-node-evacuate

## Scenario

Evacuate a satellite node of its resources before retiring or replacing it, while keeping the cluster's replica counts intact.

## Steps

1. Confirm no resources on the target node are `InUse` (DRBD Primary): `linstor resource list --nodes <node>` and check the `InUse` column.
2. If any are `InUse`, migrate the workload (unmount / fail-over) so the resource becomes `Unused`.
3. Trigger evacuation: `linstor node evacuate <node>`.
4. Wait for resources to re-sync to replacement nodes: `linstor resource list --nodes <node>` then `linstor resource list` and watch the `State` column for SyncTarget activity to finish.
5. Confirm the node has no remaining resources: `linstor resource list --nodes <node>` returns empty.
6. Delete the node from the cluster: `linstor node delete <node>`.

## Expected outcome

- After step 3, `linstor node list` shows the node `State=EVACUATE`.
- For every resource that had a replica on the target node, a new replica appears on a different node and reaches `UpToDate`.
- After step 6, the node is no longer in `linstor node list`.

## Validations

- `linstor node list --nodes <node>` returns empty after deletion.
- For each affected resource, replica count after evacuation matches the resource group's `--place-count`.
- `linstor resource list --nodes <node>` returns empty before step 6 is allowed to proceed.

## Doc reference

linstor-administration.adoc: `=== Evacuating a node` (lines 2364-2409).

## Notes

- If no suitable replacement node exists (for example, place-count equals total node count), `node evacuate` emits a warning and refuses to remove resources. Add a candidate node first or reduce `--place-count` on the affected RGs.
- For Kubernetes: cordon and drain the Kubernetes node first - see `day2-node-evacuate-kubernetes.md`.
- See `day2-node-evacuate-restore.md` for backing out before deletion.
