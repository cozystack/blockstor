# day2-node-evacuate-restore

## Scenario

Back out of a `node evacuate` operation before the node is deleted, returning the node to the eligible pool for new placements.

## Steps

1. Confirm the node is in `EVACUATE` state: `linstor node list`.
2. Run `linstor node restore <node>`.
3. If you previously set `AutoplaceTarget=false`, re-enable autoplacement: `linstor node set-property <node> AutoplaceTarget true`.

## Expected outcome

- `linstor node list` shows the node back as `Online`.
- The node's network interfaces, storage pools, properties and non-DRBD resources persist.
- Resources that LINSTOR already evacuated stay on their new hosts; they do not automatically come back.

## Validations

- `linstor node list | grep <node>` reports `Online`, not `EVACUATE`.
- `linstor storage-pool list --nodes <node>` shows the previously-configured pools intact.
- `linstor node list-properties <node>` shows `AutoplaceTarget=true` (or unset) after step 3.

## Doc reference

linstor-administration.adoc: `==== Restoring an evacuating node` (lines 2424-2443).

## Notes

- `node restore` only works before `node delete`. Once delete completes, the node entry is gone and must be re-added with `node create`.
- To move evacuated resources back onto the restored node manually, first create them on the restored node (or use `resource toggle-disk --migrate-from`), then delete them from the new host.
