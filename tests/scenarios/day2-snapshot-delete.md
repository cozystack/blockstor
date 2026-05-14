# day2-snapshot-delete

## Scenario

Remove an existing snapshot from all nodes.

## Steps

1. List snapshots: `linstor snapshot list --resource resource1`.
2. Delete: `linstor snapshot delete resource1 snap1`.
3. Verify: `linstor s l --resource resource1 | grep snap1` returns empty.

## Expected outcome

- The snapshot entry is removed from LINSTOR.
- The underlying LVM-thin / ZFS snapshot is removed from each diskful node.

## Validations

- `linstor s l --resource resource1 | grep snap1` returns empty.
- On each former diskful node, `lvs | grep snap1` or `zfs list -t snapshot | grep snap1` returns empty.

## Doc reference

linstor-administration.adoc: `==== Removing a snapshot` (lines 2522-2530).

## Notes

- Must delete all snapshots before deleting the parent RD (see `day2-rd-delete.md`).
- Deleting a snapshot does NOT affect resources that were restored from it earlier.
