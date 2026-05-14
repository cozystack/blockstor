# day2-snapshot-rollback

## Scenario

Roll a resource back to a previous snapshot state in-place (destructive - changes since the snapshot are lost).

## Steps

1. Ensure the resource is not in use anywhere: unmount it, demote DRBD to Secondary on every node.
2. Confirm with `linstor r l --resource <rd>` - no row has `InUse`.
3. Run `linstor snapshot rollback <rd> <snap>`.
4. Bring the resource back online.

## Expected outcome

- LINSTOR rolls every replica to the snapshot state.
- Modifications made after the snapshot are discarded.
- Resource returns to `UpToDate` on the nodes where the snapshot existed.

## Validations

- After rollback, the data on the DRBD device matches the snapshot content.
- `linstor r l --resource <rd>` shows all replicas `UpToDate`.

## Doc reference

linstor-administration.adoc: `==== Rolling back to a snapshot` (lines 2499-2521).

## Notes

- LINSTOR >= 1.31.2: rollback works even on nodes that were added after the snapshot was created - the new node just resyncs from an existing diskful replica.
- LINSTOR < 1.31.2 refuses if the snapshot is missing on any current diskful node.
- Side effect (>=1.31.2): if you removed replicas after the snapshot and then rolled back, the periodic balance task may re-add replicas on the nodes where the snapshot still exists.
- USE WITH CAUTION - data after the snapshot is unrecoverable. Use `day2-snapshot-restore.md` if you need both versions to coexist.
