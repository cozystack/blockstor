# day2-resource-migrate

## Scenario

Move a diskful replica from one node to another without dropping redundancy at any point (online migration).

## Steps

1. Identify source (`alpha`) and target (`bravo`) nodes; ensure `bravo` has capacity in a matching storage pool.
2. Create a diskless replica on the target: `linstor resource create bravo backups --drbd-diskless`.
3. Toggle the target diskless replica to diskful with `--migrate-from <source>`: `linstor resource toggle-disk bravo backups --storage-pool pool_ssd --migrate-from alpha`.
4. LINSTOR waits for the new disk to be fully in sync, then removes the source diskful replica.

## Expected outcome

- At every point in time there are at least N diskful replicas (where N was the pre-migration count) plus the diskless one.
- After completion, `alpha` no longer has the resource; `bravo` has a diskful replica `UpToDate`.

## Validations

- `linstor r l --resource backups` during migration shows both `alpha` (UpToDate) and `bravo` (SyncTarget → UpToDate).
- After completion, no row for `alpha`; `bravo` is `UpToDate`.

## Doc reference

linstor-administration.adoc: `==== Migrating a resource to another node` (lines 3642-3656).

## Notes

- This is the only way to keep DRBD redundancy intact during a planned move; deleting source then creating target would temporarily drop redundancy.
- If the toggle gets stuck mid-migration, see `day2-resource-toggle-disk-stuck.md`.
