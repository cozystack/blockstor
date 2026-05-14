# day2-snapshot-create

## Scenario

Create a point-in-time snapshot of a resource backed by a snapshot-capable pool (LVM-thin, ZFS, ZFS-thin, FILE_THIN on reflink-capable FS).

## Steps

1. Verify the resource's storage pool supports snapshots: `linstor sp l --node <node>` shows `LVM_THIN` / `ZFS` / `ZFS_THIN`.
2. Create the snapshot: `linstor snapshot create resource1 snap1`.
3. Confirm: `linstor snapshot list --resource resource1`.

## Expected outcome

- LINSTOR coordinates with DRBD to produce a consistent point-in-time snapshot on every diskful replica.
- A snapshot entry `snap1` appears for `resource1` with `State=Successful`.

## Validations

- `linstor s l --resource resource1 | grep snap1` returns one row with `Successful`.
- On each diskful satellite, `lvs --readonly | grep snap1` (LVM-thin) or `zfs list -t snapshot | grep snap1` (ZFS) shows the snapshot.

## Doc reference

linstor-administration.adoc: `==== Creating a snapshot` (lines 2449-2471).

## Notes

- Snapshots succeed even when the resource is in active use (consistent across replicas).
- LVM-thick and ZFS-thick pools do NOT support LINSTOR snapshots - the command fails.
