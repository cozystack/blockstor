# day2-storage-pool-create-zfs

## Scenario

Create a ZFS-backed storage pool (thick) on one or more satellites.

## Steps

1. On each satellite, create the zpool: `zpool create zpool1 /dev/nvme1n1`.
2. Register the storage pool with LINSTOR: `linstor storage-pool create zfs <node> zfs-pool zpool1`.
3. List pools to verify: `linstor storage-pool list`.

## Expected outcome

- `linstor sp l` shows the new pool with `Driver=ZFS` and `PoolName=zpool1`.
- LINSTOR resources created against the pool appear as ZFS datasets/volumes named `zpool1/<rd>_NNNNN` on the satellite.

## Validations

- `linstor sp l --node <node> | grep zfs-pool` returns one row.
- On the satellite, `zfs list` shows the datasets LINSTOR creates after a `resource create`.
- `linstor node info | grep <node>` shows `ZFS=+` in the providers table.

## Doc reference

linstor-administration.adoc: `=== Storage providers` (lines 1997-2029) and `==== Creating storage pools` (lines 610-651).

## Notes

- Thick ZFS does NOT support LINSTOR snapshots until you use the `zfsthin` driver (`zfs_thin`).
- Underlying zpool tuning (`recordsize`, `compression`, `atime`) is the operator's responsibility - LINSTOR will inherit it.
- The `StorDriver/ZfscreateOptions` property appends args to `zfs create` if needed (for example to set per-volume `volblocksize`).
