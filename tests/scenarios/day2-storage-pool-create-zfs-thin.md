# day2-storage-pool-create-zfs-thin

## Scenario

Create a thin-provisioned ZFS (zfs-thin) storage pool with snapshot support.

## Steps

1. On each satellite, create the zpool: `zpool create tank /dev/sdX`.
2. Register the LINSTOR storage pool: `linstor storage-pool create zfsthin <node> zfs-thin tank`.
3. List to verify: `linstor storage-pool list`.

## Expected outcome

- `linstor sp l` shows the new pool with `Driver=ZFS_THIN` and `PoolName=tank`.
- The pool supports LINSTOR snapshots and snapshot shipping.

## Validations

- `linstor sp l --node <node> | grep zfs-thin` returns `ZFS_THIN` rows.
- `linstor node info | grep <node>` shows `ZFS/Thin=+`.
- `linstor snapshot create <rd-on-pool> snap1` succeeds.

## Doc reference

linstor-administration.adoc: `=== Storage providers` (lines 1997-2029).

## Notes

- For storage-pool mixing, `zfs` and `zfsthin` are treated as compatible (same extent size, same DRBD initial-sync strategy), so mixing them does NOT trip the `AllowMixingStoragePoolDriver` requirement (see `day2-storage-pool-mixing.md`).
- Setting `StorDriver/ZfscreateOptions` lets you append flags such as `-o volblocksize=16k` to every `zfs create`.
