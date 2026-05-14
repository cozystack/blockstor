# day2-storage-pool-create-lvm-thin

## Scenario

Create a thin-provisioned LVM (LVM-thin) storage pool, which enables snapshot support.

## Steps

1. On each satellite, create a thin pool inside an existing VG: `lvcreate -L 100G --thinpool thinpool drbdpool`.
2. Register the storage pool: `linstor storage-pool create lvmthin <node> thin-lvm drbdpool/thinpool`.
3. Verify: `linstor storage-pool list`.

## Expected outcome

- `linstor sp l` shows the new pool with `Driver=LVM_THIN` and `PoolName=drbdpool/thinpool` on every contributing node.
- The pool can back resources that require snapshots and S3 / scheduled backup shipping.

## Validations

- `linstor sp l | grep thin-lvm` shows `LVM_THIN` rows.
- `linstor node info` for each node shows `LVMThin=+`.
- `linstor snapshot create <rd> <snap>` against a resource on this pool succeeds (basic functional check).

## Doc reference

linstor-administration.adoc: `==== Creating storage pools` (lines 610-651) plus storage providers table at lines 1997-2029.

## Notes

- Driver name in the CLI is `lvmthin` (one word), not `lvm-thin`.
- Thin pools are the only LVM variant that supports LINSTOR snapshots.
- See `day2-storage-pool-create-zfs-thin.md` for the ZFS equivalent.
