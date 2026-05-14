# day2-resource-group-filesystem

## Scenario

Configure a resource group so LINSTOR automatically creates a filesystem (ext4 or xfs) on each spawned resource.

## Steps

1. Choose ext4 or xfs and set the FS type: `linstor resource-group set-property nfs-rg FileSystem/Type xfs`.
2. (Optional) Set owner/group: `linstor resource-group set-property nfs-rg FileSystem/User nobody`, `linstor resource-group set-property nfs-rg FileSystem/Group nobody`.
3. (Optional) Add `mkfs` parameters: `linstor resource-group set-property nfs-rg FileSystem/MkfsParams '-L nfs-data'`.
4. Spawn: `linstor resource-group spawn-resources nfs-rg nfs-res 100G`.

## Expected outcome

- LINSTOR runs `mkfs.xfs` (or `mkfs.ext4`) on the DRBD device of the primary replica after creation.
- The FS is owned by the configured user/group.
- `blkid` on a satellite shows the FS type and label.

## Validations

- `linstor rg list-properties nfs-rg` shows `FileSystem/Type=xfs` (and other set keys).
- On a satellite, `blkid /dev/drbdN` returns `TYPE="xfs"`.
- After mounting, `ls -ld /mnt` shows the configured user/group.

## Doc reference

linstor-administration.adoc: `=== Creating a file system on a storage volume` (lines 1280-1340).

## Notes

- Defaults: user/group=root if `FileSystem/User` and `FileSystem/Group` are unset.
- The FS is created once, on first deployment - it is NOT recreated on subsequent toggles.
- See `mkfs.ext4(8)` / `mkfs.xfs(8)` for `MkfsParams` flags.
