# day2-rd-resize-shrink-warning

## Scenario

Shrink an existing volume safely: first shrink the file system, then ask LINSTOR to shrink the underlying block device.

## Steps

1. Verify no critical data lives in the tail of the FS; back up.
2. From the consumer, shrink the FS first: `umount /mnt; e2fsck -f /dev/drbdN; resize2fs /dev/drbdN 80G`.
3. Issue LINSTOR shrink: `linstor volume-definition set-size <rd> 0 80G`.
4. Confirm the new size on all replicas.

## Expected outcome

- The FS is smaller than or equal to the new VD size before LINSTOR shrinks the block device.
- All replicas converge to 80 GiB; DRBD broadcasts the new size.

## Validations

- `linstor vd l --resource-definition <rd>` shows 80 GiB.
- `blockdev --getsize64 /dev/drbdN` on each replica matches.
- The mounted FS reports the new size after re-mount.

## Doc reference

linstor-administration.adoc: `=== Creating and deploying resources and volumes` (line 851 WARNING) - shrinking can lose data if the FS was not shrunk first.

## Notes

- XFS cannot shrink at all; shrink only works on ext4 / btrfs / zfs (with caveats).
- LINSTOR does not enforce FS-shrink-first; it is the operator's responsibility. A successful LINSTOR shrink with a larger FS leaves the FS corrupt.
- See `day2-rd-resize.md` for the (safer) grow path.
