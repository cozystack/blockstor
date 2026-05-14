# day2-rd-resize

## Scenario

Grow (or shrink) the size of an existing LINSTOR volume definition. All replicas resize online; the file system can be grown afterwards.

## Steps

1. Determine the VD number: `linstor volume-definition list --resource-definition backups` (first volume is `0`).
2. Resize: `linstor volume-definition set-size backups 0 100G` (for grow) or to a smaller value for shrink.
3. Wait for the satellites to converge.
4. (Inside the consumer) grow the filesystem: `resize2fs /dev/drbdN` or `xfs_growfs <mountpoint>`.

## Expected outcome

- LINSTOR runs `lvextend` / `zfs set volsize` on every replica, then `drbdadm resize` to publish the new size.
- `linstor vd l` shows the new size.
- The consumer sees the new size after the FS-level grow.

## Validations

- `linstor vd l --resource-definition backups` shows `100 GiB` in the size column.
- On any satellite, `blockdev --getsize64 /dev/drbdN` reports the new size in bytes.
- After `resize2fs`, `df -h /mountpoint` shows the new capacity.

## Doc reference

linstor-administration.adoc: `=== Creating and deploying resources and volumes` lines 844-853 plus the WARNING at line 851 about shrinking.

## Notes

- Shrinking is supported since LINSTOR 1.8.0 but only if the storage layers support it AND you shrink the file system first. Otherwise you risk data loss.
- The VD index is mandatory even when the RD has only one volume.
- Cross-link: `day2-rd-resize-shrink-warning.md` covers the shrink path explicitly.
