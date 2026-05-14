# day2-backup-restore-s3

## Scenario

Restore a backup from an S3 remote into a new resource on a target node.

## Steps

1. List available backups: `linstor backup list myRemote`.
2. Run the restore, either picking the latest by resource name or a specific backup ID:
```
linstor backup restore myRemote myNode targetRsc --resource sourceRsc
# or
linstor backup restore myRemote myNode targetRsc --id sourceRsc_back_20210824_072543
```
3. If storage pool names differ between source and target, add `--storpool-rename oldname=newname`.
4. Wait for the download / restore to finish.

## Expected outcome

- LINSTOR downloads all snapshots from the last full backup up to the requested point, then restores them into a new RD `targetRsc`.
- The resource is placed on the specified node.

## Validations

- `linstor rd l | grep targetRsc` returns the new RD.
- `linstor r l --node myNode --resource targetRsc` shows the restored resource as `UpToDate`.
- Data hash matches the source.

## Doc reference

linstor-administration.adoc: `==== Restoring backups from a remote` (lines 2788-2845).

## Notes

- `--download-only` skips the restore step; useful for inspecting backups without making them active.
- If the source had a LUKS layer, you MUST pass `--passphrase <source-passphrase>` so LINSTOR can decrypt and re-encrypt under the target's master key.
- `linstor backup info myRemote --resource <rsc>` previews download size and required storage pool renames.
