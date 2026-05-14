# day2-backup-create-s3

## Scenario

Create and upload a backup (snapshot) of a LINSTOR resource to an S3 remote.

## Steps

1. Confirm an S3 remote exists: `linstor remote list`.
2. Ship a backup: `linstor backup create myRemote myRsc`.
3. Wait for the upload to finish.
4. List backups on the remote: `linstor backup list myRemote`.

## Expected outcome

- LINSTOR creates a snapshot of `myRsc` on a chosen diskful node, then uploads it to the S3 bucket.
- The remote shows a backup entry with the resource name, a timestamp suffix and the schema `<rsc>_back_YYYYMMDD_HHMMSS`.

## Validations

- `linstor backup list myRemote --resource myRsc` returns at least one entry.
- After upload, `linstor s l` shows the local snapshot used as the source.

## Doc reference

linstor-administration.adoc: `==== Shipping snapshots to an S3 remote` (lines 2633-2650).

## Notes

- The first ship is always a full backup. Subsequent ships are incremental against the most recent snapshot unless `--full` is passed.
- Use `--node <name>` to force a specific node to ship; if the node lacks the resource diskfully, LINSTOR picks another.
- Encryption must be enabled.
