# day2-backup-delete

## Scenario

Delete backups from a remote (S3) - by ID, by filter, by cluster, or all.

## Steps

1. Identify backups to remove: `linstor backup list myRemote`.
2. Choose a deletion mode:
   - All backups (this cluster's only): `linstor backup delete all myRemote --cluster`.
   - Single ID: `linstor backup delete id myRemote myRsc_back_20210824_072543` (optionally with `--cascade`).
   - Filter: `linstor backup delete filter myRemote -t 20210914_120000 -n myNode -r myRsc` (optionally with `--cascade`).
   - S3 key (clean up debris): `linstor backup delete s3key myRemote <key>`.
3. Use `--dry-run` first to preview.

## Expected outcome

- Selected backups are removed from the remote.
- Without `--cascade`, LINSTOR refuses to delete a full backup that has dependent incremental backups.

## Validations

- `linstor backup list myRemote` after the operation no longer shows the deleted backups.
- `--dry-run` output matches what the real run will delete.

## Doc reference

linstor-administration.adoc: `==== Deleting backups on a remote` (lines 2752-2786).

## Notes

- `--cascade` is required when the chosen backups have dependents - otherwise the command aborts.
- `delete s3key` is meant for cleaning up non-backup junk; deleting an active backup this way will corrupt the chain.
- Always run with `--dry-run` first on production buckets.
