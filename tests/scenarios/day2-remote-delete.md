# day2-remote-delete

## Scenario

Remove a remote when it is no longer needed (for example after retiring an S3 bucket).

## Steps

1. Confirm no scheduled backups depend on the remote: `linstor schedule list-by-resource` for any active schedule using it.
2. If scheduled, disable / delete first (see `day2-schedule-delete.md`).
3. Delete the remote: `linstor remote delete <name>`.
4. Verify: `linstor remote list`.

## Expected outcome

- The remote is removed from LINSTOR.
- Existing backups stored on the remote are NOT touched.

## Validations

- `linstor remote list | grep <name>` returns nothing.
- Any active schedule that referenced the remote logs an error on its next run (if not disabled first).

## Doc reference

linstor-administration.adoc: lines 2602-2604 (`linstor remote delete myRemoteName`).

## Notes

- Re-creating a remote with the same name and credentials again gives you access to the previously stored backups via `backup list`.
