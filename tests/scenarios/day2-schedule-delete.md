# day2-schedule-delete

## Scenario

Completely remove a backup shipping schedule LINSTOR object.

## Steps

1. (Optional) Disable first to stop active shipments cleanly.
2. Delete: `linstor schedule delete daily-bu`.
3. Verify: `linstor schedule list | grep daily-bu` returns empty.

## Expected outcome

- The schedule is removed cluster-wide.
- Already shipped backups and local snapshots are NOT deleted.
- If a shipment was in progress at the time of delete, it may be aborted mid-way (the resulting partial backup must be cleaned up manually).

## Validations

- `linstor schedule list` no longer contains the schedule.

## Doc reference

linstor-administration.adoc: `==== Deleting a backup shipping schedule` (lines 3102-3112).

## Notes

- Different from `backup schedule delete --rd ...` which removes a pair from a single scope; this command removes the schedule object entirely.
- Use `keep-local` / `keep-remote` retention to remove old artefacts; this command does not.
