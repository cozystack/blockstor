# day2-snapshot-auto-create

## Scenario

Enable periodic automatic snapshotting of a resource and retain only the N most recent snapshots.

## Steps

1. Set the snapshot interval (minutes): `linstor resource-definition set-property <rd> AutoSnapshot/RunEvery 15`.
2. (Optional) Set how many to keep: `linstor resource-definition set-property <rd> AutoSnapshot/Keep 5`.
3. Wait for the next interval and confirm snapshots appear.

## Expected outcome

- Every 15 minutes LINSTOR creates a new snapshot of the RD.
- After the 6th snapshot is created, the oldest auto-created snapshot is removed.
- Manually-created snapshots are never deleted by this mechanism.

## Validations

- `linstor s l --resource <rd>` shows auto-named snapshots arriving on schedule.
- After running for >5 intervals, the snapshot count stabilises at 5.

## Doc reference

linstor-administration.adoc: `==== Creating a snapshot` lines 2462-2471.

## Notes

- Default `AutoSnapshot/Keep` is 10 if unset or <= 0.
- Manual snapshots (`snapshot create`) are NOT counted against the keep budget and are not auto-deleted.
- Combine with `day2-schedule-create.md` for off-site shipping in addition to local retention.
