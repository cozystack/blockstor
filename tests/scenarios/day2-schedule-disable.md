# day2-schedule-disable

## Scenario

Suspend a scheduled backup shipping for a specific RG / RD / or controller scope without deleting the schedule object.

## Steps

1. Disable at the desired scope:
   - Controller-wide: `linstor backup schedule disable myRemote daily-bu`.
   - RG-only: `linstor backup schedule disable --rg my_ssd_group myRemote daily-bu`.
   - RD-only: `linstor backup schedule disable --rd <rd> myRemote daily-bu`.
2. Verify: `linstor schedule list-by-resource-details <rsc>`.

## Expected outcome

- No further backups for the disabled scope.
- Already shipped backups remain on the remote.
- The schedule object itself is still present (re-enabled with `day2-schedule-enable.md`).

## Validations

- `linstor schedule list-by-resource | grep <rsc>` shows `disabled` for the configured remote-schedule pair.
- No new entries appear on the remote at the next cron tick.

## Doc reference

linstor-administration.adoc: `==== Disabling a backup shipping schedule` (lines 3142-3170).

## Notes

- Disabling at a higher-priority level (RD) overrides enabling at a lower level (controller).
- To completely remove a disable at one level so the higher-level setting takes effect, use `backup schedule delete` (see `day2-schedule-delete-scope.md`).
