# day2-schedule-delete-scope

## Scenario

Remove a (schedule, remote) pair from a specific scope (RD or RG) without disabling it - useful when you want a higher-priority scope's setting to apply again.

## Steps

1. Identify the level: `linstor schedule list-by-resource-details <rsc>` shows where the pair is configured.
2. Delete from that scope:
   - From an RD: `linstor backup schedule delete --rd <rd> myRemote daily-bu`.
   - From an RG: `linstor backup schedule delete --rg <rg> myRemote daily-bu`.
   - From controller: `linstor backup schedule delete myRemote daily-bu` (no `--rd`/`--rg`).
3. Verify the scope no longer shows the pair.

## Expected outcome

- The pair is removed from the chosen scope.
- A pair configured at a higher-priority scope (RD > RG > controller) is unaffected and continues to apply.

## Validations

- `linstor schedule list-by-resource-details <rsc>` shows the pair only at the levels you did NOT delete from.

## Doc reference

linstor-administration.adoc: `==== Deleting aspects of a backup shipping schedule` (lines 3171-3227).

## Notes

- This command does NOT delete previously shipped backups.
- Distinct from `schedule delete` (which removes the schedule itself, see `day2-schedule-delete.md`).
