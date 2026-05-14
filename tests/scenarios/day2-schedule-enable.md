# day2-schedule-enable

## Scenario

Activate a previously created backup schedule for a specific resource group, resource definition, or the entire controller.

## Steps

1. Enable for all deployed resources (controller scope): `linstor backup schedule enable myRemote daily-bu`.
2. Or enable for a specific RG: `linstor backup schedule enable --rg my_ssd_group myRemote daily-bu`.
3. Or for a single RD: `linstor backup schedule enable --rd backups myRemote daily-bu`.
4. (Optional) Force a specific source node: `--node <node>`.

## Expected outcome

- LINSTOR begins shipping backups according to the cron schedule.
- The enable scope (controller / RG / RD) takes effect immediately; the precedence order from lowest to highest is controller -> RG -> RD.

## Validations

- `linstor schedule list-by-resource` shows the configured remote+schedule pair for the matching scope.
- At the next scheduled tick, a backup appears on the remote.

## Doc reference

linstor-administration.adoc: `==== Enabling scheduled backup shipping` (lines 3114-3140).

## Notes

- You cannot specify BOTH `--rg` and `--rd` at the same time.
- See `day2-schedule-disable.md` and `day2-schedule-delete.md` for the inverse operations.
- Schedule decision flow: RD > RG > controller; an enable/disable at a higher-priority level overrides the lower.
