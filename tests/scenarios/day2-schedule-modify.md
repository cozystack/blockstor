# day2-schedule-modify

## Scenario

Update an existing backup-shipping schedule's cron, retention, or failure-handling options.

## Steps

1. Identify the schedule: `linstor schedule list`.
2. Modify (only specify what you want to change): `linstor schedule modify --keep-local 7 --on-failure SKIP daily-bu '0 2 * * *'`.
3. Verify with `linstor schedule list`.

## Expected outcome

- Updated fields reflect the new values.
- Unspecified fields retain their previous values.

## Validations

- `linstor schedule list | grep daily-bu` shows the new `KeepLocal=7` and `OnFailure=SKIP` columns.
- Subsequent runs of the schedule honour the new settings.

## Doc reference

linstor-administration.adoc: `==== Modifying a backup shipping schedule` (lines 3027-3036).

## Notes

- To reset `keep-local` / `keep-remote` to "all", explicitly pass `--keep-local all` / `--keep-remote all`.
- To reset `max-retries` to default, pass `--max-retries forever`.
