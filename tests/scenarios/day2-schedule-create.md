# day2-schedule-create

## Scenario

Define a scheduled backup-shipping policy with cron-style full + incremental cadence and retention limits.

## Steps

1. Create a schedule:
```
linstor schedule create \
  --incremental-cron '*/5 * * * *' \
  --keep-local 5 \
  --keep-remote 4 \
  --on-failure RETRY \
  --max-retries 10 \
  daily-bu '0 2 * * *'
```
(`* * *` is the full-backup cron; `--incremental-cron` is optional.)
2. Verify: `linstor schedule list`.

## Expected outcome

- A schedule `daily-bu` exists with the chosen cron, retention and failure policy.
- The schedule is NOT yet active - it must be enabled separately (see `day2-schedule-enable.md`).

## Validations

- `linstor schedule list | grep daily-bu` returns one row showing `Full`, `Incremental`, `KeepLocal`, `KeepRemote`, `OnFailure` columns.

## Doc reference

linstor-administration.adoc: `==== Creating a backup shipping schedule` (lines 2964-3025).

## Notes

- Cron schemas must be single- or double-quoted on the shell.
- If incremental and full cron schemas overlap, the overlapping tick yields a FULL backup (not both).
- `--keep-remote` only works for S3 remotes; LINSTOR-to-LINSTOR remotes cannot manage the target side's retention.
