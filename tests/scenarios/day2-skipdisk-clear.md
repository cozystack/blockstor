# day2-skipdisk-clear

## Scenario

Clear the `DrbdOptions/SkipDisk` property that LINSTOR automatically set after a satellite detected I/O errors and detached DRBD from local storage.

## Steps

1. Identify the resource: `linstor r l` shows `Skip-Disk (R)` or `(R, N)` in the State column.
2. Fix the underlying I/O issue first (replace the failed disk, fix backend errors, etc.).
3. Clear the property by setting it without a value: `linstor resource set-property bravo rsc DrbdOptions/SkipDisk`.
4. Wait for DRBD to re-adjust and the replica to come back online.

## Expected outcome

- After the property is cleared, `drbdadm adjust` runs without `--skip-disk` and DRBD reattaches to the (now-healthy) backing storage.
- Replica state transitions from `Diskless` (after detach) to `Inconsistent` -> `SyncTarget` -> `UpToDate`.

## Validations

- `linstor r l --resource rsc | grep State` no longer contains `Skip-Disk`.
- Eventually `State=UpToDate` on the recovered node.

## Doc reference

linstor-administration.adoc: `==== SkipDisk` (lines 4427-4460).

## Notes

- Setting without a value DELETES the property.
- If you clear `SkipDisk` while the backend is still broken, DRBD will redetach on the next I/O and the property will be set again - fix root cause first.
- Cross-link: `day2-drbd-replace-failed-disk-internal-metadata.md` / `day2-drbd-replace-failed-disk-external-metadata.md` for the underlying physical fix.
