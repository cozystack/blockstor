# day2-drbd-toggle-skipdisk-via-property

## Scenario

Manually set `DrbdOptions/SkipDisk=True` to keep DRBD running with `--skip-disk` even after a transient disk error has cleared, useful during scheduled maintenance.

## Steps

1. Set the property at the desired scope: `linstor resource set-property <node> <rsc> DrbdOptions/SkipDisk True`.
2. Verify `linstor r l --resource <rsc>` shows the `Skip-Disk (R)` indicator.
3. When maintenance is complete, clear: `linstor resource set-property <node> <rsc> DrbdOptions/SkipDisk` (no value = delete).

## Expected outcome

- While the property is set, the satellite calls `drbdadm adjust --skip-disk`.
- DRBD stays running but does not try to attach local storage.

## Validations

- `linstor r l --resource <rsc> | grep Skip-Disk` returns a match while set.
- After unset, the indicator disappears.

## Doc reference

linstor-administration.adoc: `==== SkipDisk` (lines 4427-4460).

## Notes

- Used as the safe answer to repeated disk-flap events: hold off DRBD attach until you've confirmed the disk is healthy.
- Indicator `(R)` = on resource; `(R, N)` = on resource AND node level.
- Cross-link: `day2-skipdisk-clear.md` for the auto-set-by-LINSTOR scenario.
