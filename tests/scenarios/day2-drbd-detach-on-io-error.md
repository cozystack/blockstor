# day2-drbd-detach-on-io-error

## Scenario

A backing disk on a satellite started returning I/O errors. DRBD has automatically detached (resource is now Diskless on that node).

## Steps

1. Observe the event: `dmesg | grep -i 'disk(.*) failed'` plus DRBD-detached messages.
2. `drbdadm status <rsc>` reports `disk:Diskless` on the affected node, `peer-disk:UpToDate` on a healthy peer.
3. (Optional manual detach, if not auto-configured) `drbdadm detach <resource>`.
4. Replace / repair the disk.
5. Re-attach: see `day2-drbd-replace-failed-disk-internal-metadata.md` (or external variant).

## Expected outcome

- I/O continues seamlessly via the surviving peer.
- LINSTOR auto-sets `DrbdOptions/SkipDisk=True` on the resource (see `day2-skipdisk-clear.md`).
- Replacement and re-attach steps bring the replica back to `UpToDate`.

## Validations

- `linstor r l --node <affected> --resource <rsc>` shows `Skip-Disk` indicator in State.
- `drbdadm status <rsc>` on the affected node shows `disk:Diskless`.
- After repair + clear-SkipDisk, the replica returns to `UpToDate`.

## Doc reference

drbd-troubleshooting.adoc: `=== Dealing with hard disk failure` (lines 21-80) and linstor-administration.adoc: `==== SkipDisk` (lines 4427-4460).

## Notes

- Auto-detach is the LINBIT-recommended mode (`on-io-error detach`). Manual detach is rarely needed.
- Clearing `SkipDisk` BEFORE the underlying disk is repaired leads to a redetach loop.
