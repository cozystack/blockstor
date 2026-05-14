# day2-drbd-replace-failed-disk-internal-metadata

## Scenario

Replace a failed physical disk that backs a DRBD resource configured with internal metadata, and resync from the surviving peer.

## Steps

1. Confirm the resource is `Diskless` on the failed node: `drbdadm status <resource>` shows `disk:Diskless` or DRBD auto-detached.
2. Physically replace the disk and recreate the backing volume (LVM LV / ZFS vol) with at least the same size.
3. Re-initialize DRBD metadata on the new device: `drbdadm create-md <resource>` (acknowledges "v08 Magic number not found", writes new metadata).
4. Attach: `drbdadm attach <resource>`.
5. Watch the resync: `drbdadm status --verbose <resource>`.

## Expected outcome

- The new disk is bound; a full sync starts automatically from the surviving UpToDate peer.
- After sync, the local replica is `UpToDate`.

## Validations

- `drbdadm status <resource>` initially shows `peer-disk:UpToDate`, local `disk:Inconsistent` → `SyncTarget` → `UpToDate`.
- `lsblk` shows the new disk underlying the DRBD device.

## Doc reference

drbd-troubleshooting.adoc: `==== Replacing a failed disk when using internal metadata` (lines 82-106).

## Notes

- If the disk's Linux name changes, also update the LINSTOR storage pool / LVM configuration so the backing LV resolves correctly.
- For external-metadata setups the procedure adds a `drbdadm invalidate` step - see `day2-drbd-replace-failed-disk-external-metadata.md`.
- In LINSTOR-managed setups, prefer `linstor resource toggle-disk` over manual `drbdadm` so the controller knows about the change.
