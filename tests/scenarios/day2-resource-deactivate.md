# day2-resource-deactivate

## Scenario

Deactivate a resource on a node so it can receive an incoming shipped snapshot (an LVM-thin / ZFS snapshot ship needs the target to be inactive).

## Steps

1. Confirm both source and target nodes have the resource deployed.
2. On the target, deactivate: `linstor resource deactivate <nodeTarget> resource1`.
3. Run the snapshot ship (see `day2-backup-ship-linstor-to-linstor.md` or `day2-snapshot-ship-self.md`).

## Expected outcome

- The resource on the target is in an inactive state and accepts an incoming snapshot ship.
- I/O on the target's DRBD device is blocked until the resource is reactivated (only possible for non-DRBD layered resources).

## Validations

- `linstor r l --node <nodeTarget> --resource resource1` shows the resource flagged inactive.
- Any attempt to write `/dev/drbdN` on the target returns an error.

## Doc reference

linstor-administration.adoc: `===== Shipping a snapshot in the same cluster` (lines 2883-2918).

## Notes

- WARNING: A resource with DRBD in its layer list cannot be reactivated after deactivate. The only way to use the data is to RESTORE the shipped snapshot into a NEW resource (`day2-snapshot-restore.md`).
- This is one of the few operations whose effect is permanent for DRBD-layered resources - plan carefully.
