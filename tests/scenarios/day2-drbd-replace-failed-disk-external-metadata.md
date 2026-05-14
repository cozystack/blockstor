# day2-drbd-replace-failed-disk-external-metadata

## Scenario

Replace a failed disk for a DRBD resource that uses external metadata; ensure a full resync from a healthy peer.

## Steps

1. Replace the physical disk and recreate the backing volume.
2. Initialise metadata on the new device: `drbdadm create-md <resource>`.
3. Attach: `drbdadm attach <resource>`.
4. Force a full resync from the peer: `drbdadm invalidate <resource>` (RUN ONLY on the NODE WITHOUT good data).
5. Watch progress with `drbdadm status --verbose <resource>`.

## Expected outcome

- The new disk's content is overwritten with data from the UpToDate peer.
- Replica converges to `UpToDate`.

## Validations

- `drbdadm status <resource>` shows `disk:Inconsistent` immediately after `invalidate`, then `SyncTarget`, then `UpToDate`.
- Final state on the replaced node matches the peer's content.

## Doc reference

drbd-troubleshooting.adoc: `==== Replacing a failed disk when using external metadata` (lines 108-133) and explicit WARNING at line 128.

## Notes

- WARNING: Running `drbdadm invalidate` on the WRONG node (the one with the good data) destroys data. Always confirm which node is the survivor first via `drbdadm status` peer-disk states.
- LINSTOR-managed equivalent: `linstor resource toggle-disk <node> <rsc> --diskless` then re-toggle to diskful.
