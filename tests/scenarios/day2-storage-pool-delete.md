# day2-storage-pool-delete

## Scenario

Remove a storage pool from LINSTOR after all resources on it have been evacuated or deleted.

## Steps

1. Confirm no resources reference the pool: `linstor resource list --storage-pool <pool>` returns empty.
2. Delete the LINSTOR storage-pool record: `linstor storage-pool delete <node> <pool>`.
3. Verify removal: `linstor storage-pool list --node <node>`.

## Expected outcome

- The pool entry is gone from LINSTOR.
- The underlying LVM VG or ZFS zpool is left in place - LINSTOR never destroys them.

## Validations

- `linstor sp l --node <node> --storage-pool <pool>` returns empty.
- On the satellite, `vgs` / `zpool list` still shows the underlying backing pool.

## Doc reference

linstor-administration.adoc: `==== Creating storage pools by using the physical storage command` (lines 699-731) - "there is no delete command, so such action must be done manually on the nodes".

## Notes

- If any resource still references the pool, delete fails. Either move the resource first (`resource toggle-disk --migrate-from`) or remove the resource.
- The underlying VG / zpool stays on the host; if you want to reclaim disks, manually run `vgremove` / `zpool destroy` after LINSTOR delete returns success.
