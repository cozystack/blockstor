# day2-rd-delete

## Scenario

Delete a resource definition and cascade-remove every replica from all satellites.

## Steps

1. Ensure no snapshots exist for the RD: `linstor snapshot list --resource <rd>`. If snapshots exist, delete them first.
2. Issue delete: `linstor resource-definition delete <rd>`.
3. Wait for all replicas to be removed from satellites.
4. Verify: `linstor rd l | grep <rd>` returns nothing.

## Expected outcome

- LINSTOR marks the RD and all child resources `FlagDelete`.
- Each satellite tears down its DRBD device, frees ports and removes the backing LV / dataset.
- Final state: no rows in `rd l`, `r l`, or `vd l` for that RD.

## Validations

- `linstor rd l --resource <rd>` returns empty.
- `linstor r l --resource <rd>` returns empty.
- On every satellite, `drbdadm status <rd>` returns "no resources defined" (or similar).
- Ports previously used by the RD show as free in `linstor controller list-properties` / `TcpPortAutoRange` accounting.

## Doc reference

linstor-administration.adoc: `==== Deleting a resource definition` (lines 1350-1366).

## Notes

- If snapshots exist, the RD delete returns an error pointing to `snapshot delete`. Use `day2-snapshot-delete.md`.
- Force-stripping CRD finalizers in Kubernetes is FORBIDDEN - it leaves DRBD kernel state alive and ports stuck. Always use `linstor rd d`.
- See `day2-resource-delete-single-replica.md` for removing one replica without cascading.
