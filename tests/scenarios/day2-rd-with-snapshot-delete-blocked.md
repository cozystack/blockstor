# day2-rd-with-snapshot-delete-blocked

## Scenario

Verify that LINSTOR refuses to delete a resource definition while it still has snapshots, and that the operator can recover by deleting the snapshots first.

## Steps

1. Confirm snapshots exist for the RD: `linstor s l --resource <rd>`.
2. Attempt delete: `linstor resource-definition delete <rd>`.
3. Observe the error referencing snapshots.
4. Delete each snapshot: `linstor snapshot delete <rd> <snap>` (repeat).
5. Retry: `linstor resource-definition delete <rd>` and verify it succeeds.

## Expected outcome

- The first delete fails with an error pointing at the existing snapshots.
- After all snapshots are deleted, the RD delete succeeds.

## Validations

- First delete returns non-zero with `snapshot` in the error message.
- After cleanup, `linstor rd l | grep <rd>` returns empty.

## Doc reference

linstor-administration.adoc: `==== Deleting a resource definition` (lines 1364-1366 WARNING) and `<<s-linstor-snapshots-removing-a-snapshot>>` reference.

## Notes

- This is the only RD-delete blocker that LINSTOR enforces - missing finalizers in k8s can mask it.
- If you have many snapshots, scripted bulk-delete: `linstor s l --resource <rd> -p | awk '...' | xargs ...`.
