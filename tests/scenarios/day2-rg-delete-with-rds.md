# day2-rg-delete-with-rds

## Scenario

Attempt to delete a resource group that still has associated resource definitions; the operation must fail with a clear error.

## Steps

1. List the RG's RDs: `linstor resource-definition list | grep my_rg`.
2. Run `linstor resource-group delete my_rg`.
3. Expect an error.
4. Resolve by either deleting each RD (`rd d <name>`) or by reassigning them: `linstor resource-definition modify <rd> --resource-group <other_rg>`.
5. Retry `linstor resource-group delete my_rg` and verify it now succeeds.

## Expected outcome

- The first delete returns a non-zero exit with `Cannot delete resource group 'my_rg' because it has existing resource definitions.`
- After all RDs are removed or reassigned, the second delete succeeds.

## Validations

- Error message text matches `Cannot delete resource group 'my_rg' because it has existing resource definitions.`
- `linstor rg l | grep my_rg` returns empty after the second delete.

## Doc reference

linstor-administration.adoc: `==== Deleting a resource group` (lines 1403-1438).

## Notes

- The error message lists the RG by name but not the offending RDs; use `linstor rd l` to find them.
- There is no `--force` flag - the user must manually clear or reassign RDs before delete works.
