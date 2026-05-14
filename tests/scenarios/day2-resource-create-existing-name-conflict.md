# day2-resource-create-existing-name-conflict

## Scenario

Verify that creating a resource definition with the name of an existing RD fails cleanly with a 409 Conflict / actionable error.

## Steps

1. Ensure `backups` RD exists: `linstor rd l | grep backups`.
2. Re-issue create: `linstor resource-definition create backups`.
3. Inspect the error.

## Expected outcome

- The command exits non-zero.
- Error text contains `already exists` or equivalent and the offending name.

## Validations

- Error message contains `backups` and `already exists`.
- `linstor rd l | grep -c backups` still returns 1 (no duplicate).

## Doc reference

linstor-administration.adoc: `=== Creating and deploying resources and volumes` (lines 823-862) - implicit behaviour.

## Notes

- The same applies to `resource create` for an existing (node, rd) pair - LINSTOR returns a conflict instead of duplicating.
- Helpful for idempotent automation: catch the conflict and treat as success after verifying state.
