# day2-rg-unset-placement-property

## Scenario

Remove a previously-set autoplacement constraint from a resource group (for example a too-strict `--replicas-on-same`).

## Steps

1. Find the constraint: `linstor rg list-properties <rg>`.
2. Unset with the matching argument:
```
linstor resource-group modify <rg> --replicas-on-same ""
linstor resource-group modify <rg> --replicas-on-different ""
linstor resource-group modify <rg> --do-not-place-with ""
linstor resource-group modify <rg> --do-not-place-with-regex ""
linstor resource-group modify <rg> --layer-list ""
linstor resource-group modify <rg> --providers ""
```
3. Verify with `linstor rg list-properties <rg>`.

## Expected outcome

- The chosen constraint is removed; future autoplaces are no longer limited by it.
- Existing resources are unaffected.

## Validations

- `linstor rg list-properties <rg>` no longer shows the cleared key.
- A new `rg spawn` succeeds where it previously failed due to the constraint.

## Doc reference

linstor-administration.adoc: `====== Unsetting autoplacement properties` (lines 1076-1097).

## Notes

- Passing an empty string is the documented way to clear placement constraints on RGs.
- Existing replicas stay where they are; the rebalance task may move them on the next interval if the new constraints permit.
