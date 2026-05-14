# day2-rg-do-not-place-with

## Scenario

Avoid co-locating a resource with another specific resource on the same node (for example, two HA databases that should not share blast radius).

## Steps

1. Create the RG with the constraint: `linstor resource-group create db_rg --place-count 2 --do-not-place-with otherdb`.
2. Spawn a resource: `linstor resource-group spawn db_rg dbA 50G`.
3. Verify: `linstor resource list --resource dbA` and confirm it shares no node with `otherdb`.

## Expected outcome

- LINSTOR's autoplacer skips any node that already hosts `otherdb`.
- If every candidate node hosts `otherdb`, spawn fails with `Not enough available nodes`.

## Validations

- For every replica node of `dbA`, `linstor r l --node <node> --resource otherdb` is empty.

## Doc reference

linstor-administration.adoc: `===== Avoiding colocating resources when automatically placing a resource` (lines 995-1005).

## Notes

- `--do-not-place-with-regex <pattern>` lets you exclude multiple resources by name pattern.
- Constraint is enforced only at placement time; later toggling-disk a colocating resource onto a now-shared node is not prevented.
