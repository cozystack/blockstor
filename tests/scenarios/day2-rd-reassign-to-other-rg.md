# day2-rd-reassign-to-other-rg

## Scenario

Move a resource definition from one resource group to another (changes the inherited template).

## Steps

1. Find the RD's current RG: `linstor rd l | grep <rd>`.
2. Reassign: `linstor resource-definition modify <rd> --resource-group <other_rg>`.
3. Verify: `linstor rd l | grep <rd>` shows the new RG.

## Expected outcome

- The RD now inherits placement, layer, DRBD-options defaults from the new RG.
- Existing replicas are NOT moved; only future autoplaces / balance operations use the new defaults.

## Validations

- `linstor rd l --resource <rd>` shows the new ResourceGroup column value.
- New properties set on the new RG begin to affect this RD on next placement/modify.

## Doc reference

linstor-administration.adoc: `==== Deleting a resource group` (lines 1421-1429) shows the modify syntax.

## Notes

- Use this to fix "stuck with DfltRscGrp" RDs, or to migrate between SLA tiers.
- Properties set directly on the RD continue to win over the new RG (priority order: RD > RG).
