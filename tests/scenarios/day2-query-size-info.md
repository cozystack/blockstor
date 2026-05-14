# day2-query-size-info

## Scenario

Preview the maximum volume size LINSTOR will allow you to spawn from a given resource group, based on free capacity and over-provisioning ratios.

## Steps

1. Run `linstor resource-group query-size-info DfltRscGrp` (or any RG name).
2. Inspect the output table: `MaxVolumeSize`, `AvailableSize`, `Capacity`, `Next Spawn Result`.

## Expected outcome

- LINSTOR returns the largest volume size that satisfies the RG's placement constraints and the over-provisioning properties.
- The "Next Spawn Result" column shows which storage pools would be picked.

## Validations

- `linstor rg query-size-info <rg>` returns a single-row table.
- `MaxVolumeSize` is consistent with `MaxFreeCapacityOversubscriptionRatio * AvailableSize` (lowered by `MaxTotalCapacityOversubscriptionRatio * Capacity` if smaller).

## Doc reference

linstor-administration.adoc: lines 3480-3498 (example output) and over-provisioning sections.

## Notes

- This is a dry-run preview - no resources or snapshots are created.
- Reflects the current state; capacity may change before you actually spawn.
