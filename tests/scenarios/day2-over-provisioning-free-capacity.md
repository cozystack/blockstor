# day2-over-provisioning-free-capacity

## Scenario

Limit LINSTOR over-provisioning of a thin storage pool by setting a `MaxFreeCapacityOversubscriptionRatio` property.

## Steps

1. Pick a ratio, for example 10 (allow new volumes up to 10x free space).
2. Set on each affected pool: `linstor storage-pool set-property <node> my_thin_pool MaxFreeCapacityOversubscriptionRatio 10`.
3. (Alternatively at controller scope) `linstor controller set-property MaxFreeCapacityOversubscriptionRatio 10`.
4. Verify with `linstor resource-group query-size-info <rg>`.

## Expected outcome

- New volumes can be at most `ratio * free_capacity`.
- As volumes fill, free capacity drops, and so does the allowed new-volume size.

## Validations

- `linstor sp list-properties <node> my_thin_pool | grep MaxFreeCapacityOversubscriptionRatio` returns the value.
- `query-size-info` shows the maximum volume size reduced accordingly.

## Doc reference

linstor-administration.adoc: `==== Configuring a maximum free capacity over provisioning ratio` (lines 3453-3499).

## Notes

- Default is 20.
- Pool-level property overrides controller-level.
- Combine with `MaxTotalCapacityOversubscriptionRatio` (`day2-over-provisioning-total-capacity.md`); LINSTOR uses the LOWER of the two when admitting new volumes.
