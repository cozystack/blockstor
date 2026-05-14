# day2-over-provisioning-max-oversubscription

## Scenario

Set a single `MaxOversubscriptionRatio` that backstops both `MaxFreeCapacityOversubscriptionRatio` and `MaxTotalCapacityOversubscriptionRatio` when those are unset.

## Steps

1. Set the master ratio: `linstor controller set-property MaxOversubscriptionRatio 5`.
2. Optionally override one of the more specific properties on a pool: `linstor storage-pool set-property <node> <pool> MaxTotalCapacityOversubscriptionRatio 3`.
3. Verify: `linstor controller list-properties` and `linstor sp list-properties <node> <pool>`.

## Expected outcome

- Where the specific property is unset, the master ratio is used.
- Where the specific property is set, the specific value wins.

## Validations

- Newly placed volumes obey whichever of the two effective ratios is lower.

## Doc reference

linstor-administration.adoc: `==== Configuring a maximum over subscription ratio for over provisioning` (lines 3530-3548) and `==== The effects of setting values on multiple over provisioning properties` (lines 3550-3607).

## Notes

- Default is 20 for all three.
- Pool-level property always overrides controller-level for the same property.
- See `day2-over-provisioning-free-capacity.md` and `day2-over-provisioning-total-capacity.md`.
