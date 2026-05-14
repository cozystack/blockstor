# day2-over-provisioning-total-capacity

## Scenario

Limit LINSTOR over-provisioning by the pool's total capacity (rather than free capacity) - more relaxed because total capacity does not shrink as volumes fill.

## Steps

1. Set per-pool: `linstor storage-pool set-property <node> my_thin_pool MaxTotalCapacityOversubscriptionRatio 4`.
2. (Or controller-wide.) Verify with `query-size-info`.

## Expected outcome

- New volume size is capped at `ratio * total_capacity` regardless of how much has already been provisioned.
- LINSTOR admits or rejects placements by comparing free-cap-ratio and total-cap-ratio results and choosing the LOWER.

## Validations

- `linstor sp list-properties <node> my_thin_pool | grep MaxTotalCapacityOversubscriptionRatio` returns the value.
- `linstor rg query-size-info <rg>` shows `MaxVolumeSize` consistent with the formula.

## Doc reference

linstor-administration.adoc: `==== Configuring a maximum total capacity over provisioning ratio` (lines 3500-3528).

## Notes

- Default is 20.
- For overhead-heavy backends (ZFS, LVM-thin metadata), free capacity may be less than total - the two ratios can give very different answers.
- Test the interaction with `day2-over-provisioning-max-oversubscription.md`.
