# day2-rg-create

## Scenario

Create a LINSTOR resource group as a reusable template for resources of similar shape (placement count, storage pool, DRBD options).

## Steps

1. Create the resource group with placement count and storage pool: `linstor resource-group create my_ssd_group --storage-pool pool_ssd --place-count 2`.
2. Create the matching volume group inside it: `linstor volume-group create my_ssd_group`.
3. Verify with `linstor resource-group list` and `linstor volume-group list`.

## Expected outcome

- The resource group exists with `--place-count 2` and `--storage-pool pool_ssd` recorded as defaults.
- A volume group is associated, ready for future spawned resources.

## Validations

- `linstor rg l | grep my_ssd_group` shows the RG with the correct `SelectFilter` values.
- `linstor vg l | grep my_ssd_group` returns one row.

## Doc reference

linstor-administration.adoc: `=== Using resource groups to deploy LINSTOR provisioned volumes` (lines 744-808).

## Notes

- `DfltRscGrp` is always present; every RD that isn't explicitly assigned a group lives there.
- Resource groups can contain DRBD options (`linstor resource-group drbd-options --verify-alg crc32c my_ssd_group`) that are inherited by every spawned resource.
