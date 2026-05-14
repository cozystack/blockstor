# day2-rg-spawn-resource

## Scenario

Spawn a concrete resource (with its volumes) from an existing resource group - the recommended way to create LINSTOR storage in production.

## Steps

1. Confirm the RG exists with the desired settings: `linstor resource-group list`.
2. Spawn: `linstor resource-group spawn-resources my_ssd_group my_ssd_res 20G`.
3. Watch `linstor resource list --resource my_ssd_res` until all replicas reach `UpToDate`.

## Expected outcome

- A new RD `my_ssd_res` and one VD of 20 GiB are created.
- LINSTOR autoplaces the resource on `--place-count` nodes from the configured storage pool.
- All replicas converge to `UpToDate` once the initial sync finishes.

## Validations

- `linstor rd l | grep my_ssd_res` shows one row.
- `linstor vd l --resource-definition my_ssd_res` shows the 20 GiB volume.
- `linstor r l --resource my_ssd_res` shows the expected number of rows, all `UpToDate`.

## Doc reference

linstor-administration.adoc: `=== Using resource groups to deploy LINSTOR provisioned volumes` (lines 744-808).

## Notes

- `spawn-resources` is the spelled-out alias for `spawn`. Both work.
- Settings defined on the RG (DRBD options, layer list, FS type) propagate to the spawned RD/VD unless explicitly overridden later.
