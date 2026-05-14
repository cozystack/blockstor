# day2-rd-create

## Scenario

Manually create a resource definition (without an RG) to set up storage for an ad-hoc resource.

## Steps

1. Create the RD: `linstor resource-definition create backups`.
2. Create a volume definition under it: `linstor volume-definition create backups 500G`.
3. (Optional) Set per-RD properties: `linstor resource-definition drbd-options --protocol C backups`.

## Expected outcome

- A new RD `backups` with one 500 GiB VD exists.
- The RD belongs to `DfltRscGrp` unless `--resource-group` was passed.

## Validations

- `linstor rd l | grep backups` returns one row.
- `linstor vd l --resource-definition backups` shows a 500 GiB volume with number 0.
- `linstor rd list-properties backups | grep protocol` shows the DRBD protocol if set.

## Doc reference

linstor-administration.adoc: `=== Creating and deploying resources and volumes` (lines 823-862).

## Notes

- Bare RDs without resources just reserve a name + ports - no storage is allocated until `resource create` is run.
- Production setups should use RGs (`day2-rg-spawn-resource.md`) instead of bare RDs for inheritance of placement constraints.
