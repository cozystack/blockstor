# day2-resource-create-manual

## Scenario

Manually place a resource on specific nodes (instead of letting the autoplacer choose). Useful for surgical placement or when you need explicit control over which storage pool is used per node.

## Steps

1. Ensure the RD and VD exist: `linstor rd c backups`, `linstor vd c backups 500G`.
2. Create the resource on each desired node, choosing the storage pool: `linstor resource create alpha backups --storage-pool pool_hdd` (repeat for bravo, charlie).
3. Wait for sync to complete.

## Expected outcome

- One DRBD replica per `resource create` invocation.
- Replicas reach `UpToDate` after the initial sync.

## Validations

- `linstor r l --resource backups` shows one row per node specified.
- All rows reach `State=UpToDate`.

## Doc reference

linstor-administration.adoc: `==== Manually placing resources` (lines 864-874).

## Notes

- Manual placement does NOT count as "autoplaced"; `BalanceResources` will leave manually placed resources alone unless the property allows.
- For diskless placement, use `--drbd-diskless` instead of `--storage-pool` - see `day2-resource-create-drbd-diskless.md`.
