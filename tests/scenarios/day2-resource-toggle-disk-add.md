# day2-resource-toggle-disk-add

## Scenario

Convert a diskless replica to diskful (provision backing storage on the node and let DRBD sync data into it).

## Steps

1. Identify a diskless replica: `linstor r l --resource backups` shows `State=Diskless` on some node.
2. Toggle that replica to diskful: `linstor resource toggle-disk alpha backups --storage-pool pool_ssd`.
3. Monitor sync: `linstor r l --resource backups` and DRBD on the node.

## Expected outcome

- LINSTOR creates a backing LV / dataset on `alpha`.
- DRBD begins a sync from a peer; `State` transitions through `Inconsistent` -> `SyncTarget` -> `UpToDate`.
- The replica is no longer diskless.

## Validations

- Before: `linstor r l --node alpha --resource backups | grep State` shows `Diskless`.
- After (post-sync): same query shows `UpToDate`.
- On the satellite, the backing LV / dataset exists.

## Doc reference

linstor-administration.adoc: `=== Toggling a resource between diskful and diskless` (lines 3608-3629).

## Notes

- Toggle-disk can become stuck on backend errors. Recovery: re-issue the same command (retry) or issue the inverse to cancel - see `day2-resource-toggle-disk-stuck.md`.
- Inverse operation: `linstor resource toggle-disk alpha backups --diskless`.
