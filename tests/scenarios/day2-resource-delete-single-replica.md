# day2-resource-delete-single-replica

## Scenario

Remove a single replica of a resource from a specific node without deleting the resource definition.

## Steps

1. Confirm the resource has more than one replica: `linstor r l --resource backups`.
2. Issue the delete: `linstor resource delete worker-3 backups`.
3. Wait for the satellite finalizer to remove the backing LV / dataset.
4. Confirm the replica is gone.

## Expected outcome

- The replica on `worker-3` is removed; remaining replicas keep their data.
- LINSTOR may emit INFO messages about quorum DRBD options being unset if the replica count drops below quorum threshold.

## Validations

- `linstor r l --node worker-3 --resource backups` returns empty.
- `linstor r l --resource backups` shows N-1 replicas where N was the previous count.
- On `worker-3`, the backing LV / dataset is removed.

## Doc reference

linstor-administration.adoc: `==== Deleting a resource` (lines 1368-1401).

## Notes

- In a 3-node cluster, removing one replica drops quorum to 2-of-2; LINSTOR prints INFO messages such as `Resource-definition property 'DrbdOptions/Resource/quorum' was removed as there are not enough resources for quorum`.
- Snapshots are NOT affected; they persist on the surviving replicas.
- If you want LINSTOR to add a replacement replica (rebalance), keep `--place-count` higher than the new replica count; the periodic balance task will fill in.
