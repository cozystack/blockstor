# day2-snapshot-ship-self

## Scenario

Ship a snapshot from one node to another WITHIN the same LINSTOR cluster (via the recommended `backup ship` path, using a self-remote).

## Steps

1. Get the cluster ID: `linstor controller list-properties | grep -i cluster`.
2. Create a "self" remote pointing at the local controller:
```
linstor remote create linstor self 127.0.0.1 --cluster-id <LINSTOR_CLUSTER_ID>
```
3. Ensure both source and target nodes have the resource deployed.
4. Deactivate the target replica: `linstor resource deactivate <nodeTarget> resource1`.
5. Ship: `linstor backup ship self localRsc targetRsc`.

## Expected outcome

- A snapshot of `localRsc` is created, transferred, and applied as `targetRsc` on the target node.
- DRBD-layered targets are NOT reactivatable after deactivate; to use the new data, restore into a new RD (`day2-snapshot-restore.md`).

## Validations

- `linstor s l` shows the snapshot used.
- `linstor r l --resource targetRsc` shows the new replica.

## Doc reference

linstor-administration.adoc: `==== Snapshot shipping within a single LINSTOR cluster` (lines 2725-2737) and `===== Shipping a snapshot in the same cluster` (lines 2883-2918).

## Notes

- The legacy `snapshot ship` command is deprecated; use `backup ship` with a self-remote.
- WARNING for DRBD-layered resources: deactivate is permanent for that replica - plan to restore into a new RD.
