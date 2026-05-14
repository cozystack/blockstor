# day2-remote-create-linstor

## Scenario

Register a remote LINSTOR cluster (or self) as a target for snapshot shipping (LINSTOR-to-LINSTOR replication).

## Steps

1. Ensure encryption is enabled in BOTH clusters.
2. On the target cluster, get its cluster ID: `linstor controller list-properties | grep -i cluster`.
3. On the source cluster, create the remote pointing at the target controller IP: `linstor remote create linstor myTarget 192.168.0.15`.
4. On the target cluster, create the inverse remote with the source's cluster ID:
```
linstor remote create linstor --cluster-id <SOURCE_CLUSTER_ID> mySource <source-controller-ip-or-hostname>
```

## Expected outcome

- Each side has a remote pointing to the other (the target needs the source's cluster ID to accept incoming shipments).
- `linstor backup ship <remote> <localRsc> <targetRsc>` now works from source to target.

## Validations

- On each cluster, `linstor remote list` shows the new remote.
- A test ship (`day2-backup-ship-linstor-to-linstor.md`) completes successfully.

## Doc reference

linstor-administration.adoc: `===== Creating a LINSTOR remote` (lines 2581-2598) and `===== Creating a remote for a LINSTOR target cluster` (lines 2659-2683).

## Notes

- Without the target-side `--cluster-id <source>`, the ship fails with `Unknown Cluster`.
- LUKS-layered resources require a `--passphrase` to be set when creating the remote (see `day2-remote-create-linstor-luks.md`).
- A "self" remote can be created with localhost: `linstor remote create linstor self 127.0.0.1 --cluster-id <LINSTOR_CLUSTER_ID>` (used by `day2-snapshot-ship-self.md`).
