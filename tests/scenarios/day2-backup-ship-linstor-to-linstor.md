# day2-backup-ship-linstor-to-linstor

## Scenario

Ship a snapshot of a resource from one LINSTOR cluster to another (DR / replication scenario).

## Steps

1. On the target cluster, ensure a LINSTOR remote pointing back at the source exists (with `--cluster-id <source>`).
2. On the source cluster, ensure a LINSTOR remote `myTarget` points at the target controller.
3. From the source, ship: `linstor backup ship myTarget localRsc targetRsc`.
4. Verify on the target: `linstor resource list --resource targetRsc` shows the restored resource.

## Expected outcome

- A snapshot of `localRsc` is created on the source, transferred over the network, and restored as `targetRsc` on the target cluster.
- The target resource is immediately available on the target cluster (unless `--download-only` was passed or the target already has a deployed resource by that name).

## Validations

- On the target, `linstor rd l | grep targetRsc` returns the new RD.
- On the source, `linstor s l` lists the new snapshot used for shipping.
- The data hash on source and target matches.

## Doc reference

linstor-administration.adoc: `===== Shipping a snapshot of a resource to a LINSTOR remote` (lines 2684-2704).

## Notes

- Use `--source-node` / `--target-node` to pin sender / receiver; LINSTOR picks an alternative if the named node is unavailable.
- If `targetRsc` already exists on the target, the snapshot is shipped but NOT auto-restored - you must run `snapshot resource restore` manually.
- WAN shipping: see `day2-backup-ship-linstor-to-linstor-wan.md` for a WAN-specific interface.
