# day2-snapshot-restore

## Scenario

Restore a snapshot into a new resource definition (the original RD may still exist or may have been deleted; either way works).

## Steps

1. Create the target RD: `linstor resource-definition create resource2`.
2. Restore the volume definitions: `linstor snapshot volume-definition restore --from-resource resource1 --from-snapshot snap1 --to-resource resource2`.
3. (Optional) Tune the new RD/VD (e.g. set properties).
4. Restore the resources: `linstor snapshot resource restore --from-resource resource1 --from-snapshot snap1 --to-resource resource2`.

## Expected outcome

- A new RD `resource2` with the same VD layout as `resource1` at snapshot time is created.
- Replicas are placed on the nodes where the snapshot exists (or explicitly chosen via additional CLI options).
- The new resource is independently usable; modifications do not affect the original.

## Validations

- `linstor rd l | grep resource2` returns the new RD.
- `linstor vd l --resource-definition resource2` matches the volume layout of the snapshot.
- `linstor r l --resource resource2` shows replicas in `UpToDate`.

## Doc reference

linstor-administration.adoc: `==== Restoring a snapshot` (lines 2473-2497).

## Notes

- Defaults to restoring on every node where the snapshot exists; pass node names to `snapshot resource restore` to narrow.
- This is the SAFE alternative to `snapshot rollback` when the resource is currently in use.
