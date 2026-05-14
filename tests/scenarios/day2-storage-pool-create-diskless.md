# day2-storage-pool-create-diskless

## Scenario

Create a diskless storage pool on a node so that the node can host diskless DRBD replicas (tiebreaker or initiator).

## Steps

1. Pick a satellite that needs to participate in DRBD without contributing storage.
2. Run `linstor storage-pool create diskless <node> diskless-pool`.
3. Verify with `linstor storage-pool list`.

## Expected outcome

- `linstor sp l --node <node>` shows a row with `Driver=DISKLESS`, free/total capacity reported as `0`.
- The node can be picked as a `--drbd-diskless` target or as an auto-placed tiebreaker.

## Validations

- `linstor sp l --node <node> --storage-pool diskless-pool` shows the entry.
- After `linstor resource create <node> <rd> --drbd-diskless` using this pool, `linstor r l --node <node>` shows the resource with `State=Diskless`.

## Doc reference

linstor-administration.adoc: `=== Storage providers` (lines 1997-2029) - Diskless entry; `=== DRBD clients` (lines 1686-1699).

## Notes

- Diskless pools cost nothing on disk but still consume LINSTOR ports for the DRBD device.
- Required for tiebreaker reconciler (`AutoAddQuorumTiebreaker`) and for k8s clusters where some nodes have no local storage.
