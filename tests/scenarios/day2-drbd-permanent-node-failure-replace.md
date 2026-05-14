# day2-drbd-permanent-node-failure-replace

## Scenario

Replace a permanently destroyed satellite with a fresh node of equivalent (or better) hardware, then rejoin it to the cluster.

## Steps

1. Confirm the dead node is unrecoverable; remove its entry: `linstor node lost <node>` (see `day2-node-lost.md`).
2. Install new hardware with disk capacity >= the dead node's. (Smaller disks are not supported.)
3. Install the same base OS, DRBD packages, and LINSTOR satellite.
4. Re-register the new node with LINSTOR: `linstor node create <newname> <ip>` (see `day2-node-add.md`).
5. Recreate the storage pool(s) on the new node: `linstor sp c <driver> <newname> <pool> <backing>`.
6. Let autoplace / `BalanceResources` fill in missing replicas, or manually run `resource create --auto-place +1 <rsc>` for each affected RD.
7. DRBD will sync replicas onto the new node from existing UpToDate peers.

## Expected outcome

- The cluster regains its full replica count.
- DRBD performs full resyncs onto the new node (no metadata is preserved because the hardware is fresh).

## Validations

- `linstor node list | grep <newname>` returns `Online`.
- All affected RDs show their replica count restored after autoplace + sync.
- DRBD goes through `Inconsistent` -> `SyncTarget` -> `UpToDate` on the new node.

## Doc reference

drbd-troubleshooting.adoc: `==== Dealing with permanent node failure` (lines 192-216) and linstor-administration.adoc: `==== Auto-evict` (lines 4281-4348).

## Notes

- Replacing with smaller-capacity disks is not supported; DRBD refuses to connect.
- If `AutoEvictAllowEviction=true` and the dead node was offline long enough, LINSTOR may have already auto-evicted it. In that case skip step 1.
- Cross-link: `day2-node-lost.md` (manual eviction), `day2-auto-evict-tuning.md` (configure auto-evict).
