# day2-backup-ship-linstor-to-linstor-wan

## Scenario

Ship a snapshot to a remote LINSTOR cluster over a WAN, routing the data through a specific (WAN-facing) NIC.

## Steps

1. Create a node net-interface on the source's nodes that maps to the WAN IP of the remote controller: `linstor node interface create <node> wan-nic <remote-wan-ip>` (use the source node's WAN IP for the local interface).
2. Ensure the LINSTOR remote on the source exists for the target.
3. Ship using `--target-net-if`: `linstor backup ship myTarget localRsc targetRsc --target-net-if wan-nic`.

## Expected outcome

- DRBD replication / shipping traffic goes through the WAN NIC, not the management or default NIC.
- The target cluster receives and restores the snapshot.

## Validations

- During shipping, `tcpdump -n -i <wan-nic-dev>` on the source shows DRBD traffic.
- `linstor node interface list <node>` shows the WAN nic with the expected IP.
- The shipped resource appears on the target with matching content.

## Doc reference

linstor-administration.adoc: `===== Shipping a snapshot of a resource to a LINSTOR remote over a WAN` (lines 2705-2723).

## Notes

- The interface name passed to `--target-net-if` must exist on every candidate sender; LINSTOR may pick a different sender if the named one isn't available.
- For replication-only traffic isolation (not just shipping), see `day2-node-prefnic.md` and `day2-resource-connection-multipath.md`.
