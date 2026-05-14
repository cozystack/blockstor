# day2-resource-create-drbd-diskless

## Scenario

Create a permanently diskless DRBD client on a node - the node accesses data over the network from peers that have disks.

## Steps

1. Ensure the RD has at least one diskful replica on a peer (otherwise diskless has nothing to read).
2. On the target node, create a diskless replica: `linstor resource create delta backups --drbd-diskless`.
3. Confirm: `linstor resource list --resource backups`.

## Expected outcome

- A new replica appears on `delta` with `State=Diskless`.
- The DRBD device `/dev/drbdN` is usable for I/O; reads/writes traverse the network to a diskful peer.

## Validations

- `linstor r l --node delta --resource backups` shows `State=Diskless`.
- On `delta`, `drbdadm status backups` shows `disk:Diskless` and a peer with `peer-disk:UpToDate`.
- I/O on `/dev/drbdN` succeeds.

## Doc reference

linstor-administration.adoc: `=== DRBD clients` (lines 1686-1699).

## Notes

- `--diskless` is deprecated in favor of `--drbd-diskless` / `--nvme-initiator`.
- A diskless replica still consumes a TCP port but no on-disk storage.
- Auto-diskful (`day2-rd-auto-diskful.md`) can convert this node to diskful if it spends too long in the Primary role.
