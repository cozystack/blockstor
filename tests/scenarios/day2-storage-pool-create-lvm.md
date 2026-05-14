# day2-storage-pool-create-lvm

## Scenario

Create a thick-provisioned LVM storage pool on one or more satellites so that LINSTOR can carve volumes from it.

## Steps

1. On each contributing satellite, ensure an LVM volume group exists: `vgcreate vg_ssd /dev/nvme0n1 [...]`.
2. Make sure `/etc/lvm/lvm.conf` has `global_filter = [ "r|^/dev/drbd|", "r|^/dev/mapper/[lL]instor|" ]` to keep LVM from scanning DRBD devices.
3. Register the storage pool with LINSTOR on each node: `linstor storage-pool create lvm <node> pool_ssd vg_ssd`.
4. List pools: `linstor storage-pool list` (or `linstor sp l`).

## Expected outcome

- `linstor sp l` shows one row per node with `Driver=LVM`, `PoolName=vg_ssd`, and a `FreeCapacity`/`TotalCapacity` that matches `vgs` output on the host.
- Subsequent `resource create` / `resource-group spawn` against this pool succeeds.

## Validations

- For every node, `linstor sp l --node <node> | grep pool_ssd` returns one row.
- `FreeCapacity` is greater than zero and within a few MiB of the VG free size.
- On the satellite, `grep global_filter /etc/lvm/lvm.conf` shows the recommended filter is in place.

## Doc reference

linstor-administration.adoc: `==== Creating storage pools` (lines 610-651).

## Notes

- Skipping the `global_filter` change can cause high CPU load or LVM commands hanging on hosts that also use DRBD.
- The LINSTOR storage pool name (`pool_ssd`) is independent of the LVM VG name; use the same LINSTOR name across nodes to enable resource placement.
