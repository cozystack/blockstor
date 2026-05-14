# day2-storage-pool-shared

## Scenario

Configure a shared LVM2 storage pool (same VG accessible from multiple nodes, for example via a SAN) and let LINSTOR coordinate access.

## Steps

1. Verify both nodes can see the same VG (`vgs` returns identical UUID on each host).
2. On each node, register the LINSTOR pool with `--shared-space <vg-uuid>` and `--external-locking`:
```
linstor storage-pool create lvm --external-locking \
  --shared-space O1btSy-UO1n-lOAo-4umW-ETZM-sxQD-qT4V87 \
  alpha pool_ssd shared_vg_ssd
linstor storage-pool create lvm --external-locking \
  --shared-space O1btSy-UO1n-lOAo-4umW-ETZM-sxQD-qT4V87 \
  bravo pool_ssd shared_vg_ssd
```
3. Verify pools and shared-space ID: `linstor storage-pool list`.

## Expected outcome

- The pool exists on both nodes with the same `--shared-space` UUID.
- Resources created in `pool_ssd` can be present on either node without LINSTOR duplicating the underlying LV.

## Validations

- `linstor sp l --storage-pool pool_ssd` shows two rows with identical `SharedSpace` column.
- `vgs` UUIDs match on both hosts.
- Concurrent `lvcreate` from outside LINSTOR is forbidden - any external LVM command on the shared VG can corrupt metadata; CI test should attempt one and assert the warning is documented (do not actually corrupt).

## Doc reference

linstor-administration.adoc: `==== Sharing storage pools with multiple nodes` (lines 660-697).

## Notes

- WARNING: never run interactive `lvcreate` / `lvextend` / `lvremove` on the shared VG. LVM auto-activation at boot must be disabled.
- `--external-locking` tells LINSTOR to rely on LVM's own lock manager (sanlock / lvmlockd) rather than its internal locks.
- Only LVM2 storage pools support shared-space; ZFS does not.
