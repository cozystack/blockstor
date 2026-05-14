# day2-storage-pool-mixing

## Scenario

Allow mixing storage-pool drivers (for example LVM thick on one node, ZFS thin on another) for a single LINSTOR resource, typically for an online migration between backends.

## Steps

1. Verify prerequisites: LINSTOR controller >= 1.27.0, DRBD >= 9.2.7 (or 9.1.18) on all satellites.
2. Enable mixing on the controller (or RG / RD scope): `linstor controller set-property AllowMixingStoragePoolDriver true`.
3. Create or modify a resource so its replicas land on differently-typed pools, for example: `linstor resource create alpha myrsc --storage-pool lvm_pool` then `linstor resource create bravo myrsc --storage-pool zfsthin_pool`.
4. Verify replication starts.

## Expected outcome

- The resource is created on both nodes; DRBD syncs successfully.
- Because LVM extent size differs from ZFS (4 MiB vs 16 KiB), LINSTOR treats the resource as thick - thin-only features (snapshots driven by the backend) may be limited.

## Validations

- `linstor controller list-properties | grep AllowMixingStoragePoolDriver` shows `true`.
- `linstor r l --resource myrsc` shows replicas on both nodes; `State` settles to `UpToDate`.
- A subsequent `linstor snapshot create myrsc s1` may succeed or fail depending on the mix - test both and document.

## Doc reference

linstor-administration.adoc: `==== Mixing storage pools of different storage providers` (lines 2030-2069) plus prerequisites at lines 2038-2047.

## Notes

- Mixing `zfs` + `zfsthin` is NOT considered mixing (same extent size, same initial-sync strategy) - no `AllowMixingStoragePoolDriver` needed.
- Setting `AllowMixingStoragePoolDriver=true` on the controller applies cluster-wide; override per-RG / per-RD if you want it disabled selectively.
- Mixed resources are always treated as thick regardless of the backing pool type - lose space savings.
