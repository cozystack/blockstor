# day2-rg-providers

## Scenario

Restrict autoplacement to a specific subset of storage-provider types (for example only LVM or LVM-thin pools, never ZFS).

## Steps

1. Create the RG with provider filter: `linstor resource-group create lvm_only_rg --place-count 3 --providers LVM,LVM_THIN`.
2. Spawn a resource and verify all replicas live on matching pools.

## Expected outcome

- The autoplacer only considers `LVM` and `LVM_THIN` pools when fulfilling `--place-count`.
- Even if a ZFS pool has more free space, it is not selected.

## Validations

- For each replica, `linstor r l --resource <new-rsc>` plus `linstor sp l --node <node>` confirms the pool's `Driver` is in `LVM` or `LVM_THIN`.
- Setting `--providers ZFS` on a cluster with only LVM pools yields `Not enough available nodes`.

## Doc reference

linstor-administration.adoc: `===== Constraining automatic resource placement by LINSTOR layers or storage pool providers` (lines 1201-1230).

## Notes

- The CSV list does NOT imply priority - LINSTOR still uses free-space / throughput / count strategies to rank candidates within the allowed set.
- Combine with `--layer-list` to lock both shape and provider, for example `--layer-list drbd,storage --providers LVM_THIN`.
