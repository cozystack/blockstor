# day2-multi-volume-rd-per-pool

## Scenario

In a multi-volume RD, route each volume to a different storage pool (e.g. volume 0 on HDD pool, volume 1 on SSD pool).

## Steps

1. Create RD and VDs as in `day2-multi-volume-rd.md`:
```
linstor resource-definition create backups
linstor volume-definition create backups 500G
linstor volume-definition create backups 100G
```
2. Set per-volume `StorPoolName`:
```
linstor volume-definition set-property backups 0 StorPoolName pool_hdd
linstor volume-definition set-property backups 1 StorPoolName pool_ssd
```
3. Create the resources (no `--storage-pool` needed; per-VD property wins):
```
linstor resource create alpha backups
linstor resource create bravo backups
linstor resource create charlie backups
```

## Expected outcome

- Volume 0 lives in `pool_hdd` on each replica.
- Volume 1 lives in `pool_ssd` on each replica.
- DRBD still treats the two volumes as a consistency group.

## Validations

- On each satellite, `lvs` / `zfs list` shows volume 0 in the HDD pool's backing VG/zpool and volume 1 in the SSD pool's.
- `linstor v l --resource backups` shows different `StoragePool` per row.

## Doc reference

linstor-administration.adoc: `=== Placing volumes of one resource in different storage pools` (lines 1721-1755).

## Notes

- Property lookup order for fallback: VolumeDefinition > Resource > ResourceDefinition > Node. If none set, controller falls back to literal `DfltStorPool` (which usually doesn't exist - causing creation to fail with a helpful message).
- Useful for tiered storage: small metadata vol on SSD, large data vol on HDD.
