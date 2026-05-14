# day2-cache-layer

## Scenario

Create a LINSTOR resource that uses the DM-Cache layer (data + cache + metadata devices).

## Steps

1. Confirm storage pools: `lvmpool` (data), `pmempool` (cache, and optionally metadata).
2. RD and VD: `linstor resource-definition create r1` and `linstor volume-definition create r1 100G`.
3. Configure properties:
```
linstor volume-definition set-property r1 0 Cache/CachePool pmempool
linstor volume-definition set-property r1 0 Cache/Cachesize 1%
```
4. Create with the CACHE layer: `linstor resource create node1 r1 --storage-pool lvmpool --layer-list CACHE,STORAGE`.

## Expected outcome

- DM-Cache device is set up; data on `lvmpool`, cache + metadata on `pmempool`.

## Validations

- On the satellite, `dmsetup table | grep cache` shows the device.
- `linstor r l --resource r1` shows `Layers=CACHE,STORAGE`.

## Doc reference

linstor-administration.adoc: `==== Cache layer` (lines 1944-1976).

## Notes

- `Cache/MetaPool` defaults to `Cache/CachePool` if unset.
- Cross-link: `day2-writecache-layer.md` for the simpler write-only cache.
- Cozystack/blockstor explicitly DOES NOT support CACHE layer.
