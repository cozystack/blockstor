# day2-writecache-layer

## Scenario

Create a LINSTOR resource that uses the DM-Writecache layer (fast cache device + slower data device).

## Steps

1. Confirm both pools exist on the target node: one for data (e.g. `lvmpool`) and one for cache (e.g. `pmempool`).
2. Create the RD and VD:
```
linstor resource-definition create r1
linstor volume-definition create r1 100G
```
3. Configure the cache: `linstor volume-definition set-property r1 0 Writecache/PoolName pmempool` and `linstor volume-definition set-property r1 0 Writecache/Size 1%`.
4. Create the resource with the `WRITECACHE` layer: `linstor resource create node1 r1 --storage-pool lvmpool --layer-list WRITECACHE,STORAGE`.

## Expected outcome

- LINSTOR creates the data LV in `lvmpool` and the cache LV in `pmempool` (1% of the data LV size).
- A DM-Writecache device is set up on top.
- I/O is acknowledged once it hits the cache device.

## Validations

- On the satellite, `dmsetup table | grep writecache` shows the device.
- `lsblk` shows the cache device under the data LV.
- `linstor r l --resource r1` shows `Layers=WRITECACHE,STORAGE`.

## Doc reference

linstor-administration.adoc: `==== Writecache layer` (lines 1908-1942).

## Notes

- Properties `Writecache/PoolName` and `Writecache/Size` MUST be set; LINSTOR refuses otherwise.
- For per-node defaults, set the properties at controller level.
- Cross-link: `day2-cache-layer.md` for the cache layer (separate data+cache+metadata).
- Cozystack/blockstor explicitly DOES NOT support WRITECACHE layer.
