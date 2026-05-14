# day2-resource-toggle-disk-remove

## Scenario

Convert a diskful replica to diskless (drop the backing storage but keep the DRBD client active on the node).

## Steps

1. Confirm the resource has at least one OTHER diskful replica still UpToDate.
2. Issue the toggle: `linstor resource toggle-disk alpha backups --diskless`.
3. Verify: `linstor r l --node alpha --resource backups` reports `State=Diskless`.

## Expected outcome

- The backing LV / dataset on `alpha` is removed.
- DRBD on `alpha` detaches from local storage and operates as a diskless client.
- The remaining peers continue to serve I/O.

## Validations

- After: `linstor r l --node alpha --resource backups | grep State` shows `Diskless`.
- On the satellite, the backing LV / dataset no longer exists.
- I/O on `/dev/drbdN` on `alpha` still works (reads/writes go over the network).

## Doc reference

linstor-administration.adoc: `=== Toggling a resource between diskful and diskless` (lines 3608-3629).

## Notes

- DOES NOT work if `alpha` is the last diskful replica - DRBD would have no peer to read from. LINSTOR refuses.
- Stuck toggle recovery: see `day2-resource-toggle-disk-stuck.md`.
