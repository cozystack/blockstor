# day2-multi-volume-rd

## Scenario

Create a resource definition with two volumes (DRBD consistency group) so writes across both volumes are replicated in chronological order.

## Steps

1. Create the RD: `linstor resource-definition create backups`.
2. Add two volume definitions:
```
linstor volume-definition create backups 500G
linstor volume-definition create backups 100G
```
3. Deploy on the desired nodes (manual placement or autoplace).

## Expected outcome

- Each replica has TWO backing volumes (`backups_00000` and `backups_00001`).
- DRBD treats them as a consistency group; replication order is preserved across both.

## Validations

- `linstor vd l --resource-definition backups` shows two rows, indices 0 and 1.
- On the satellite, two backing LVs / datasets exist.
- After heavy writes, both volumes' content on every replica is consistent at every recovery point.

## Doc reference

linstor-administration.adoc: `=== DRBD consistency groups (multiple volumes within a resource)` (lines 1700-1719).

## Notes

- This is the only way to get cross-volume write ordering in LINSTOR; multiple separate RDs do NOT share a consistency group.
- See `day2-multi-volume-rd-per-pool.md` for routing each volume to a different storage pool.
