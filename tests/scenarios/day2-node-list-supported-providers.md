# day2-node-list-supported-providers

## Scenario

Determine which storage providers and storage layers each satellite node supports, for example to debug why an autoplace skipped a node.

## Steps

1. Run `linstor node info`.
2. Inspect the two tables: providers (Diskless, LVM, LVMThin, ZFS/Thin, File/Thin, SPDK, Storage Spaces, ...) and layers (DRBD, LUKS, NVMe, Cache, BCache, WriteCache, Storage).

## Expected outcome

- Output contains two tables, one per node row, with `+` for "supported" and `-` for "not supported".
- A node missing `drbd-utils` or the DRBD kernel module reports `-` under the DRBD column.
- A node without `cryptsetup` reports `-` under LUKS.

## Validations

- `linstor node info | grep <node>` returns one row in each table.
- The provider table contains at minimum the `Diskless` column.
- For every diskful node, `Diskless=+`, `LVM=+` or `ZFS=+`, and `DRBD=+`.

## Doc reference

linstor-administration.adoc: `==== Listing supported storage providers and storage layers` (lines 560-602).

## Notes

- This command is the fastest way to confirm DRBD or LUKS prerequisites are installed on a satellite before creating a storage pool.
- Run after upgrading the satellite OS or kernel - module availability can change with a kernel update.
