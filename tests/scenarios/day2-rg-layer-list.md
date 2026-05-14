# day2-rg-layer-list

## Scenario

Constrain a resource group to spawn resources with a specific DRBD layer stack (for example `drbd,luks` for transparently encrypted replicated volumes).

## Steps

1. Create the RG with layers in top-down order: `linstor resource-group create my_luks_rg --place-count 3 --layer-list drbd,luks`.
2. Add a volume group: `linstor volume-group create my_luks_rg`.
3. Spawn a resource: `linstor resource-group spawn my_luks_rg my_secret 20G`.
4. Confirm the layer stack: `linstor resource list --resource my_secret` shows `Layers=DRBD,LUKS,STORAGE`.

## Expected outcome

- Spawned resources have DRBD on top of LUKS on top of the storage layer.
- A LUKS master passphrase must already be set on the controller; otherwise creation fails.

## Validations

- `linstor r l --resource my_secret` shows `Layers` column containing `DRBD,LUKS,STORAGE`.
- On the satellite, `lsblk` shows a `crypt` device under the LV.

## Doc reference

linstor-administration.adoc: `===== Constraining automatic resource placement by LINSTOR layers or storage pool providers` (lines 1201-1230) and layer table at lines 1819-1831.

## Notes

- Layer order is "top-down"; left layer wraps the right. `STORAGE` is implicit at the bottom for diskful resources.
- Layer combinations are validated; for example `drbd,cache,nvme` is rejected (NVMe under DRBD is illegal per the table).
- For LUKS see `day2-encryption-create-passphrase.md`.
