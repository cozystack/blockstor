# day2-external-drbd-metadata

## Scenario

Direct LINSTOR to store DRBD metadata for new resources on a separate storage pool (typical: fast NVMe metadata, slower HDD/SSD data).

## Steps

1. Verify the metadata pool exists on each satellite: `linstor sp l`.
2. Set the metadata-pool property at the desired scope (e.g. node level): `linstor node set-property <node> StorPoolNameDrbdMeta meta_pool`.
3. Create a NEW resource - the property only affects future creations.
4. Inspect the satellite to confirm metadata lives on the chosen pool.

## Expected outcome

- For new resources, LINSTOR creates two LVs/datasets: one for data (in the resource's main pool) and one for DRBD metadata (in `meta_pool`).
- Existing resources are NOT migrated; the property only affects future creates.

## Validations

- `linstor node list-properties <node> | grep StorPoolNameDrbdMeta` returns the configured pool.
- On the satellite after a fresh resource create, two backing volumes exist (data + meta).
- `drbdadm dump-md <rd>` confirms the metadata location.

## Doc reference

linstor-administration.adoc: `=== Using external DRBD metadata` (lines 4462-4534).

## Notes

- Priority (low to high): node, resource-group, resource-definition, resource, volume-group, volume-definition.
- Removing the property: `linstor <object> set-property StorPoolNameDrbdMeta` with no value (deletes).
- Mixing external and internal metadata in the same resource (across replicas) is supported but unusual.
