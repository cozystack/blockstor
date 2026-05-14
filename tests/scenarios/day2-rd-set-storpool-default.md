# day2-rd-set-storpool-default

## Scenario

Set the default storage pool for a resource definition so subsequent `resource create` without `--storage-pool` uses that pool.

## Steps

1. Set on the RD: `linstor resource-definition set-property <rd> StorPoolName pool_ssd`.
2. (Optional) Set per-volume on VD if multi-volume: `linstor vd set-property <rd> 0 StorPoolName pool_ssd`.
3. Create new resources without `--storage-pool` and verify the chosen pool.

## Expected outcome

- New resources for this RD land in `pool_ssd` by default.
- Property lookup order (highest priority first): volume-definition > resource > resource-definition > node.

## Validations

- `linstor rd list-properties <rd> | grep StorPoolName` returns `pool_ssd`.
- `linstor r l --resource <rd>` shows replicas in `pool_ssd`.

## Doc reference

linstor-administration.adoc: lines 1742-1755 (property fallback order for StorPoolName).

## Notes

- If none of the scopes set `StorPoolName`, LINSTOR falls back to the literal `DfltStorPool` (which usually doesn't exist on satellites - error).
- Useful when an autoplaced RG doesn't pin a storage pool but you want this particular RD to bind to a specific one.
