# day2-resource-create-autoplace-plus-one

## Scenario

Add exactly one additional replica to an existing resource, regardless of the resource group's configured `--place-count`.

## Steps

1. Check current replica count: `linstor resource list --resource testResource`.
2. Make sure a candidate node satisfies any active constraints (`--replicas-on-same`, `--replicas-on-different`); set aux properties as needed.
3. Run `linstor resource create --auto-place +1 testResource`.
4. Verify the new replica is `UpToDate`.

## Expected outcome

- A single new replica appears on a constraint-eligible node.
- The RG's `--place-count` is NOT updated - subsequent spawns still use the old count.

## Validations

- `linstor r l --resource testResource` shows one more row than before.
- The new replica reaches `UpToDate`.

## Doc reference

linstor-administration.adoc: `====== Using auto-place to extend existing resource deployments` (lines 1246-1278).

## Notes

- `+1` is only valid for `resource create`, NOT `resource-group create`.
- If no candidate node satisfies the constraints, the command fails with `Not enough available nodes` and no replica is created.
- To permanently increase the replica count, modify the RG's `--place-count` first (see `day2-rg-modify-place-count.md`).
