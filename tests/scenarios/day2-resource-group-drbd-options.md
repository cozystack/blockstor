# day2-resource-group-drbd-options

## Scenario

Set DRBD options at the resource-group level so every resource spawned from the group inherits them.

## Steps

1. Set a `verify-alg` on the RG: `linstor resource-group drbd-options --verify-alg crc32c my_ssd_group`.
2. Set a network protocol: `linstor resource-group drbd-options --protocol C my_ssd_group`.
3. Spawn a resource and confirm the options propagate.

## Expected outcome

- New resources spawned from `my_ssd_group` include `verify-alg crc32c` and `protocol C` in their `.res` files.

## Validations

- `linstor rg list-properties my_ssd_group | grep -E 'verify-alg|protocol'` returns the values.
- A spawned resource's `.res` shows the options.

## Doc reference

linstor-administration.adoc: `=== Using resource groups to deploy LINSTOR provisioned volumes` (lines 768-808).

## Notes

- Hierarchy: RD options override RG options.
- Removing an RG option: `linstor rg drbd-options --unset-verify-alg my_ssd_group`.
