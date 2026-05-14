# day2-drbd-options-rd

## Scenario

Apply DRBD-level options (for example `--protocol C`, `--c-max-rate`, `--verify-alg`) to all resources spawned from a resource definition.

## Steps

1. Set the option: `linstor resource-definition drbd-options --protocol C backups`.
2. (Optional) Set additional ones: `linstor resource-definition drbd-options --verify-alg sha256 backups`.
3. Verify: `linstor resource-definition list-properties backups`.

## Expected outcome

- The `.res` files for every replica of `backups` include `net { protocol C; }`.
- Existing connections re-negotiate on next adjust.

## Validations

- `linstor rd list-properties backups | grep protocol` returns `C`.
- On a satellite, `grep -E 'protocol|verify-alg' /var/lib/linstor.d/backups.res` shows the new values.

## Doc reference

linstor-administration.adoc: `=== Setting DRBD options for LINSTOR objects` (lines 3300-3328).

## Notes

- Setting an option in `/etc/drbd.d/global_common.conf` directly is IGNORED by LINSTOR.
- Removing an option: `linstor rd drbd-options --unset-protocol backups`.
- Hierarchy: VD/RD options override RG; RG options override controller defaults.
