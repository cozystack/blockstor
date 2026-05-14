# day2-drbd-options-unset

## Scenario

Remove a previously-set DRBD option from a LINSTOR object, returning it to its default value.

## Steps

1. Identify the property to remove: `linstor <object> list-properties <name>`.
2. Unset by prefixing `--unset-`: `linstor resource-definition drbd-options --unset-protocol backups`.
3. (For peer-options) `linstor resource-connection drbd-peer-options --unset-max-buffers node-0 node-1 backups`.
4. Verify removal.

## Expected outcome

- The property is removed from the LINSTOR DB.
- Next adjust returns the option to its DRBD default value.

## Validations

- `linstor <object> list-properties <name>` no longer lists the unset key.
- On the satellite, the option is absent from `<rd>.res`.

## Doc reference

linstor-administration.adoc: `==== Removing DRBD options from LINSTOR objects` (lines 3414-3434).

## Notes

- Default values are listed in `linstor <object> drbd-options --help`.
- The same `unset-` syntax works for both `drbd-options` and `drbd-peer-options`.
