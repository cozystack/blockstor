# day2-drbd-peer-options-resource-connection

## Scenario

Tune a DRBD net-section option (for example `max-buffers`) for the connection between two specific nodes of a resource.

## Steps

1. Set the option: `linstor resource drbd-peer-options --max-buffers 8192 node-0 node-1 backups`.
2. Verify: `linstor resource-connection list-properties node-0 node-1 backups`.

## Expected outcome

- The `connection { net { max-buffers 8192; ... } }` block in `backups.res` is updated for the alpha-bravo pair only.
- Other connections (e.g. node-0 to node-2) are unchanged.

## Validations

- On the satellite, `/var/lib/linstor.d/backups.res` shows `max-buffers 8192` only in the relevant connection block.
- `linstor resource-connection list-properties node-0 node-1 backups | grep max-buffers` returns `8192`.

## Doc reference

linstor-administration.adoc: `==== Setting DRBD peer options for LINSTOR resources or resource connections` (lines 3328-3385).

## Notes

- `resource drbd-peer-options` and `resource-connection drbd-peer-options` are equivalent (same REST endpoint).
- If multiple paths exist between the two nodes, the option applies to all of them.
- Remove with `--unset-max-buffers`.
