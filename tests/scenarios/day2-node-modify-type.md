# day2-node-modify-type

## Scenario

Change an existing LINSTOR node's type (for example, promote a satellite-only node to `combined` so it can also host the controller service).

## Steps

1. Run `linstor node list` to confirm the current type, for example `SATELLITE`.
2. Change the type: `linstor node modify --node-type combined bravo`.
3. List nodes again to confirm the new type.

## Expected outcome

- `linstor node list` reports `NodeType=COMBINED` for the modified node.
- The node remains `Online` and its existing resource assignments and properties are preserved.

## Validations

- `linstor node list` shows `COMBINED` in the `NodeType` column for `bravo`.
- `linstor resource list --nodes bravo` is unchanged from the pre-modify state.
- No spurious reconnect or restart of the satellite service is observed in journald.

## Doc reference

linstor-administration.adoc: `==== Specifying LINSTOR node types` (lines 542-559).

## Notes

- Allowed values: `controller`, `auxiliary`, `combined`, `satellite`.
- `--node-type` defaults to `satellite` when first creating a node.
- Changing the type does not move the controller service itself - that is a separate database/HA operation (see `day2-controller-ha-failover`).
