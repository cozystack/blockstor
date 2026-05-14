# day2-properties-aux-set-unset

## Scenario

Set or remove an arbitrary auxiliary (`Aux/`) property on a LINSTOR object (most commonly a node) for placement and observability use.

## Steps

1. Set with the convenience `--aux` flag: `linstor node set-property --aux <node> rack-id rack-7`.
2. Verify: `linstor node list-properties <node> | grep Aux/rack-id`.
3. Unset (clear): `linstor node set-property --aux <node> rack-id` (no value).

## Expected outcome

- The property `Aux/rack-id` is present (or absent) under the node.
- It is now usable as a placement constraint (`--replicas-on-same`, `--replicas-on-different`, `--x-replicas-on-different`).

## Validations

- `linstor node list-properties <node>` reflects set / cleared state.
- A subsequent `rg spawn` with `--replicas-on-same Aux/rack-id` observes the property.

## Doc reference

linstor-administration.adoc: lines 1006-1076 (use of Aux/ properties for placement) and lines 1077-1097 (unset).

## Notes

- The `--aux` flag is shorthand; under the hood the property is stored as `Aux/<key>`.
- Aux properties on the same name are independent across nodes / RGs / RDs - no inheritance unless documented for a specific feature.
