# day2-rg-replicas-on-different

## Scenario

Force LINSTOR to spread replicas across nodes that have differing values of an auxiliary property (typical anti-affinity rule).

## Steps

1. Label nodes by zone: `linstor node set-property --aux node-0 zone z1`, `linstor node set-property --aux node-1 zone z2`, `linstor node set-property --aux node-2 zone z3`.
2. Create RG: `linstor resource-group create rg_spread --place-count 3 --replicas-on-different zone`.
3. Spawn a resource and inspect placement.

## Expected outcome

- Each replica lands on a node with a different `Aux/zone` value.
- If only N candidate values exist, place count > N causes a "Not enough available nodes" error.

## Validations

- `linstor r l --resource <new-rsc>` shows one replica per zone.
- `linstor node list-properties <each-replica-node> | grep Aux/zone` returns distinct values across replicas.

## Doc reference

linstor-administration.adoc: `===== Avoiding colocating resources when automatically placing a resource` and `===== Constraining automatic resource placement by using auxiliary node properties` (lines 995-1076).

## Notes

- Nodes WITHOUT the property are treated as "different" - a node with no `Aux/zone` will satisfy "different from any zone".
- Equivalent to `--x-replicas-on-different <prop> 1` (see `day2-rg-x-replicas-on-different.md`).
