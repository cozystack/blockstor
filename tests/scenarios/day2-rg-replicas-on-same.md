# day2-rg-replicas-on-same

## Scenario

Force the autoplacer to keep all replicas of a resource on nodes that share the same value of a node-level auxiliary property (for example a "zone" or "label").

## Steps

1. Set an aux property on the candidate nodes: `for n in node-0 node-2; do linstor node set-property --aux $n testProperty 1; done` and `linstor node set-property --aux node-1 testProperty 0`.
2. Create an RG with the constraint: `linstor resource-group create testRscGrp --place-count 2 --replicas-on-same testProperty=1`.
3. Spawn a resource: `linstor resource-group spawn-resources testRscGrp testResource 100M`.
4. Verify placement: `linstor resource list --resource testResource`.

## Expected outcome

- The two replicas land on nodes whose `Aux/testProperty=1` (`node-0` and `node-2` in the example).
- No replica is placed on `node-1`, even though it has capacity.

## Validations

- `linstor r l --resource testResource` lists exactly the constraint-matching nodes.
- For each replica node, `linstor node list-properties <node> | grep Aux/testProperty` returns `1`.

## Doc reference

linstor-administration.adoc: `===== Constraining automatic resource placement by using auxiliary node properties` (lines 1006-1076).

## Notes

- `--replicas-on-same` expects an `Aux/`-namespaced property; you can use `--aux` shorthand on `node set-property`.
- In Kubernetes, set `Aux/...` properties to mirror node labels and use `replicasOnSame` in the StorageClass for the same effect (see `day2-rg-replicas-on-different.md` for the inverse).
