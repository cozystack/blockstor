# day2-rg-x-replicas-on-different

## Scenario

Ensure stretched-cluster placement: N replicas allowed per site, but at least one replica must be off-site for disaster recovery (e.g. 2 replicas in `dc1`, 1 replica in `dc2`).

## Steps

1. Label nodes: `linstor node set-property --aux <node> site dc1` (or `dc2`).
2. Spawn the resource with the constraint: `linstor resource-group spawn --x-replicas-on-different site 2 --place-count 3 myrg myres 200G`.
3. Verify placement: `linstor resource list --resource myres`.

## Expected outcome

- 3 replicas total: 2 in one site, 1 in the other.
- If placement is impossible (for example `--x-replicas-on-different site 1 --place-count 3` in a 2-site cluster with no nodes missing the property), spawn fails with `Not enough available nodes`.

## Validations

- Count of nodes in each site (`Aux/site=...`) in the resource's placement matches `--x-replicas-on-different` semantics (at most N replicas per site value).
- `linstor r l --resource myres` shows 3 rows.

## Doc reference

linstor-administration.adoc: `===== Ensuring automatic resource placement on different nodes for disaster recovery` (lines 1098-1199).

## Notes

- `--x-replicas-on-different site 1` is equivalent to `--replicas-on-different site`.
- Nodes with the aux property unset count as their own "different" group; if you have unlabeled nodes in the cluster the constraint loosens unexpectedly. Either label every node or filter them out via `--storage-pool`.
