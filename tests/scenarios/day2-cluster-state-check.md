# day2-cluster-state-check

## Scenario

Get a high-level health snapshot of the LINSTOR cluster (nodes, storage pools, resources).

## Steps

1. List nodes: `linstor node list`.
2. List storage pools grouped by size: `linstor storage-pool list --groupby Size`.
3. List resources: `linstor resource list`.
4. (Optional) Filter / sort: `linstor resource list --groupby Node`, `--resource <rd>`, `--nodes <node>`.

## Expected outcome

- All nodes are `Online`.
- All storage pools report sane free / total capacity.
- All resources are `UpToDate` or in a known healthy non-Primary state.

## Validations

- `linstor node list | grep -v Online` returns only header rows.
- `linstor resource list | awk '{print $NF}'` shows expected states (`UpToDate`, `Inconsistent` during sync only).

## Doc reference

linstor-administration.adoc: `=== Checking cluster state` (lines 2352-2362).

## Notes

- The `--groupby` option lets you sort/group across multiple columns - useful in CLI dashboards.
- For machine-readable output, add `--machine-readable` to any list command.
