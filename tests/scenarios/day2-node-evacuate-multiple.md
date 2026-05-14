# day2-node-evacuate-multiple

## Scenario

Evacuate several nodes at once (for example a whole rack going offline) without LINSTOR re-placing resources onto another node that is also scheduled for evacuation.

## Steps

1. Mark every node planned for evacuation as ineligible for placement: for each node, `linstor node set-property <node> AutoplaceTarget false`.
2. Evacuate each node in turn: `linstor node evacuate <node>`.
3. Monitor sync progress: `linstor resource list` and wait for all `SyncTarget` rows to settle to `UpToDate` on the remaining (non-evacuating) nodes.
4. Once all evacuating nodes are empty, delete them: `linstor node delete <node>`.

## Expected outcome

- No resource gets re-placed onto another node that is also being evacuated.
- After the last delete, the cluster contains only the surviving nodes and every resource has its full replica count on those survivors.

## Validations

- For each `<node>` set to `AutoplaceTarget=false`, `linstor node list-properties <node>` contains `AutoplaceTarget=false`.
- During evacuation, no new resource appears on a node where `AutoplaceTarget=false`.
- After completion, `linstor resource list | grep -E '<evacuated_nodes>'` is empty.

## Doc reference

linstor-administration.adoc: `==== Evacuating multiple nodes` (lines 2410-2423).

## Notes

- `AutoplaceTarget=false` only affects autoplacement; manual `resource create` is still possible.
- If the surviving cluster cannot fit the desired replica count, evacuate emits a warning and leaves the resource on its source node - shrink `--place-count` first if you intend to permanently reduce redundancy.
