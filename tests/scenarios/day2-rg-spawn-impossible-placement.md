# day2-rg-spawn-impossible-placement

## Scenario

Spawn a resource whose placement constraints cannot be satisfied (for example `--place-count 7` in a 3-node cluster); verify that LINSTOR refuses gracefully without creating partial state.

## Steps

1. Create an RG with an unreachable place count: `linstor resource-group create huge_rg --place-count 7 --storage-pool pool_ssd`.
2. Attempt to spawn: `linstor resource-group spawn huge_rg fail_res 10G`.
3. Inspect the error and ensure no resources or RDs were left behind.

## Expected outcome

- `spawn` returns a non-zero exit and emits an error containing `Not enough available nodes`.
- No RD, no VD and no resource entries were created.

## Validations

- `linstor rd l | grep fail_res` returns empty.
- `linstor r l | grep fail_res` returns empty.
- The error message contains `Not enough available nodes`.

## Doc reference

linstor-administration.adoc: warning box at lines 916-931 ("Creating a resource group with impossible placement constraints").

## Notes

- The RG itself is still allowed to exist with an impossible constraint; only `spawn` fails.
- To recover, lower `--place-count` on the RG (`rg modify --place-count <n>`) or add more eligible nodes.
- Cross-reference: see `day2-rg-modify-place-count.md` for fixing the RG.
