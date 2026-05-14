# day2-rg-modify-place-count

## Scenario

Change the placement count on an existing resource group; subsequent spawned resources use the new count, and the balance-resources task eventually adjusts existing resources too.

## Steps

1. Note the current count: `linstor resource-group list`.
2. Modify it: `linstor resource-group modify my_ssd_group --place-count 3`.
3. (Optional) Trigger immediate rebalance by waiting for the next `BalanceResourcesInterval` tick (default 3600s) or by issuing autoplace on each affected RD.

## Expected outcome

- New `rg spawn` from the group yields resources with 3 replicas.
- Existing resources of the group eventually converge to 3 replicas via the periodic balance task (unless `BalanceResourcesEnabled=false`).

## Validations

- `linstor rg l | grep my_ssd_group` shows `PlaceCount=3`.
- A freshly spawned resource has 3 rows in `linstor r l --resource <new-rsc>`.

## Doc reference

linstor-administration.adoc: `===== Automatically maintaining resource group placement count` (lines 885-907) and `===== Placement count` (lines 909-931).

## Notes

- The controller-level property `BalanceResourcesEnabled=false` (or on RG / RD) disables auto-rebalance for matching objects.
- `BalanceResourcesInterval` controls how often (default 1h); `BalanceResourcesGracePeriod` controls how long new resources are ignored after creation (default 1h).
- If the new count is impossible (more replicas than candidate nodes), spawn fails with `Not enough available nodes`.
