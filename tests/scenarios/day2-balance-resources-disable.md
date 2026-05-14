# day2-balance-resources-disable

## Scenario

Disable the periodic balance task that re-evaluates resource placement against the RG's `--place-count`. Useful when you want fully manual control or while running maintenance.

## Steps

1. Disable globally: `linstor controller set-property BalanceResourcesEnabled false`.
2. Or selectively: `linstor resource-group set-property <rg> BalanceResourcesEnabled false` (or on RD).
3. Verify: `linstor controller list-properties | grep BalanceResources`.

## Expected outcome

- After disabling, LINSTOR no longer reconciles existing resources toward the RG's `--place-count`.
- New `rg spawn` is unaffected; manual `resource create` / `resource delete` is unaffected.

## Validations

- `linstor controller list-properties | grep BalanceResourcesEnabled` shows `false`.
- Deliberately under-placing a resource (deleting one replica) does NOT trigger an auto-replacement.

## Doc reference

linstor-administration.adoc: `===== Automatically maintaining resource group placement count` (lines 885-907).

## Notes

- Default scan interval is `BalanceResourcesInterval=3600` seconds (1 hour).
- Grace period after creation: `BalanceResourcesGracePeriod=3600` seconds.
- The property hierarchy: RD overrides RG, RG overrides controller.
