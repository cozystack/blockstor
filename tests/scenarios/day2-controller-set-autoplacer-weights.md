# day2-controller-set-autoplacer-weights

## Scenario

Tune which selection strategy the LINSTOR autoplacer prefers when choosing a storage pool: free space, reserved space, resource count, or throughput.

## Steps

1. Set the controller-level weights (any decimal value):
```
linstor controller set-property Autoplacer/Weights/MaxFreeSpace 1
linstor controller set-property Autoplacer/Weights/MinReservedSpace 2
linstor controller set-property Autoplacer/Weights/MinRscCount 1
linstor controller set-property Autoplacer/Weights/MaxThroughput 0
```
2. Confirm: `linstor controller list-properties | grep Autoplacer/Weights`.
3. Spawn resources and observe selection results.

## Expected outcome

- The autoplacer normalises each strategy's score, multiplies by the weight, and sums up; the highest-scoring storage pool group wins.
- Pools with score 0 or negative are still eligible but ranked last.

## Validations

- `linstor controller list-properties` lists the configured weights.
- Repeated `rg spawn` placements gravitate toward pools that score best under your chosen weighting.

## Doc reference

linstor-administration.adoc: `===== Storage pool placement` (lines 933-993).

## Notes

- Defaults: only `MaxFreeSpace=1`, all others=0 (backward-compatible behaviour).
- For thin-provisioned pools, `MaxFreeSpace` may understate usage; consider mixing in `MinReservedSpace`.
