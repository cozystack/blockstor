# day2-storage-pool-set-max-throughput

## Scenario

Configure the `Autoplacer/MaxThroughput` property on storage pools so that the autoplacer can choose pools by remaining I/O budget rather than free space alone.

## Steps

1. Set the max throughput in bytes/s on each candidate storage pool: `linstor storage-pool set-property <node> <pool> Autoplacer/MaxThroughput 524288000`.
2. Increase the weight of the `MaxThroughput` strategy in the controller: `linstor controller set-property Autoplacer/Weights/MaxThroughput 2`.
3. Trigger an autoplaced spawn: `linstor resource-group spawn myrg myrsc 10G`.

## Expected outcome

- The autoplacer adds `MaxThroughput` to its scoring; pools with more available throughput score higher.
- For every volume placed in the pool, its `sys/fs/blkio_throttle_read` and `sys/fs/blkio_throttle_write` are subtracted from the pool's budget when calculating remaining throughput.

## Validations

- `linstor sp l --storage-pool <pool>` (with `--show-props` or list-properties) shows the `Autoplacer/MaxThroughput` value.
- `linstor controller list-properties | grep Autoplacer/Weights` shows the configured weights.

## Doc reference

linstor-administration.adoc: `===== Storage pool placement` (lines 933-993).

## Notes

- Default weights: `MaxFreeSpace=1`, others=0. Setting `MaxThroughput` weight without setting `MaxThroughput` values per pool leaves the score at 0 and has no effect.
- Negative scores do not exclude a pool; they just rank lower.
- See also `day2-controller-set-autoplacer-weights.md` for tuning all four selection strategies.
