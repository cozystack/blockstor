# day2-drbd-proxy-modify-compression

## Scenario

Adjust DRBD Proxy compression and memlimit on a long-distance link to balance latency vs. CPU cost.

## Steps

1. List current options: `linstor drbd-proxy list-properties <rsc>`.
2. Set new memlimit: `linstor drbd-proxy options <rsc> --memlimit 200000000`.
3. Change compression: `linstor drbd-proxy compression zlib <rsc> --level 6`.
4. Verify; check Proxy logs for the new effective values.

## Expected outcome

- Proxy uses the new buffer and compression configuration.
- No DRBD reconnect is required; Proxy applies live.

## Validations

- `linstor drbd-proxy list-properties <rsc>` reflects the new values.
- Proxy log on each Proxy-enabled node shows the new compression level.

## Doc reference

linstor-administration.adoc: lines 3673-3688.

## Notes

- Higher compression level = more CPU, less bandwidth. Tune to network conditions.
- Memlimit is in bytes per connection. Setting too low can stall remote replication under burst load.
