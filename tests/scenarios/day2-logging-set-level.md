# day2-logging-set-level

## Scenario

Change the LINSTOR controller log level at runtime without restarting the controller.

## Steps

1. Set the level (`TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`): `linstor controller set-log-level DEBUG`.
2. Tail logs: `journalctl -fu linstor-controller` (bare metal) or `kubectl -n linbit-sds logs -f deploy/linstor-controller`.
3. Reset when finished: `linstor controller set-log-level INFO`.

## Expected outcome

- Log output increases in verbosity at DEBUG / TRACE.
- No controller restart is required.

## Validations

- After `set-log-level DEBUG`, the controller log shows debug lines.
- After `set-log-level INFO`, debug lines stop appearing.

## Doc reference

linstor-administration.adoc: `=== Logging` (lines 3926-...; references `controller set-log-level` since client 1.20.1).

## Notes

- DEBUG / TRACE add significant I/O; revert after troubleshooting.
- For per-satellite log levels, set via the per-satellite TOML / `linstor_satellite.toml` reload.
