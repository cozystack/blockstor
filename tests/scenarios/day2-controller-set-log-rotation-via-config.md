# day2-controller-set-log-rotation-via-config

## Scenario

Tune LINSTOR controller logging via the bundled `logback.xml` (file rotation, log level per logger, output destinations).

## Steps

1. Locate the logback config (default `/usr/share/linstor-server/logback.xml` or similar).
2. Edit it: adjust appender rolling policy (size, time, retention) and per-logger levels.
3. Restart the controller: `systemctl restart linstor-controller`.
4. Verify the new behaviour: `journalctl -u linstor-controller | head` or examine the log files in `/var/log/linstor`.

## Expected outcome

- Log files rotate as configured; per-logger levels apply without restarting the JVM more than once.

## Validations

- `ls -la /var/log/linstor` shows rolled files.
- Log entries reflect the configured logger levels.

## Doc reference

linstor-administration.adoc: `=== Logging` (lines 3926-...). Mentions SLF4J + logback as the binding.

## Notes

- Use `set-log-level` for runtime adjustments (see `day2-logging-set-level.md`); editing `logback.xml` is for persistent / structural changes.
- Don't ship `logback.xml` to git - keep node-local.
