# day2-error-reports-list

## Scenario

List recent error reports across the cluster for troubleshooting.

## Steps

1. List: `linstor error-reports list`.
2. Filter by time: `--since <ISO timestamp>` or `--to <ISO timestamp>`.
3. Filter by node: `--nodes <node>`.
4. Fetch a specific report: `linstor error-reports show <id>`.
5. (Optional) Delete old reports: `linstor error-reports delete --before <date>`.

## Expected outcome

- A list of error reports (each with ID, timestamp, node, brief description) is shown.
- `show <id>` returns the full stack-trace and context.

## Validations

- `linstor error-reports list | head` returns rows when errors exist; empty list when healthy.
- Each report has a unique ID and timestamp.

## Doc reference

linstor-administration.adoc: `=== Generating SOS reports` (lines 4653-4677) - error reports are part of the SOS data; the CLI subcommand is in the LINSTOR client help.

## Notes

- The Prometheus `/metrics` endpoint exposes a count of error reports (see `day2-monitoring-prometheus.md`).
- Combine with `linstor sos-report create` for a complete diagnostic bundle.
