# day2-sos-report

## Scenario

Generate an SOS report bundle for LINBIT / LINSTOR support investigation.

## Steps

1. Create the report on the controller: `linstor sos-report create`.
2. Or create AND download to the local machine: `linstor sos-report download`.
3. Locate the resulting `.tar.gz` in `/var/log/linstor/controller/` or in the CWD if downloaded.

## Expected outcome

- A `.tar.gz` archive containing logs, dmesg, tool versions, `ip a`, DB dump, and per-node debug info is produced.
- Safe to attach to a LINBIT support ticket.

## Validations

- File exists: `ls -la /var/log/linstor/controller/sos_*.tar.gz`.
- `tar tf sos_*.tar.gz | head` shows per-node directories.

## Doc reference

linstor-administration.adoc: `==== Generating SOS reports` (lines 4653-4677).

## Notes

- The bundle includes the LINSTOR DB dump - treat as sensitive (contains node IPs, pool names, possibly aux properties).
- For Kubernetes deployments, run the command inside the controller pod: `kubectl -n linbit-sds exec deploy/linstor-controller -- linstor sos-report create`.
