# day2-node-interface-modify-active

## Scenario

Switch the controller-satellite communication channel to a different NIC by marking a new interface as the `--active` one.

## Steps

1. Create the desired interface on all relevant nodes: `linstor node interface create <node> satconn_1G <ip>`.
2. Mark it active: `linstor node interface modify <node> satconn_1G --active`.
3. Verify: `linstor node interface list <node>` shows the `StltCon` label on `satconn_1G`.

## Expected outcome

- The controller now reaches the satellite via `satconn_1G`.
- Existing DRBD replication paths are unaffected; only the LINSTOR control traffic moves.

## Validations

- `linstor node interface list <node>` shows `StltCon` next to `satconn_1G`.
- `tcpdump -i <linux-nic-for-satconn> port 3366` shows controller traffic.

## Doc reference

linstor-administration.adoc: `=== Managing network interface cards` (lines 2161-2185).

## Notes

- LINSTOR cannot route controller-CLIENT traffic (port 3370) through a specific NIC via CLI - use `ip route`/`iptables` for that.
- Disabling the active interface without first activating another locks out the controller; always have a fallback.
