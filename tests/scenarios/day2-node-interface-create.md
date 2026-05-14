# day2-node-interface-create

## Scenario

Add a secondary network interface to a satellite node so that DRBD or controller-satellite traffic can flow through a different NIC.

## Steps

1. Run `linstor node interface create node-0 nic_10G 192.168.43.231`.
2. Verify: `linstor node interface list node-0`.

## Expected outcome

- A new net-interface row `nic_10G` with the supplied IP appears for the node.
- No change to existing traffic until a `PrefNic` is set or `--active` is used.

## Validations

- `linstor node interface list node-0 | grep nic_10G` returns one row.
- Interface name follows naming rules (length 3-32, starts with letter or `_`, alphanumerics + `_` + `-`).

## Doc reference

linstor-administration.adoc: `=== Managing network interface cards` (lines 2119-2185).

## Notes

- The interface is identified by IP only; the LINSTOR name is arbitrary and unrelated to the Linux kernel NIC name.
- Use `PrefNic` (`day2-node-prefnic.md`) to route DRBD traffic; use `--active` (`day2-node-interface-modify-active.md`) to switch the controller-satellite channel.
