# day2-node-prefnic

## Scenario

Steer DRBD replication traffic through a chosen network interface by setting the `PrefNic` property at the node level.

## Steps

1. Make sure the interface exists: `linstor node interface list <node>`.
2. Set the property: `linstor node set-property <node> PrefNic nic_10G`.
3. Verify: `linstor node list-properties <node>`.
4. (Optional) Cause LINSTOR to regenerate `.res` files - happens automatically on next resource create/modify, or by toggling a property.

## Expected outcome

- DRBD traffic for resources whose storage pool is on this node uses `nic_10G`'s IP.
- Existing connections continue to use the previously-configured paths until the resource `.res` file is regenerated.

## Validations

- `linstor node list-properties <node> | grep PrefNic` returns `nic_10G`.
- After regeneration, `/var/lib/linstor.d/<rd>.res` on the satellite references the `nic_10G` IP for `<node>`.

## Doc reference

linstor-administration.adoc: `=== Managing network interface cards` (lines 2120-2185).

## Notes

- Setting `PrefNic` on a node is safer than on a storage pool. Diskless / tiebreaker resources will still use the `default` interface unless that one also has `PrefNic` configured.
- For multi-path DRBD (not just one-NIC), use `linstor resource-connection path` - see `day2-resource-connection-multipath.md`.
