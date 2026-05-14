# day2-resource-connection-multipath

## Scenario

Configure multiple DRBD network paths between two nodes for a resource (failover or load-balanced replication).

## Steps

1. Create the per-node interfaces:
```
linstor node interface create alpha nic1 192.168.43.221
linstor node interface create alpha nic2 192.168.44.221
linstor node interface create bravo nic1 192.168.43.222
linstor node interface create bravo nic2 192.168.44.222
```
2. Add paths in the resource connection:
```
linstor resource-connection path create alpha bravo myResource path1 nic1 nic1
linstor resource-connection path create alpha bravo myResource path2 nic2 nic2
```
3. (Optional) Add the implicit default as a third path: `linstor resource-connection path create alpha bravo myResource path3 default default`.

## Expected outcome

- The `.res` file gains two (or three) `path { ... }` blocks for the alpha-bravo connection.
- TCP transport: paths used round-robin on failure. RDMA transport: paths used concurrently and balanced.

## Validations

- On a satellite, `/var/lib/linstor.d/myResource.res` shows multiple `path { ... }` blocks with the chosen IPs and dynamic ports.
- `drbdadm status myResource` continues to show `Connected` after disconnecting one of the underlying physical paths.

## Doc reference

linstor-administration.adoc: `==== Creating multiple DRBD paths with LINSTOR` (lines 2186-2255).

## Notes

- Port numbers passed when creating the interface are IGNORED by `resource-connection path create`; LINSTOR dynamically assigns from the controller's `TcpPortAutoRange` (default 7000-7999).
- Adding any explicit path makes the implicit default disappear from this connection; re-add explicitly if you want it.
- See `day2-controller-tcp-port-range.md` for narrowing the dynamic port range.
