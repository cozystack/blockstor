# day2-node-add

## Scenario

Add a new satellite node to a running LINSTOR cluster so that it can host LINSTOR storage resources.

## Steps

1. On the new host, install the LINSTOR satellite packages and start the service: `systemctl enable --now linstor-satellite`.
2. From any host with `linstor` CLI configured to talk to the controller, register the node and supply the satellite's IP address: `linstor node create bravo 10.43.70.3`.
3. Wait roughly 10 seconds for the controller to handshake with the new satellite.
4. Verify the node is reachable: `linstor node list`.

## Expected outcome

- `linstor node list` shows the new node with `NodeType=SATELLITE` and `State=Online`.
- The node has at least one network interface implicitly named `default` whose address matches the IP supplied at creation time.
- No resources are placed on the node yet.

## Validations

- `linstor node list | grep bravo` contains `Online` and `10.43.70.3:3366 (PLAIN)`.
- `linstor node interface list bravo` shows an interface named `default`.
- `linstor resource list --nodes bravo` returns an empty resource table.

## Doc reference

linstor-administration.adoc: `=== Adding nodes to your cluster` (lines 465-541) and `==== Starting and enabling a LINSTOR satellite node` (lines 519-541).

## Notes

- Omitting the IP makes the controller try to resolve the node name as a DNS hostname; if resolution fails on the controller host the create returns `Unable to resolve ip address`.
- If `node list` shows `Offline` for more than ~10 seconds, the satellite service is not running or a firewall is blocking the controller TCP port (default 3366 PLAIN / 3367 SSL).
- To create the node with a non-default network interface name, pass `--interface-name`.
