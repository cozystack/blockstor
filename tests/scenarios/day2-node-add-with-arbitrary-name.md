# day2-node-add-with-arbitrary-name

## Scenario

Add a node to the LINSTOR cluster using a logical name that does not match the host's actual `uname --nodename`.

## Steps

1. Pick an arbitrary LINSTOR node name (for example `storage-a`) and the satellite's IP address.
2. Create the node and explicitly pass the IP so the controller does not need to resolve the name: `linstor node create storage-a 10.43.70.10`.
3. Confirm that an INFO message is logged about the name/hostname mismatch.
4. Verify that LINSTOR still resolves the satellite's actual hostname for the DRBD resource configuration.

## Expected outcome

- The node is registered with the chosen name.
- The controller logs `[...] 'storage-a' and hostname 'node-1' doesn't match.`
- DRBD `.res` files generated for resources placed on this node use the satellite's real `uname --nodename`, not `storage-a`.

## Validations

- `linstor node list | grep storage-a` shows the node `Online`.
- After deploying a resource, the satellite's `/var/lib/linstor.d/<rsc>.res` lists the real Linux hostname inside the `on <hostname>` block.

## Doc reference

linstor-administration.adoc: `==== Naming LINSTOR nodes` (lines 504-518).

## Notes

- Mixing arbitrary LINSTOR names with real hostnames in the same cluster is supported but confusing; LINBIT recommends matching the LINSTOR node name to the hostname.
- This affects the DRBD `on` clause; if you later rename the OS hostname, the DRBD config can become inconsistent.
