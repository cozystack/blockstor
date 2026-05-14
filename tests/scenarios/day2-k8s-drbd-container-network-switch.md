# day2-k8s-drbd-container-network-switch

## Scenario

Switch DRBD replication BACK to the Kubernetes container network after previously using the host network. Two recipes: reboot-based or drbdadm-based.

## Steps (drbdadm-based, online)

1. WARNING: do not create new volumes or snapshots during this procedure.
2. Suspend I/O on every satellite: `kubectl exec ds/linstor-satellite.<node> -- drbdadm suspend-io all` for each node.
3. Disconnect all DRBD connections: `kubectl exec ds/linstor-satellite.<node> -- drbdadm disconnect --force all` on each.
4. Reset all paths: `kubectl exec ds/linstor-satellite.<node> -- drbdadm del-path all` on each.
5. Delete the `LinstorSatelliteConfiguration` that set `hostNetwork: true`: `kubectl delete linstorsatelliteconfigurations.piraeus.io host-network`.
6. New satellite pods come up on the container network and reconfigure the resources.

## Steps (reboot-based, simpler)

1. Delete the `LinstorSatelliteConfiguration` host-network.
2. Reboot each node (rolling).
3. After reboot all DRBD resources are reconfigured for the container network.

## Expected outcome

- After completion, `<rsc>.res` files show container-network peer IPs.
- DRBD reconnects on the new addresses.

## Validations

- `kubectl exec ds/linstor-satellite.<node> -- cat /var/lib/linstor.d/<rsc>.res` shows pod-network IPs.
- `drbdadm status` shows `Connected`.

## Doc reference

linstor-kubernetes.adoc: `==== Configuring DRBD replication to use the container network` (lines 2891-2946).

## Notes

- Mid-procedure, rebooted and non-rebooted nodes can't replicate to each other.
- The drbdadm-based path keeps existing pods running; the reboot-based path requires planned downtime.
- See `day2-k8s-drbd-host-network.md` for the inverse.
