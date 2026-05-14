# day2-k8s-drbd-host-network

## Scenario

Switch DRBD replication traffic from the Kubernetes container network to the host network (Operator v2).

## Steps

1. Apply a `LinstorSatelliteConfiguration` with `hostNetwork: true`:
```
apiVersion: piraeus.io/v1
kind: LinstorSatelliteConfiguration
metadata:
  name: host-network
spec:
  podTemplate:
    spec:
      hostNetwork: true
```
2. Wait for satellite pods to be recreated.
3. LINSTOR reconfigures existing resources with new (host) peer IPs.

## Expected outcome

- After pods restart, every DRBD `.res` shows host-network IPs for peers.
- Replication traffic is independent of CNI plugin and pod lifecycle.

## Validations

- `kubectl exec ds/linstor-satellite.<node> -- cat /var/lib/linstor.d/<rsc>.res` shows host-network IPs.
- `drbdadm status` on the node shows `Connected` to the new peer IPs.

## Doc reference

linstor-kubernetes.adoc: `==== Configuring DRBD replication to use the host network` (lines 2870-2890).

## Notes

- You MUST ensure firewall rules allow DRBD ports (typically 7000-7999) between hosts on the host network.
- Pros: replication survives satellite-pod restarts. Cons: NetworkPolicy can no longer block unauthorised DRBD access.
- To switch BACK, see `day2-k8s-drbd-container-network-switch.md`.
