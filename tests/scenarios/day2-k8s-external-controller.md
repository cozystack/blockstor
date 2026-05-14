# day2-k8s-external-controller

## Scenario

Configure the Kubernetes Operator (v2 or v1) to use an EXTERNAL LINSTOR controller (e.g. running on bare metal), rather than the one bundled in the cluster.

## Steps (Operator v2)

1. Disable the bundled controller in the `LinstorCluster` CR: set `spec.externalController.url` to the external controller's URL/IP.
2. (Optional) Configure host networking for satellites so they can reach the external controller:
```
spec:
  podTemplate:
    spec:
      hostNetwork: true
```
3. Apply.
4. Verify connectivity: `kubectl -n linbit-sds exec deploy/linstor-controller -- ...` should fail (no in-cluster controller); use `linstor --controllers <external>` from the outside instead.

## Expected outcome

- All `LinstorSatellite` pods register with the external controller.
- Existing cluster operations work via the external CLI.

## Validations

- `linstor --controllers <external> node list` shows the Kubernetes nodes as `Online`.
- No `linstor-controller` Deployment in the cluster namespace.

## Doc reference

linstor-kubernetes.adoc: `==== Operator v2 deployment with an external LINSTOR controller` (lines 1441-1538) and v1 variant at lines 1539-1574.

## Notes

- The external controller must be reachable on its REST port (3370 plain / 3371 HTTPS) from every node.
- Failover of the external controller is the operator's responsibility (Pacemaker, drbd-reactor, manual).
- For HA between clusters, see `day2-controller-ha-failover.md`.
