# day2-k8s-ha-controller-fast-failover

## Scenario

Deploy the LINSTOR High Availability Controller (`linstor-ha-controller`) so that pods stuck on a failed node failover faster (taint-based eviction within seconds, not 5 minutes).

## Steps

1. Install the HA controller helm chart in the same namespace as the Operator: `helm install linstor-ha-controller linstor/linstor-ha-controller`.
2. Verify the pod is running: `kubectl get pods | grep ha-controller`.
3. Simulate a node failure: power off a node hosting one or more workloads.
4. Observe pods being evicted within seconds and rescheduled onto surviving nodes.

## Expected outcome

- Within ~30 seconds of node failure, affected pods are killed and rescheduled.
- The HA controller adds a `node.kubernetes.io/out-of-service` taint to the failed node so workloads with `Detach` policy can be moved.

## Validations

- After power-off, the failed node receives the appropriate taint quickly.
- Pods that were on the node restart on another node.

## Doc reference

linstor-kubernetes.adoc: `===== Fast workload failover using the high availability controller` (lines 1383-1421).

## Notes

- Without the HA controller, default Kubernetes pod-eviction timing applies (5 minutes for `tolerationSeconds`).
- The HA controller is independent from the LINSTOR controller itself - it just speeds up Kubernetes scheduling decisions.
