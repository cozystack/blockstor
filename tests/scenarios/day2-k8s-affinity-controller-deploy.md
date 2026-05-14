# day2-k8s-affinity-controller-deploy

## Scenario

Deploy the LINSTOR Affinity Controller so that PV node-affinity is updated when LINSTOR moves replicas (avoiding orphaned scheduling constraints).

## Steps

1. Add the LINSTOR Helm repo (or use the existing piraeus chart): `helm repo update`.
2. Install in the same namespace as the Operator: `helm install linstor-affinity-controller linstor/linstor-affinity-controller`.
3. Verify the controller pod is running: `kubectl -n <ns> get pods | grep linstor-affinity`.
4. Trigger a test: evacuate a node hosting some PVs and watch the affinity-controller update affected PVs.

## Expected outcome

- After LINSTOR evacuates / moves a replica, the corresponding PV is replaced with an updated version whose node affinity reflects current replica locations.
- Pods can be rescheduled onto nodes where the new replicas live without manual PV intervention.

## Validations

- `kubectl get pv <pv> -o yaml | grep -A5 nodeAffinity` shows the new node list.
- Pod scheduling succeeds on nodes that now have replicas.

## Doc reference

linstor-kubernetes.adoc: `=== LINSTOR affinity controller` (lines 2697-2718).

## Notes

- Without this controller, PV node-affinity is set ONCE at creation and never updated; nodes added later that gain replicas are not added to the affinity list.
- Upstream project: https://github.com/piraeusdatastore/linstor-affinity-controller
