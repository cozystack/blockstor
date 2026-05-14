# day2-k8s-set-drbd-options-via-node-connection

## Scenario

Apply DRBD options between specific Kubernetes node pairs from a Kustomize / Helm overlay.

## Steps

1. Identify the Kubernetes node names that correspond to LINSTOR node names (one-to-one in most Operator deployments).
2. Apply DRBD options via the LINSTOR client inside the controller pod:
```
kubectl -n linbit-sds exec deploy/linstor-controller -- \
  linstor node-connection drbd-peer-options --max-buffers 8192 kube-0 kube-1
```
3. Or declaratively, via `LinstorClusterConfiguration` (Operator-specific CRD) - apply a YAML that defines `nodeConnections`.

## Expected outcome

- The DRBD net options take effect for traffic between the two specified nodes for all resources.

## Validations

- `kubectl -n linbit-sds exec deploy/linstor-controller -- linstor node-connection list-properties kube-0 kube-1` shows the configured value.
- On the satellite, `<rsc>.res` shows the option in the relevant connection block.

## Doc reference

linstor-kubernetes.adoc: `===== Setting DRBD options on a LINSTOR node connection in Kubernetes` (lines 1882-1925).

## Notes

- Some Operator versions expose this as a CRD field; others require imperative `linstor` commands.
- Hierarchy: RD > RG > resource-connection > node-connection > controller (lowest priority).
