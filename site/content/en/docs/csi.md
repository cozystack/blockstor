---
title: "Provisioning volumes with CSI"
linkTitle: "CSI and StorageClasses"
weight: 25
description: "Connect linstor-csi to blockstor and provision PersistentVolumes."
---

Kubernetes provisions blockstor volumes through the LINSTOR CSI driver, `linstor-csi`. The driver speaks the LINSTOR REST API, and blockstor serves that API, so you point the driver at the blockstor API server instead of a LINSTOR controller.

## What to deploy

Piraeus normally ships a whole LINSTOR distribution. With blockstor you take only the client side:

| Piraeus component | With blockstor |
|---|---|
| `linstor-controller` | Skip. The blockstor controller and API server replace it. |
| `linstor-satellite` | Skip. Blockstor runs its own satellites. |
| `linstor-csi` (controller and node plugin) | Deploy, pointed at the blockstor API server. |
| HA controller, `drbd-reactor` | Optional. Point them at the API server the same way as CSI. |

## Point the driver at blockstor

The API server listens with mutual TLS on port 3371 of the `blockstor-apiserver` Service in `blockstor-system`. The installation created a client certificate for in-cluster clients: the Secret `blockstor-apiserver-client-tls` carries `tls.crt`, `tls.key` and `ca.crt`. A pod in `blockstor-system` can mount that Secret directly; for a driver in another namespace, see [Client certificates in another namespace](#client-certificates-in-another-namespace) first. Mount the Secret into both the CSI controller and the CSI node plugin, and set the endpoint and certificate paths:

```yaml
env:
  - name: LS_CONTROLLERS
    value: "https://blockstor-apiserver.blockstor-system.svc:3371"
  - name: LS_USER_CERTIFICATE
    value: /etc/linstor/client/tls.crt
  - name: LS_USER_KEY
    value: /etc/linstor/client/tls.key
  - name: LS_ROOT_CA
    value: /etc/linstor/client/ca.crt
volumeMounts:
  - name: client-tls
    mountPath: /etc/linstor/client
    readOnly: true
volumes:
  - name: client-tls
    secret:
      secretName: blockstor-apiserver-client-tls
```

The node plugin's `--node` argument must match the name of a blockstor `Node`, which is the Kubernetes node name.

## Client certificates in another namespace

The supported layout runs the CSI driver in `blockstor-system`, next to blockstor: install piraeus-operator into that namespace, and its components take their client certificates from `blockstor-api-ca` directly, with nothing to copy. The rest of this section is for a driver that has to run elsewhere.

The Secret `blockstor-apiserver-client-tls` and the `blockstor-api-ca` Issuer both live in `blockstor-system`, and `blockstor-api-ca` is a namespaced `Issuer`. cert-manager looks up an `Issuer` in the Certificate's own namespace, so a Certificate in any other namespace that names `blockstor-api-ca` is never issued. A pod can only mount a Secret from its own namespace. A CSI driver that runs outside `blockstor-system` needs the client material brought into its namespace. There are two ways to do that.

**Mirror the CA into the client's namespace.** Copy the CA Secret `blockstor-api-ca` into the client's namespace and create a CA `Issuer` over it there. Certificates in that namespace are then issued and renewed by cert-manager like any other. This is what blockstor's own test installation does for piraeus-operator, in the namespace `piraeus-datastore`:

```sh
kubectl -n blockstor-system get secret blockstor-api-ca -o json \
  | jq 'del(.metadata.namespace, .metadata.resourceVersion, .metadata.uid, .metadata.creationTimestamp, .metadata.ownerReferences, .metadata.annotations, .metadata.labels)' \
  | kubectl -n piraeus-datastore apply -f -
kubectl apply -f - <<'EOF'
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: blockstor-api-ca
  namespace: piraeus-datastore
spec:
  ca:
    secretName: blockstor-api-ca
EOF
```

The copy carries the CA's private key. Anyone who can read Secrets in that namespace can sign a client certificate, and a client certificate is full access to the API. Mirror it only into a namespace you trust as much as `blockstor-system`.

**Copy a client certificate.** Issue the client Certificate in `blockstor-system`, from the `blockstor-api-ca` Issuer, and copy the Secret it produces, with `tls.crt`, `tls.key` and `ca.crt`, into the client's namespace. The CA key stays in `blockstor-system`. The copy is not renewed: the installation's client certificate lasts 90 days and is renewed 15 days before it expires, and you have to copy the Secret again after each renewal, or the client fails once the old certificate expires.

## With piraeus-operator

If you install `linstor-csi` with the piraeus operator, set `LinstorCluster.spec.externalController.url` to `https://blockstor-apiserver.blockstor-system.svc:3371`. That turns off the operator's own LINSTOR controller and points the CSI driver at blockstor.

For the client certificate, point `spec.apiTLS.certManager` at the `blockstor-api-ca` Issuer when the operator runs in `blockstor-system`. In any other namespace, mirror the CA into it as shown above and point `spec.apiTLS.certManager` at the `Issuer` you created there. The operator then creates, in its own namespace and from that Issuer, the client Certificates for itself, for the CSI pods and for its other components:

```yaml
apiVersion: piraeus.io/v1
kind: LinstorCluster
metadata:
  name: linstorcluster
spec:
  externalController:
    url: https://blockstor-apiserver.blockstor-system.svc:3371
  apiTLS:
    certManager:
      name: blockstor-api-ca
      kind: Issuer
```

To use copied Secrets instead, leave out `certManager` and provide a client Secret for every component the operator runs, not only for the CSI driver. The operator itself connects to the API server to configure the cluster, and the LinstorCluster does not become available without its Secret:

| Field in `spec.apiTLS` | Default name | Used by |
|---|---|---|
| `clientSecretName` | `linstor-client-tls` | The operator itself |
| `csiControllerSecretName` | `linstor-csi-controller-tls` | The CSI controller |
| `csiNodeSecretName` | `linstor-csi-node-tls` | The CSI node plugin |
| `affinityControllerSecretName` | `linstor-affinity-controller-tls` | The affinity controller |
| `nfsServerSecretName` | `linstor-csi-nfs-server-tls` | The NFS server, if you enable it |

Copy a client certificate into the operator's namespace under each default name, or set the fields to the names you used. Each Secret must carry `tls.crt`, `tls.key` and `ca.crt`, and you copy them again after each renewal. The mirrored CA with `certManager` avoids all of this, which is why blockstor's own test installation uses it.

cert-manager renews the certificates it issues in place, and the API server picks up a new certificate without a restart.

## StorageClass

Use the `linstor.csi.linbit.com` provisioner:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: blockstor-replicated
provisioner: linstor.csi.linbit.com
parameters:
  linstor.csi.linbit.com/storagePool: "data"
  linstor.csi.linbit.com/placementCount: "2"
  # Optional: take the policy from a resource group you created.
  # linstor.csi.linbit.com/resourceGroup: "replicated"
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
```

- `storagePool` is the pool that holds the replicas with data.
- `placementCount` is the number of replicas with data. Blockstor adds a tiebreaker when that number is even.
- `resourceGroup` uses a [resource group](../resources/#resourcegroup) as the policy. Without it, `linstor-csi` does not use `DfltRscGrp`: it creates a resource group named `sc-<uuid>` from the StorageClass parameters and puts the volumes there.

`storagePool` and `placementCount` are covered by blockstor's own tests. `resourceGroup` and the other `linstor.csi.linbit.com/*` parameters are passed through to the API unchanged; test the ones you rely on in your cluster first.

## A first PVC

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: blockstor-test
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: blockstor-replicated
  resources:
    requests:
      storage: 1Gi
```

With `WaitForFirstConsumer`, the PVC binds once a pod that uses it is scheduled. The PersistentVolume's name is also the name of the blockstor resource definition:

```sh
kubectl get pvc blockstor-test
kubectl get pv
blockstor resource list
```

Delete volumes through Kubernetes: deleting the PVC makes the CSI driver delete the definition and its replicas.
