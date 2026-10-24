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

The API server listens with mutual TLS on port 3371 of the `blockstor-apiserver` Service in `blockstor-system`. The installation created a client certificate for in-cluster clients: the Secret `blockstor-apiserver-client-tls` carries `tls.crt`, `tls.key` and `ca.crt`. Mount it into both the CSI controller and the CSI node plugin, and set the endpoint and certificate paths:

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

If you install `linstor-csi` with the piraeus operator, set `LinstorCluster.spec.externalController.url` to `https://blockstor-apiserver.blockstor-system.svc:3371`. That turns off the operator's own LINSTOR controller and points the CSI driver at blockstor. Point `spec.apiTLS.certManager` at the `blockstor-api-ca` Issuer, so the operator issues the client certificate for the CSI pods.

The Secret lives in `blockstor-system`. If the CSI driver runs in another namespace, issue a second client certificate into that namespace from the same `blockstor-api-ca` Issuer.

cert-manager renews the certificates in place, and the API server picks up a new certificate without a restart.

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
- `resourceGroup` uses a [resource group](../resources/#resourcegroup) as the policy. Without it, volumes go into the default group, `DfltRscGrp`.

`storagePool`, `placementCount` and `resourceGroup` are covered by blockstor's own tests. Other `linstor.csi.linbit.com/*` parameters are passed through to the API unchanged; test the ones you rely on in your cluster first.

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
