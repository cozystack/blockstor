---
title: "Quickstart"
linkTitle: "Quickstart"
weight: 15
description: "From an empty cluster to a pod writing to a replicated volume."
---

This page is the shortest path from a Kubernetes cluster with DRBD-capable nodes to a pod writing to a replicated volume. It leaves out the choices. [Installation](../installation/) and [Provisioning volumes with CSI](../csi/) explain them.

## Before you start

You need:

- Three or more worker nodes with the DRBD 9 kernel module loaded, and the ZFS module if you want ZFS pools. On Talos Linux both come from the `siderolabs/drbd` and `siderolabs/zfs` system extensions.
- One spare block device on each of those nodes. The example below uses `/dev/sdb`.
- [cert-manager](https://cert-manager.io/) in the cluster. It issues the certificates for the API.
- `kubectl` with cluster-admin access, and Go to install the `blockstor` CLI.

The satellite image carries `drbd-utils`, `lvm2`, `cryptsetup` and `zfsutils`. Only the kernel modules come from the host.

## 1. Deploy blockstor

Check out the release tag and point the manifests at the published images:

```sh
git clone https://github.com/cozystack/blockstor.git
cd blockstor
git checkout v<VERSION>

TAG=<VERSION>
mkdir -p rendered
for f in blockstor-deploy.yaml blockstor-apiserver-tls.yaml blockstor-apiserver-deploy.yaml blockstor-satellite-daemonset.yaml; do
  sed -e "s#__REGISTRY__/\(blockstor-[a-z]*\):dev#ghcr.io/cozystack/\1:${TAG}#" "stand/$f" > "rendered/$f"
done
```

Apply the pieces in this order:

```sh
kubectl apply -f config/crd/bases/
kubectl apply -f rendered/blockstor-deploy.yaml
kubectl apply -f rendered/blockstor-apiserver-tls.yaml
kubectl apply -f rendered/blockstor-apiserver-deploy.yaml
kubectl apply -f rendered/blockstor-satellite-daemonset.yaml
```

Wait until everything is running. The satellite DaemonSet lands on every node that is not a control-plane node:

```sh
kubectl -n blockstor-system rollout status deploy/blockstor-controller
kubectl -n blockstor-system rollout status deploy/blockstor-apiserver
kubectl -n blockstor-system rollout status daemonset/blockstor-satellite
```

## 2. Register nodes and pools

Install the CLI. It reads your kubeconfig and needs no port-forward:

```sh
go install github.com/cozystack/blockstor/cmd/blockstor@v<VERSION>
```

Register each storage node under its Kubernetes node name. The address is the node's own IP, the one other nodes can reach, and it is what DRBD replicates over. `kubectl get nodes -o wide` shows it as `INTERNAL-IP`:

```sh
blockstor node create worker-1 10.0.0.11
blockstor node create worker-2 10.0.0.12
blockstor node create worker-3 10.0.0.13
```

Turn the spare disk on each node into a pool called `data`. This creates the ZFS pool and registers it in one step:

```sh
blockstor physical-storage create-device-pool zfs worker-1 /dev/sdb --pool-name data
blockstor physical-storage create-device-pool zfs worker-2 /dev/sdb --pool-name data
blockstor physical-storage create-device-pool zfs worker-3 /dev/sdb --pool-name data
```

Check the result. Every node should be `ONLINE` and every pool should show free and total capacity:

```sh
blockstor node list
blockstor storage-pool list
```

## 3. Connect the CSI driver

Kubernetes provisions blockstor volumes through `linstor-csi`. Deploy the driver's controller and node plugin, and point them at the blockstor API server with the client certificate the installation created:

- Endpoint: `https://blockstor-apiserver.blockstor-system.svc:3371`
- Client certificate: the Secret `blockstor-apiserver-client-tls` in `blockstor-system`, with `tls.crt`, `tls.key` and `ca.crt`

The exact environment variables and volume mounts, and the piraeus-operator alternative, are in [Provisioning volumes with CSI](../csi/#point-the-driver-at-blockstor).

Then create a StorageClass that places two replicas with data in the `data` pool:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: blockstor-replicated
provisioner: linstor.csi.linbit.com
parameters:
  linstor.csi.linbit.com/storagePool: "data"
  linstor.csi.linbit.com/placementCount: "2"
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
```

Two replicas is an even number, so blockstor adds a tiebreaker on a third node.

## 4. Create a volume and write to it

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: quickstart
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: blockstor-replicated
  resources:
    requests:
      storage: 1Gi
---
apiVersion: v1
kind: Pod
metadata:
  name: quickstart
spec:
  containers:
    - name: writer
      image: busybox
      command: ["sh", "-c", "echo hello from blockstor > /data/hello && sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: quickstart
```

With `WaitForFirstConsumer`, the volume is created when the pod is scheduled.

## 5. Check that it worked

```sh
kubectl get pvc quickstart
kubectl get pv
blockstor resource list
kubectl exec quickstart -- cat /data/hello
```

What you should see:

- The PVC is `Bound`.
- The PersistentVolume has the same name as a blockstor resource definition.
- `blockstor resource list` shows two replicas in `UpToDate` and one `TieBreaker`, on three different nodes.
- The file reads back.

Delete the pod and the PVC when you are done. The CSI driver deletes the definition and its replicas with the PVC.

## Where next

- [Networking and ports](../networking/) for what a firewall between nodes must allow.
- [Using the CLI](../cli/) and [Resources](../resources/) for day-to-day work.
- [Day-2 operations](../operations/) for adding nodes, moving replicas and upgrading.
- [Troubleshooting](../troubleshooting/) when something does not converge.
