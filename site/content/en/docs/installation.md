---
title: "Installation"
linkTitle: "Installation"
weight: 20
description: "Prepare the nodes, deploy blockstor, and check that it is up."
---

Blockstor installs onto an existing Kubernetes cluster. You need the storage nodes prepared, cert-manager in the cluster, and the blockstor manifests from the release you want to run.

## Prerequisites

On every storage node that will run the satellite:

- The DRBD 9 kernel module, loaded (`modprobe drbd`).
- For ZFS pools, the ZFS kernel module. On Talos Linux both modules come from the `siderolabs/drbd` and `siderolabs/zfs` system extensions.
- At least one spare block device, or an existing LVM volume group or ZFS pool to back the storage pools.

The satellite image already carries the userspace tools (`drbd-utils`, `lvm2`, `cryptsetup`, `zfsutils`); only the kernel modules come from the host.

In the cluster:

- [cert-manager](https://cert-manager.io/), which issues the certificates for the API's mutual TLS.
- Permission to run privileged pods in the `blockstor-system` namespace. The satellite needs host networking and access to the host's block devices.

## Get the manifests

The CRDs and workload manifests are in the blockstor repository. Check out the tag of the release you are installing:

```sh
git clone https://github.com/cozystack/blockstor.git
cd blockstor
git checkout v<VERSION>
```

The workload manifests carry an `__REGISTRY__/<image>:dev` placeholder used by the development stand. Point them at the published images for your version:

```sh
TAG=<VERSION>
mkdir -p rendered
for f in blockstor-deploy.yaml blockstor-apiserver-tls.yaml blockstor-apiserver-deploy.yaml blockstor-satellite-daemonset.yaml; do
  sed -e "s#__REGISTRY__/\(blockstor-[a-z]*\):dev#ghcr.io/cozystack/\1:${TAG}#" "stand/$f" > "rendered/$f"
done
```

Each release publishes the images as `MAJOR.MINOR.PATCH` and `MAJOR.MINOR`, and stable releases also as `latest`.

## Deploy

Apply the pieces in this order.

1. The custom resource definitions:

   ```sh
   kubectl apply -f config/crd/bases/
   ```

2. The namespace, the controller and its RBAC:

   ```sh
   kubectl apply -f rendered/blockstor-deploy.yaml
   ```

3. The certificate chain for the API. It creates a CA, the API server's serving certificate, and a client certificate for in-cluster clients such as CSI:

   ```sh
   kubectl apply -f rendered/blockstor-apiserver-tls.yaml
   ```

4. The API server and the satellites:

   ```sh
   kubectl apply -f rendered/blockstor-apiserver-deploy.yaml
   kubectl apply -f rendered/blockstor-satellite-daemonset.yaml
   ```

The satellite DaemonSet runs on every node that is not a control-plane node. Wait for everything to come up:

```sh
kubectl -n blockstor-system rollout status deploy/blockstor-controller
kubectl -n blockstor-system rollout status deploy/blockstor-apiserver
kubectl -n blockstor-system rollout status daemonset/blockstor-satellite
```

## Register nodes and storage pools

Each storage node is a cluster-scoped `Node` resource named after the Kubernetes node, and each pool on a node is a `StoragePool`. You can create them as YAML (see [Resources](../resources/)) or with the CLI.

Install the CLI from the release tag. It needs Go and reads your kubeconfig, so it talks to the cluster the same way `kubectl` does:

```sh
go install github.com/cozystack/blockstor/cmd/blockstor@v<VERSION>
```

Register the nodes. The address is the one DRBD uses for replication traffic between nodes:

```sh
blockstor node create worker-1 10.0.0.11
blockstor node create worker-2 10.0.0.12
blockstor node create worker-3 10.0.0.13
```

Create a pool from a raw disk in one step. This prepares the device and registers the pool:

```sh
blockstor physical-storage create-device-pool zfs worker-1 /dev/sdb --pool-name data
```

Or register a volume group or ZFS pool that already exists:

```sh
blockstor storage-pool create lvmthin worker-1 data my_vg/my_thinpool
```

The providers are `lvm`, `lvmthin`, `zfs`, `zfsthin`, `file`, `filethin` and `diskless`. Every node also gets a diskless pool, `DfltDisklessStorPool`, so it can host diskless replicas and tiebreakers.

## Check the installation

```sh
blockstor controller version
blockstor node list
blockstor storage-pool list
```

Every node should show as online, and every pool should report its free and total capacity once its satellite has seen it.

Then create a first replicated volume to see the whole path work:

```sh
blockstor resource-group create mygroup --place-count 2 --storage-pool data
blockstor volume-group create mygroup
blockstor resource-group spawn mygroup test-volume 1G
blockstor resource list
```

A healthy result is two `UpToDate` replicas and a `TieBreaker` on a third node. Remove it when you are done:

```sh
blockstor resource-definition delete test-volume
```

Next, connect Kubernetes to it: [Provisioning volumes with CSI](../csi/).
