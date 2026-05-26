# Installing and using blockstor

This guide covers deploying blockstor onto a Kubernetes cluster and driving it with the upstream `linstor` command-line client. blockstor exposes a LINSTOR-compatible REST API, so the stock `linstor` client (`python-linstor` / `linstor-client`) works unchanged.

If you only want a throwaway local dev environment, use the Talos+QEMU stand instead (`make up && make blockstor`, see the [README](../README.md) Quick start). This page is for installing onto a real cluster.

## Components

blockstor ships three container images, published by CI on every version tag (see [`.github/workflows/release.yml`](../.github/workflows/release.yml)):

| Image | Dockerfile target | Role |
|-------|-------------------|------|
| `ghcr.io/cozystack/blockstor` | `controller` | controller-runtime manager hosting the RD / RG / RP / Snapshot / Resource / Node reconcilers. Runs as a Deployment. |
| `ghcr.io/cozystack/blockstor-apiserver` | `apiserver` | Stateless LINSTOR-compatible REST front end backed by the CRD store. Runs as a multi-replica Deployment behind a Service. This is what the `linstor` client, linstor-csi and piraeus-operator talk to. |
| `ghcr.io/cozystack/blockstor-satellite` | `satellite` | Per-node manager that reconciles the DRBD / LUKS / STORAGE layers and shells out to `drbdadm` / `lvs` / `zfs` / `cryptsetup`. Runs as a DaemonSet on storage nodes. |

Each tag publishes `MAJOR.MINOR.PATCH`, `MAJOR.MINOR`, and (for stable, non pre-release tags) `latest`.

## Host requirements

Storage nodes that run the satellite need:

- The DRBD 9 kernel module loaded (`modprobe drbd`).
- `drbd-utils`, `lvm2`, `cryptsetup`, and (for ZFS pools) the ZFS kernel module plus `zfsutils-linux`. The satellite image ships the userspace tools; the kernel modules come from the host (for Talos, via the `siderolabs/drbd` and `siderolabs/zfs` extensions).
- At least one spare block device or a pre-created LVM volume group / ZFS pool to back the storage pools.

## Deploy

The reference manifests live under `config/crd/bases/` (CRDs) and `stand/` (the controller / apiserver / satellite workloads). Apply in this order:

1. **CRDs** — `Resource`, `ResourceDefinition`, `ResourceGroup`, `StoragePool`, `Snapshot`, `Node`, `PhysicalDevice`, `ControllerConfig`:

   ```sh
   kubectl apply -f config/crd/bases/
   ```

2. **Node CRs** — one cluster-scoped `Node` per storage node, with `metadata.name` equal to the Kubernetes node name. These can be created up front (the satellites also reconcile against them) or registered later through the `linstor` client (see below). A minimal Node CR:

   ```yaml
   apiVersion: blockstor.cozystack.io/v1alpha1
   kind: Node
   metadata:
     name: worker-1
   spec:
     type: SATELLITE
     netInterfaces:
       - name: default
         address: 10.0.0.11   # the node's InternalIP, used for DRBD replication
   ```

3. **Workloads** — the controller, apiserver, and satellite DaemonSet. The manifests in `stand/` carry an `__REGISTRY__/<image>:dev` placeholder for the dev stand; for a real cluster set the `image:` fields to the published tags, e.g. `ghcr.io/cozystack/blockstor:<TAG>`, `ghcr.io/cozystack/blockstor-apiserver:<TAG>`, `ghcr.io/cozystack/blockstor-satellite:<TAG>`:

   - `stand/blockstor-deploy.yaml` — controller + RBAC + the `blockstor-system` namespace.
   - `stand/blockstor-apiserver-deploy.yaml` — apiserver Deployment + Service.
   - `stand/blockstor-satellite-daemonset.yaml` — satellite DaemonSet.

All workloads run in the `blockstor-system` namespace. Wait for them to come up:

```sh
kubectl -n blockstor-system rollout status deploy/blockstor-controller
kubectl -n blockstor-system rollout status deploy/blockstor-apiserver
kubectl -n blockstor-system rollout status daemonset/blockstor-satellite
```

## Point the `linstor` client at the apiserver

Install the client (`apt install linstor-client`, or `pip install python-linstor`). The apiserver Service exposes a mutual-TLS listener on port **3371**; a plain-HTTP debug port **3370** is bound pod-local only. The simplest way to reach it from a workstation is a port-forward to the plain-HTTP port:

```sh
kubectl -n blockstor-system port-forward deploy/blockstor-apiserver 3370:3370
```

Then point the client at it (set this once per shell):

```sh
export LS_CONTROLLERS=http://localhost:3370
# or pass --controllers on every call:
linstor --controllers http://localhost:3370 node list
```

For in-cluster clients (linstor-csi, piraeus-operator) use the mTLS Service endpoint on 3371 with the issued client certificate instead of the port-forward.

A quick connectivity check:

```sh
linstor controller version
linstor node list
```

> Note: `linstor controller version` reports the blockstor version with a `git=blockstor` build stamp (e.g. `1.33.2+ git=blockstor`) rather than a hex commit hash — this is intentional. See [`docs/cli-parity-known-deltas.md`](cli-parity-known-deltas.md) for the full list of user-visible divergences from upstream LINSTOR (notably `advise`, `backup`, and `schedule` subcommands are not implemented).

## Basic setup, end to end

The commands below assume `LS_CONTROLLERS` is exported (otherwise add `--controllers …`). Replace node names, devices, and pool names with your own.

### 1. Register nodes

If you did not pre-create the `Node` CRs, register each satellite node:

```sh
linstor node create worker-1 10.0.0.11
linstor node create worker-2 10.0.0.12
linstor node create worker-3 10.0.0.13
linstor node list
```

The IP is the address used for DRBD replication traffic between nodes. Each node automatically gets a `DfltDisklessStorPool` (diskless) pool so it can host diskless replicas and tiebreakers.

### 2. Create a storage pool from a physical device

The one-shot way is `physical-storage create-device-pool` (short: `ps cdp`), which prepares the device and registers the pool in a single call. For a ZFS pool on `/dev/sdb`:

```sh
linstor physical-storage create-device-pool \
    --pool-name data --storage-pool data zfs worker-1 /dev/sdb
```

`zfs` selects the provider (also `lvm`, `lvmthin`, `zfsthin`). Repeat per node, or pass multiple devices to build a multi-device pool. If you already have a volume group or ZFS pool, register it directly instead:

```sh
linstor storage-pool create lvmthin worker-1 data my_vg/my_thinpool
```

Check the pools (and their free capacity) once the satellite has reported back:

```sh
linstor storage-pool list
```

### 3. Define a resource group (recommended)

A resource group captures replication and placement policy once, so each volume spawned from it inherits the settings:

```sh
linstor resource-group create mygroup --place-count 3 --storage-pool data
linstor volume-group create mygroup
linstor resource-group list
```

### 4. Create a replicated volume

There are two equivalent paths.

**One-shot via the resource group** — creates the resource-definition, volume-definition, replicas, and tiebreaker in a single call:

```sh
linstor resource-group spawn mygroup myvolume 10G
```

**Explicit, step by step** — useful when you are not using a resource group:

```sh
linstor resource-definition create myvolume
linstor volume-definition create myvolume 10G
linstor resource create myvolume --auto-place 3 --storage-pool data
```

`--auto-place N` lets the autoplacer pick N nodes (capacity-weighted, anti-affinity aware). To pin replicas to specific nodes instead, name them:

```sh
linstor resource create worker-1 worker-2 myvolume --storage-pool data
```

### 5. Inspect state

```sh
linstor node list                 # nodes and their online status
linstor storage-pool list         # pools, provider, free/total capacity
linstor resource-definition list  # resource definitions and their groups
linstor resource list             # per-node replica placement and DRBD state
linstor volume list               # per-volume size and device path
```

A healthy 3-way replicated volume shows two `UpToDate` diskful replicas plus one `TieBreaker` (diskless) row in `linstor resource list`, all sharing the same DRBD port.

## Cleanup

Deleting the resource-definition removes every replica, frees the DRBD port, and tears down the backing volumes:

```sh
linstor resource-definition delete myvolume
```
