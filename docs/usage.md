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

3. **Workloads** — the controller, apiserver, and satellite DaemonSet. The manifests in `stand/` carry an `__REGISTRY__/<image>:dev` placeholder for the dev stand; for a real cluster set the `image:` fields to the published tags, e.g. `ghcr.io/cozystack/blockstor-controller:<TAG>`, `ghcr.io/cozystack/blockstor-apiserver:<TAG>`, `ghcr.io/cozystack/blockstor-satellite:<TAG>`:

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

## Dynamic provisioning: piraeus components + the `linstor-csi` driver

So far you have driven blockstor by hand with the `linstor` client. To let Kubernetes provision DRBD-replicated PVs on demand, point the LINBIT CSI driver (`linstor-csi`) at the blockstor apiserver and create a `StorageClass`. The CSI driver speaks the same LINSTOR REST API the `linstor` client does, so blockstor is a drop-in controller backend for it.

### What blockstor replaces — deploy the client side only

blockstor **is** the LINSTOR controller plus the satellites: the controller binary hosts the reconcilers, the apiserver serves the REST API, and the satellite DaemonSet drives DRBD on each node. piraeus is normally a full LINSTOR distribution (operator + `linstor-controller` + `linstor-satellite` + the CSI driver). When running against blockstor you take only the **client-side** pieces from piraeus and skip everything blockstor already provides:

| piraeus component | Against blockstor |
|-------------------|-------------------|
| `linstor-controller` | **Skip** — blockstor's controller + apiserver already are the controller. |
| `linstor-satellite` | **Skip** — blockstor ships its own satellite DaemonSet (`stand/blockstor-satellite-daemonset.yaml`). |
| `linstor-csi` (csi-controller + csi-node) | **Deploy** — this is what provisions PVs. Point it at the blockstor apiserver. |
| HA / affinity controller, `drbd-reactor` | Optional — deploy if you want failover/affinity. Point them at the apiserver the same way as the CSI driver (mTLS :3371 + client cert). |

> The dev stand's `make piraeus` (`stand/install-piraeus.sh`) installs piraeus as a **separate, parallel** LINSTOR stack for coexistence testing — it brings up piraeus's own controller and satellites and does **not** point them at blockstor. Do not copy that path for a blockstor-backed cluster; use the wiring below instead.

### Point `linstor-csi` at the blockstor apiserver

The CSI driver's controller endpoint is the apiserver's mTLS Service on port **3371** — the same `blockstor-controller` Service the `linstor` client resolves (the plain-HTTP debug port 3370 is pod-local and is not an option here). `linstor-csi` wraps `golinstor`, which upgrades the endpoint to HTTPS and presents a client certificate when the `LS_USER_*` / `LS_ROOT_CA` variables point at PEM files. The exact wiring (verbatim from `stand/csi-sanity-job.yaml`, the in-repo reference for a CSI client against blockstor):

```yaml
image: quay.io/piraeusdatastore/piraeus-csi:v1.10.1
args:
  - --csi-endpoint=$(CSI_ENDPOINT)
  - --node=$(KUBE_NODE_NAME)            # downward-API spec.nodeName; must match a blockstor Node name
  - --linstor-endpoint=$(LS_CONTROLLERS)
env:
  - {name: CSI_ENDPOINT,        value: "unix:///csi/csi.sock"}
  - name: KUBE_NODE_NAME
    valueFrom: {fieldRef: {fieldPath: spec.nodeName}}
  # mTLS endpoint — the only port the Service exposes.
  - {name: LS_CONTROLLERS,      value: "https://blockstor-controller.blockstor-system.svc:3371"}
  - {name: LS_USER_CERTIFICATE, value: "/etc/linstor/client/tls.crt"}
  - {name: LS_USER_KEY,         value: "/etc/linstor/client/tls.key"}
  - {name: LS_ROOT_CA,          value: "/etc/linstor/client/ca.crt"}
volumeMounts:
  - {name: client-tls, mountPath: /etc/linstor/client, readOnly: true}
volumes:
  - name: client-tls
    secret:
      secretName: blockstor-apiserver-client-tls
```

Apply the same `LS_CONTROLLERS` / `LS_USER_*` / `LS_ROOT_CA` env and the same `client-tls` mount to **both** the csi-controller Deployment and the csi-node DaemonSet. If you deploy `linstor-csi` through the piraeus operator instead of plain manifests, set the cluster's external controller URL to the same `https://blockstor-controller.blockstor-system.svc:3371` and mount the same client Secret — verify the exact field name for your piraeus-operator version.

### mTLS client certificate via cert-manager

The apiserver runs a `RequireAndVerifyClientCert` TLS listener and verifies every client cert against the CA it was issued from, so the CSI driver needs a client cert from that same CA. `stand/blockstor-apiserver-tls.yaml` provisions the whole chain with cert-manager:

```
ca-bootstrapper (self-signed Issuer)
  └─> blockstor-api-ca (CA Certificate, 10y)
        └─> blockstor-api-ca (CA Issuer)
              ├─> blockstor-apiserver-server  (serving cert; Service DNS SANs) → Secret blockstor-apiserver-server-tls
              └─> blockstor-apiserver-client   (client cert for in-cluster consumers) → Secret blockstor-apiserver-client-tls
```

The `blockstor-apiserver-client` Certificate emits Secret `blockstor-apiserver-client-tls` carrying `tls.crt`, `tls.key` **and** `ca.crt`. Mount that Secret into the CSI pods (as above): `tls.crt`/`tls.key` authenticate the client to the apiserver, and `ca.crt` lets the client verify the apiserver's serving cert. cert-manager rotates these in place and the apiserver hot-reloads them without a restart, so no reloader annotations are needed. The serving cert's SANs already cover the `blockstor-controller` Service name, so dialing `https://blockstor-controller.blockstor-system.svc:3371` validates cleanly.

If the CSI driver runs in a different namespace from the apiserver, replicate the client Secret there (e.g. issue a second `Certificate` from the same `blockstor-api-ca` Issuer into that namespace) — a Secret is namespace-scoped and the Issuer must be reachable.

### Create a StorageClass and a PVC

Use the `linstor.csi.linbit.com` provisioner. The parameter keys are the standard `linstor.csi.linbit.com/*`-prefixed CSI keys; the values map onto blockstor's resource-group / storage-pool / placement semantics. The shape below mirrors `tests/smoke.sh`:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: blockstor-replicated
provisioner: linstor.csi.linbit.com
parameters:
  linstor.csi.linbit.com/storagePool: "data"     # the blockstor storage pool to place diskful replicas in
  linstor.csi.linbit.com/placementCount: "2"      # number of diskful replicas (autoplacer adds a tiebreaker)
  # Optional — pin volumes to a resource group you pre-created with `linstor resource-group create`.
  # The default RG is the canonical "DfltRscGrp".
  # linstor.csi.linbit.com/resourceGroup: "mygroup"
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
---
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

`storagePool` and `placementCount` are the keys exercised by blockstor's own smoke and csi-sanity tests, and `resourceGroup` is honored verbatim (the default RG is `DfltRscGrp`). Other `linstor.csi.linbit.com/*` keys that upstream `linstor-csi` supports — e.g. `layerList`, `disklessStoragePool`, `allowRemoteVolumeAccess` — are passed straight through to the LINSTOR REST API; verify the ones you rely on against your deployment before depending on them.

### Verify

```sh
kubectl get pvc blockstor-test                 # PVC should bind once a consumer schedules it
kubectl get pv                                  # the bound PV's name is the LINSTOR resource name (pvc-<uuid>)
```

Then confirm the resource shows up through the `linstor` client (port-forward or in-cluster, as above):

```sh
linstor resource-definition list   # a pvc-<uuid> RD appears
linstor resource list              # placementCount diskful replicas (UpToDate) + a TieBreaker, sharing one DRBD port
linstor volume list                # the backing volume and its device path
```

A bound PVC plus an `UpToDate` resource in `linstor resource list` confirms the CSI driver is provisioning against blockstor end to end.

## Cleanup

Deleting the resource-definition removes every replica, frees the DRBD port, and tears down the backing volumes:

```sh
linstor resource-definition delete myvolume
```

For CSI-provisioned volumes, delete the PVC instead (`kubectl delete pvc blockstor-test`) — the CSI driver issues the resource-definition delete for you.
