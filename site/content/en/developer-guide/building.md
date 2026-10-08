---
title: "Building"
linkTitle: "Building"
weight: 10
description: "Build the binaries and images, and bring up the dev stand."
---

## Binaries

Blockstor is a Go module, `github.com/cozystack/blockstor`. `make build` regenerates the manifests and deep-copy code, runs `go fmt` and `go vet`, and builds the controller, the satellite, the CLI and the migration tool into `bin/`. The API server builds the usual Go way:

```sh
make build
go build -o bin/apiserver ./cmd/apiserver
```

| Binary | Source | What it is |
|---|---|---|
| `controller` | `cmd/controller` | controller-runtime manager with the controller-side reconcilers. |
| `apiserver` | `cmd/apiserver` | the stateless LINSTOR-compatible REST front end. |
| `satellite` | `cmd/satellite` | the per-node manager that drives DRBD, LUKS and storage. |
| `blockstor` | `cmd/blockstor` | the operator CLI. |
| `linstor-migrate` | `cmd/linstor-migrate` | the LINSTOR database converter. |

Other generators: `make manifests` regenerates the CRDs and RBAC from the kubebuilder markers in `api/v1alpha1`, and `make generate` regenerates the deep-copy code.

## Images

One multi-stage [`Dockerfile`](https://github.com/cozystack/blockstor/blob/main/Dockerfile) builds all three images: the `controller`, `apiserver` and `satellite` targets. The controller and API server images are distroless; the satellite image is Debian-based, because it shells out to `drbdadm`, `lvs`, `zfs` and `cryptsetup`.

Releases are built by [`release.yml`](https://github.com/cozystack/blockstor/blob/main/.github/workflows/release.yml) on a `v*` tag and pushed to `ghcr.io/cozystack/blockstor-{controller,apiserver,satellite}` for `linux/amd64` and `linux/arm64`.

## The dev stand

`stand/` brings up a throwaway Talos cluster in QEMU, with DRBD, ZFS and LVM in the node image, so you can exercise the real data path on your machine.

Host requirements:

- Linux x86_64 with KVM (`/dev/kvm` accessible);
- `talosctl`, `kubectl`, `helm`, `qemu-system-x86_64`;
- the DRBD 9 kernel module loaded on the host;
- about 8 GB of free RAM and 20 GB of disk per cluster.

```sh
make up                 # create the cluster (default name "blockstor")
make piraeus            # linstor-csi in external-controller mode
make blockstor          # build and install blockstor
make smoke-blockstor    # quick end-to-end check
make down
```

Real-disk pools on extra disks:

```sh
make pools
STORPOOL=zfs-thin make smoke-blockstor
```

Several clusters can run side by side, each with its own `10.<slot>.0.0/24` network. Their configuration lands in `.work/<NAME>/`:

```sh
make up NAME=alice
eval "$(make use NAME=alice)"
kubectl get nodes
```

The stand pins every image by digest, so each rebuild rolls the pods.
