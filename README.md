# cozystack-blockstor

blockstor is a Kubernetes control plane for LVM and ZFS storage with [DRBD](https://linbit.com/drbd/) replication. It exposes a [LINSTOR](https://linbit.com/linstor/)-compatible REST API so existing clients (linstor-csi, piraeus-operator, ha-controller, golinstor) keep working.

## Why a new implementation?

### Kubernetes-native architecture

- **Reconciliation control plane.** State of truth lives in Kubernetes CRDs; controller and satellite are `controller-runtime` managers with watch-based informers, declarative reconcile loops, and Status SSA. No synchronous fan-out RPC, no central in-memory state, no per-request controller→node polling. Desired/observed convergence is automatic.
- **First-class CRDs.** `Resource`, `ResourceDefinition`, `ResourceGroup`, `StoragePool`, `Snapshot`, `Node`, `PhysicalDevice`, `ControllerConfig` are designed to be read and (where appropriate) written by other operators: cozystack tenant operators, GitOps tooling, custom monitoring/alerting, admission webhooks. Schemas carry kubebuilder enum/min/max validation; multi-writer Status uses Server-Side Apply field managers.
- **Per-node satellite as a controller.** Each satellite is a controller-runtime manager that watches its own slice of CRDs (filtered by `Spec.NodeName`) and writes observed state back via Status SSA directly. No gRPC dispatch from a central controller.

See [`docs/architecture.md`](docs/architecture.md) for the load-bearing design notes.

### Ecosystem fit

Go is the lingua franca of the Kubernetes ecosystem — apiserver, kubelet, etcd, the bulk of CSI drivers, controller-runtime itself. Writing blockstor in Go aligns the project with the tooling, libraries, and contributor base of that ecosystem.

### New functionality

- **Shared-LUN provisioning** with thick LVM + thin qcow2-on-LVM (no filesystem layer), following the design proven in [oVirt VDSM](https://github.com/oVirt/vdsm/blob/master/doc/thin-provisioning.md).
- **VDUSE backend** via `qemu-storage-daemon` for shared-SAN Kubernetes — see the [LVM/qcow shared-SAN write-up](https://blog.deckhouse.io/lvm-qcow-csi-driver-shared-san-kubernetes-81455201590e) for the design rationale.
- **Bring-your-own-key (BYOK) encryption** with operator-managed Secret references in the CRD spec rather than a controller-owned passphrase bag.

## Acknowledgements

blockstor implements a LINSTOR-compatible REST API and was inspired by LINBIT's work on DRBD and LINSTOR, and by the wider DRBD / LINSTOR / Piraeus community.

## What's here

- `cmd/` — three binaries from the Phase 11.x apiserver split:
  - `cmd/controller/` — controller-runtime manager hosting the RD / RG / RP / Snapshot / Resource / Node reconcilers. The LINSTOR-compatible REST surface is disabled by default since the Phase 11.x apiserver split; pass `--enable-rest-api` (with `--rest-bind-address`) for the legacy single-binary deployment.
  - `cmd/satellite/` — per-node controller-runtime manager that watches its own slice of CRDs and reconciles DRBD / LUKS / STORAGE layers + drives the events2 observer.
  - `cmd/apiserver/` — stateless LINSTOR-compatible REST front end backed by the CRD store; runs as a 3-replica Deployment.
- `pkg/api/v1/` — REST shape types, layer-stack resolver.
- `pkg/rest/` — REST handlers (LINSTOR-compatible).
- `pkg/store/` + `pkg/store/k8s/` — InMemory + CRD-backed store, both behind the same `store.Store` interface and exercised by a shared test suite.
- `pkg/satellite/` — DRBD/LUKS/STORAGE layer reconciler, snapshot-ship dispatcher.
- `pkg/satellite/controllers/` — controller-runtime reconcilers on the satellite (Resource, StoragePool, Snapshot, PhysicalDevice + events2 observer Runnable).
- `pkg/storage/{lvm,zfs,loopfile,file}` — provider implementations (LVM-thin, LVM-thick, ZFS / ZFS_THIN, loopfile, host file).
- `pkg/luks/` — `cryptsetup` wrapper for the LUKS layer.
- `pkg/drbd/` — `drbdadm` / `drbdsetup` wrappers, .res ConfFileBuilder, events2 parser, options resolver.
- `pkg/placer/` — autoplacer (capacity-weighted, anti-affinity, shared-LUN-aware).
- `pkg/dispatcher/` — CRD → DesiredResource translator (resolves layer_stack, options, passphrases). Used by the satellite-side c-r reconcilers.
- `internal/controller/` — controller-side controller-runtime reconcilers (RD, RG, RP, Snapshot, Resource, Node).
- `stand/` — Talos+QEMU dev stand (DRBD, ZFS, LVM extensions baked in).
- `docs/` — `architecture.md`, `layer-stack.md` (DRBD/LUKS/STORAGE compositions), `csi-api-surface.md`.
- `tests/` — `contract/` (REST contract conformance), `e2e/` (cluster-side scenarios), `smoke-blockstor.sh`, `burnin-blockstor.sh`.

## Layer stack

blockstor implements an ordered layer-stack model. RDs declare a chain — the satellite walks it bottom-up on Apply, top-down on teardown.

| Stack                       | Use case                                     |
|-----------------------------|----------------------------------------------|
| `["DRBD","STORAGE"]`        | Default. Replicated PVC.                     |
| `["LUKS","STORAGE"]`        | Single-replica encrypted PVC, no DRBD.       |
| `["DRBD","LUKS","STORAGE"]` | Encrypted at-rest + replicated.              |
| `["STORAGE"]`               | Single-replica local mode (cache, scratch). |

See `docs/layer-stack.md` for the full operator-facing reference.

## Requirements (host)

- Linux x86_64 with KVM enabled (`/dev/kvm` accessible)
- `talosctl`, `kubectl`, `helm`, `qemu-system-x86_64`
- DRBD9 kernel module loaded on host (`modprobe drbd`)
- ~8 GB free RAM and ~20 GB disk per cluster

## Quick start

```sh
# Single cluster (default name "blockstor")
make up
make piraeus
make blockstor              # install blockstor controller + satellite DaemonSet
make smoke-blockstor
make down

# Real-disk pools (ZFS + LVM-thin) on extra disks
make pools
STORPOOL=zfs-thin make smoke-blockstor

# Multiple parallel clusters — each gets its own 10.<slot>.0.0/24 CIDR
make up   NAME=alice
make up   NAME=bob
```

Each cluster's config lands under `.work/<NAME>/` (talos+kube).

## Selecting a stand from your shell

```sh
eval "$(make use NAME=alice)"
kubectl get nodes
```

## e2e scenarios

```sh
make e2e-list                                # enumerate scenarios
make e2e NAME=alice SCENARIO=tiebreaker
make e2e NAME=alice SCENARIO=luks-layer
```

Scenarios live under `tests/e2e/` and each takes a `WORK_DIR` arg.

## Layout

```
cmd/               controller + satellite + apiserver binaries (Phase 11.x split)
pkg/               API, REST, store, satellite, storage, drbd, luks, placer, dispatcher
internal/controller/  controller-side controller-runtime reconcilers
pkg/satellite/controllers/  satellite-side controller-runtime reconcilers
stand/             Talos+QEMU dev stand
docs/              architecture, layer-stack, CSI surface notes
tests/
  contract/        REST contract conformance
  e2e/             cluster-side scenarios (lib.sh + per-scenario .sh)
  smoke-blockstor.sh
  burnin-blockstor.sh
```
