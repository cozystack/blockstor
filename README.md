![Blockstor](img/blockstor-logo-black.svg#gh-light-mode-only)
![Blockstor](img/blockstor-logo-white.svg#gh-dark-mode-only)

# Blockstor

**Blockstor** is a Kubernetes control plane for LVM and ZFS storage with [DRBD](https://linbit.com/drbd/) replication. It speaks a [LINSTOR](https://linbit.com/linstor/)-compatible REST API, so the clients you already use — `linstor-csi`, `piraeus-operator`, `ha-controller`, `golinstor`, the `linstor` CLI — keep working unchanged.

The difference is what's underneath. Instead of a central controller with its own database and synchronous RPC to every node, blockstor is built the way Kubernetes operators are built: the desired state lives in CRDs, and a set of `controller-runtime` reconcilers drive the cluster toward it. There is no external database to back up, no in-memory state to lose on restart, and no controller→node polling to fall behind.

## Why?

### Kubernetes-native architecture

- **Reconciliation control plane.** State of truth lives in Kubernetes CRDs; controller and satellite are `controller-runtime` managers with watch-based informers, declarative reconcile loops, and Status SSA. No synchronous fan-out RPC, no central in-memory state, no per-request controller→node polling. Desired/observed convergence is automatic.
- **First-class CRDs.** `Resource`, `ResourceDefinition`, `ResourceGroup`, `StoragePool`, `Snapshot`, `Node`, `PhysicalDevice` are designed to be read and (where appropriate) written by other operators: cozystack tenant operators, GitOps tooling, custom monitoring/alerting, admission webhooks. Schemas carry kubebuilder enum/min/max validation; multi-writer Status uses Server-Side Apply field managers.
- **Per-node satellite as a controller.** Each satellite is a controller-runtime manager that watches its own slice of CRDs (filtered by `Spec.NodeName`) and writes observed state back via Status SSA directly. No gRPC dispatch from a central controller.

### Ecosystem fit

- **Built in Go.** Go is the lingua franca of the Kubernetes ecosystem — apiserver, kubelet, etcd, the bulk of CSI drivers, controller-runtime itself — so blockstor shares its tooling, libraries, and contributor base.
- **Small footprint.** The controller, apiserver, and satellite are statically-linked Go binaries — no JVM and no language runtime to ship — and DRBD is a Linux kernel module rather than a userspace daemon. The images are multi-arch (`linux/amd64`, `linux/arm64`) and cross-compile cleanly, so blockstor fits on edge and small ARM nodes where a multi-hundred-megabyte Java runtime would not.

## Status

What works today:

- [x] Replicated volumes over DRBD, on LVM, LVM-thin, ZFS, ZFS-thin, or file backends
- [x] Running **without DRBD** — plain local storage (single-replica diskful or diskless)
- [x] **LUKS encryption** — volume-level encryption at rest
- [x] Autoplacement with constraints (zones, node properties, replicas-on-different)
- [x] TieBreaker + quorum policies
- [x] **Snapshots** — create, restore as a new resource, roll back, and clone
- [x] Intra-cluster snapshot shipping (`zfs send`/`recv`, `thin-send-recv`) for clone / add-replica
- [x] Online volume resize
- [x] Device-pool creation from physical disks (`physical-storage create-device-pool`)
- [x] LINSTOR-compatible REST API for the whole client ecosystem, served over mTLS

Not implemented — the API answers these with `501 Not Implemented`:

- [ ] Cross-cluster snapshot shipping (disaster recovery)
- [ ] Backup create / restore / ship / abort, and the backup queue
- [ ] Schedules (cron-driven backups)
- [ ] Remote backends — S3, LINSTOR remotes
- [ ] Extra storage providers — SPDK, NVMe-oF, OpenFlex, Exos

On the roadmap:

- [ ] **Bring-your-own-key encryption** — operator-managed Secret references in the spec, instead of a controller-owned passphrase bag
- [ ] **Migration tool from LINSTOR** — adopt an existing LINSTOR cluster's resources into blockstor in place
- [ ] [Shared-LUN provisioning](https://github.com/oVirt/vdsm/blob/master/doc/thin-provisioning.md) — thick LVM plus thin qcow2-on-LVM, no filesystem layer
- [ ] [VDUSE backend](https://blog.deckhouse.io/lvm-qcow-csi-driver-shared-san-kubernetes-81455201590e) via `qemu-storage-daemon`, for shared-SAN Kubernetes

## Getting started

Blockstor installs onto an existing cluster and is driven with the standard `linstor` client and piraeus `linstor-csi`. The full walkthrough — installing the control plane, registering nodes and pools, and wiring up CSI — lives in **[`docs/usage.md`](docs/usage.md)**.

The three images are published to GHCR on every release:

- `ghcr.io/cozystack/blockstor-controller` — the reconcilers
- `ghcr.io/cozystack/blockstor-apiserver` — the LINSTOR-compatible REST API (mTLS)
- `ghcr.io/cozystack/blockstor-satellite` — the per-node DRBD / storage agent

## Documentation

- [`docs/usage.md`](docs/usage.md) — install and operate blockstor with the `linstor` client + piraeus/linstor-csi.
- [`docs/architecture.md`](docs/architecture.md) — the load-bearing design decisions.
- [`docs/layer-stack.md`](docs/layer-stack.md) — DRBD / LUKS / STORAGE compositions.
- [`AGENTS.md`](AGENTS.md) — repository layout and the local Talos+QEMU dev stand, for contributors.

## Acknowledgements

Blockstor was inspired by LINBIT's [LINSTOR](https://linbit.com/linstor/), and it operates [DRBD](https://linbit.com/drbd/) to provide block-level replication. It deliberately speaks LINSTOR's API so that the rich ecosystem the LINSTOR community has built keeps working unchanged. Heartfelt thanks to LINBIT and to the wider DRBD / LINSTOR / Piraeus community.

## License

Blockstor is licensed under [Apache 2.0](LICENSE). The code is provided as-is with no warranties.

Third-party Go modules and their licenses are catalogued in [THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md); the runtime dependency graph is constrained to an Apache-2.0–compatible allowlist (Apache-2.0, BSD-2-Clause, BSD-3-Clause, MIT, MPL-2.0, ISC) by the license-scan CI gate in [`.github/workflows/license-check.yml`](.github/workflows/license-check.yml). For interoperability with LINSTOR clients, type definitions are sourced from the Apache-2.0 [`golinstor`](https://github.com/LINBIT/golinstor) library; blockstor does not copy or generate code from upstream LINSTOR (GPL) sources.

## Trademarks

LINSTOR, LINBIT, and DRBD are trademarks or registered trademarks of LINBIT. Blockstor is an independent project and is not affiliated with, endorsed by, or sponsored by LINBIT.
