# Contributing to blockstor

This file orients contributors (and coding agents) around the repository: where
things live, how to bring up a local cluster, and how to run the tests. For what
blockstor *is*, start with the [README](README.md); for the design rationale, see
[`docs/architecture.md`](docs/architecture.md). Claude-specific working rules live
in [`CLAUDE.md`](CLAUDE.md).

## Licensing — Apache 2.0, clean-room

Blockstor is Apache 2.0. **Do not copy structure or code from GPL sources** —
specifically `linstor-server` (Java) and `drbd-utils`. You may read GPL sources
for *behavioural understanding only* (what they do, never how they're written),
and you must not reproduce their structure in blockstor.

Apache-2.0 references that are safe to study and adapt:

- `drbd-reactor` (Apache 2.0) — events2 parsing, DRBD state enums
- `linstor-csi` (Apache 2.0) — error-envelope patterns
- The public DRBD-9 protocol documentation and the `drbdsetup` / `drbdadm` man pages

When in doubt, work clean-room: one person reads the GPL source to write a
behavioural spec, and a different person implements the Go from that spec
without ever seeing the original.

## Repository layout

- `cmd/` — three binaries built from one multi-stage `Dockerfile`:
  - `cmd/controller/` — controller-runtime manager hosting the RD / RG / RP / Snapshot / Resource / Node reconcilers. The LINSTOR-compatible REST surface is off by default; pass `--enable-rest-api` (with `--rest-bind-address`) for the legacy single-binary deployment.
  - `cmd/satellite/` — per-node manager that watches its own slice of CRDs and reconciles the DRBD / LUKS / STORAGE layers, plus the events2 observer.
  - `cmd/apiserver/` — stateless LINSTOR-compatible REST front end backed by the CRD store; runs as a 3-replica Deployment.
- `pkg/api/v1/` — REST shape types and the layer-stack resolver.
- `pkg/rest/` — REST handlers (LINSTOR-compatible).
- `pkg/store/` + `pkg/store/k8s/` — InMemory and CRD-backed stores, both behind the same `store.Store` interface and exercised by one shared test suite.
- `pkg/satellite/` — DRBD / LUKS / STORAGE layer reconciler and the snapshot-ship dispatcher.
- `pkg/satellite/controllers/` — satellite-side controller-runtime reconcilers (Resource, StoragePool, Snapshot, PhysicalDevice) plus the events2 observer Runnable.
- `pkg/storage/{lvm,zfs,loopfile,file}` — provider implementations (LVM-thin, LVM-thick, ZFS / ZFS_THIN, loopfile, host file).
- `pkg/luks/` — `cryptsetup` wrapper for the LUKS layer.
- `pkg/drbd/` — `drbdadm` / `drbdsetup` wrappers, the `.res` ConfFileBuilder, the events2 parser, and the options resolver.
- `pkg/placer/` — the autoplacer (capacity-weighted, anti-affinity, shared-LUN-aware).
- `pkg/dispatcher/` — CRD → DesiredResource translator (resolves layer stack, options, passphrases); used by the satellite reconcilers.
- `internal/controller/` — controller-side reconcilers (RD, RG, RP, Snapshot, Resource, Node).
- `stand/` — the Talos+QEMU dev stand (DRBD, ZFS, LVM extensions baked in).
- `docs/` — `architecture.md`, `layer-stack.md`, `usage.md`, `csi-api-surface.md`, and audit notes.
- `tests/` — `contract/` (REST contract conformance), `e2e/` (cluster-side scenarios), `smoke-blockstor.sh`, `burnin-blockstor.sh`.

```
cmd/                          controller + satellite + apiserver binaries
pkg/                          API, REST, store, satellite, storage, drbd, luks, placer, dispatcher
internal/controller/          controller-side reconcilers
pkg/satellite/controllers/    satellite-side reconcilers
stand/                        Talos+QEMU dev stand
docs/                         architecture, layer-stack, usage, CSI surface
tests/
  contract/                   REST contract conformance
  e2e/                        cluster-side scenarios (lib.sh + per-scenario .sh)
  smoke-blockstor.sh
  burnin-blockstor.sh
```

## The dev stand

The stand brings up a throwaway Talos+QEMU cluster with DRBD, ZFS and LVM baked
into the node image, so you can exercise the real data path locally.

### Host requirements

- Linux x86_64 with KVM enabled (`/dev/kvm` accessible)
- `talosctl`, `kubectl`, `helm`, `qemu-system-x86_64`
- The DRBD9 kernel module loaded on the host (`modprobe drbd`)
- ~8 GB free RAM and ~20 GB disk per cluster

### Quick start

```sh
# Single cluster (default name "blockstor")
make up
make piraeus
make blockstor              # install the controller + satellite DaemonSet
make smoke-blockstor
make down

# Real-disk pools (ZFS + LVM-thin) on extra disks
make pools
STORPOOL=zfs-thin make smoke-blockstor

# Multiple parallel clusters — each gets its own 10.<slot>.0.0/24 CIDR
make up   NAME=alice
make up   NAME=bob
```

Each cluster's config lands under `.work/<NAME>/` (talos + kube).

### Selecting a stand from your shell

```sh
eval "$(make use NAME=alice)"
kubectl get nodes
```

### e2e scenarios

```sh
make e2e-list                                # enumerate scenarios
make e2e NAME=alice SCENARIO=tiebreaker
make e2e NAME=alice SCENARIO=luks-layer
```

Scenarios live under `tests/e2e/` and each takes a `WORK_DIR` argument.

## Tests

`go test ./...` runs the unit suite (the envtest-backed integration tests skip
cleanly when `KUBEBUILDER_ASSETS` is unset). `make lint` must be clean before you
push. The full test pyramid — unit, contract, integration, e2e, and the
operator-CLI replay harness — is described in [`CLAUDE.md`](CLAUDE.md).
