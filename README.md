# cozystack-blockstor

A Go reimplementation of LINSTOR (controller + satellite) targeting
1:1 REST API compatibility with linstor-csi, piraeus-operator,
ha-controller, and the rest of the LINSTOR client ecosystem. State of
truth lives in Kubernetes CRDs; logic is reconcile-driven.

Status: Phases 1–9 complete (110/110 PLAN.md checkboxes ticked).
The `master` branch carries the production build.

## What's here

- `cmd/` — `controller/` + `satellite/` binaries.
- `pkg/api/v1/` — REST shape types, layer-stack resolver.
- `pkg/rest/` — REST handlers (1:1 with upstream LINSTOR endpoints).
- `pkg/store/` + `pkg/store/k8s/` — InMemory + CRD-backed store, both
  behind the same `store.Store` interface and exercised by a shared
  test suite.
- `pkg/satellite/` — DRBD/LUKS/STORAGE layer reconciler, events2
  observer, snapshot-ship dispatcher (zfs send|recv, thin-send-recv).
- `pkg/storage/{lvm,zfs,loopfile,file}` — provider implementations
  (LVM-thin, LVM-thick, ZFS / ZFS_THIN, loopfile, host file).
- `pkg/luks/` — `cryptsetup` wrapper for the LUKS layer.
- `pkg/drbd/` — `drbdadm` / `drbdsetup` wrappers, .res ConfFileBuilder,
  events2 parser, options resolver.
- `pkg/placer/` — autoplacer (capacity-weighted, anti-affinity, shared-LUN-aware).
- `pkg/dispatcher/` — RD → satellite Apply translator (resolves
  layer_stack, options, passphrases).
- `internal/controller/` — controller-runtime reconcilers (RD, RG, RP,
  Snapshot, Resource, Node).
- `proto/satellite/v1alpha1/` — controller↔satellite gRPC.
- `stand/` — Talos+QEMU dev stand (DRBD, ZFS, LVM extensions baked in).
- `docs/` — `layer-stack.md` (DRBD/LUKS/STORAGE compositions),
  `csi-api-surface.md`.
- `tests/` — `contract/` (oracle diff vs. Java LINSTOR), `e2e/`
  (cluster-side scenarios), `smoke-blockstor.sh`, `burnin-blockstor.sh`.

## Layer stack

blockstor implements LINSTOR's `layer_list` model. RDs declare an ordered
chain — the satellite walks it bottom-up on Apply, top-down on teardown.

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
cmd/               controller + satellite binaries
pkg/               API, REST, store, satellite, storage, drbd, luks, placer, dispatcher
internal/controller/  controller-runtime reconcilers
proto/             gRPC contracts
stand/             Talos+QEMU dev stand
docs/              layer-stack, CSI surface notes
tests/
  contract/        oracle diff vs. Java LINSTOR
  e2e/             cluster-side scenarios (lib.sh + per-scenario .sh)
  smoke-blockstor.sh
  burnin-blockstor.sh
```
