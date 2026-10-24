---
title: "Internals"
linkTitle: "Internals"
weight: 40
description: "The rules the code relies on, with pointers into it."
---

This page is about the invariants whose violation would quietly corrupt cluster state. For the user-level picture, see [Architecture](/docs/architecture/). For a tour of the code, follow the call graph from `cmd/controller/main.go` and `cmd/satellite/main.go`.

## Spec and Status

Every blockstor CRD has two halves:

- **Spec** is desired state. Users, REST handlers and the controller's placement write it. The satellite never writes Spec.
- **Status** is observed state. The satellite writes what it reads from the kernel (`drbdsetup events2`, `drbdadm status`, `lvs`), and the controller's allocators write values derived from Spec. The REST API never writes Status.

A whole-object `Update` is unsafe whenever both halves have different writers: a Spec write in flight would overwrite a concurrent Status write, and the other way round. Status goes through the Status subresource. Fields with more than one writer, such as `Resource.Status.DRBDPort` from the controller next to `DiskState` and `CurrentGi` from the satellite, go through Server-Side Apply with separate field managers, `blockstor-controller` and `blockstor-satellite`, so each side owns only its own fields.

| Field | Half | Why |
|---|---|---|
| `Resource.Spec.NodeName` | Spec | Placement target. |
| `Resource.Spec.Flags` | Spec | `DISKLESS` and `TIE_BREAKER` are requested, not observed. |
| `Resource.Spec.StoragePool` | Spec | Written by the allocator. |
| `Resource.Spec.Volumes[i].SeedFromGi` | Spec | DRBD generation ID to stamp on first activation. |
| `Resource.Status.DrbdState`, `InUse` | Status | Kernel state from `events2`. |
| `Resource.Status.Volumes[i].DiskState`, `CurrentGi` | Status | Per-volume kernel state. |
| `Node.Status.ConnectionStatus` | Status | Whether the satellite is alive. |

A field that fits neither half, typically a transient hint, goes into an annotation.

## Writes are patches

Every mutating path, in the REST handlers and in the CLI, goes through the store's `Patch*` entry points. They fetch the current object, apply the caller's change to it, and retry on conflict. A caller hands over a change, never a whole object, so a key another writer added in between is kept. Checks that depend on current state, such as refusing a shrink, run inside the patch against the state the write lands on.

Invariants that must hold for every writer live in the CRD, not in a client: size bounds are `Minimum` and `Maximum` on the field, and the controller-allocated identities (`drbdPort`, `drbdNodeID`, `skipInitialSync`) are settable-once CEL rules. A check that lives in one client is not a check the data is subject to.

## Option inheritance

DRBD configuration resolves from the broadest scope to the narrowest:

```
ControllerConfig -> ResourceGroup -> ResourceDefinition -> Resource
```

The typed resolver is `ResolveDRBDOptions` in `pkg/drbd/typed_resolver.go`. Pointer fields carry the nil-versus-set distinction: nil inherits, and any non-nil value, including `false` and zero, overrides. A merge written as `if *src.X { out.X = src.X }` would drop an explicit `false`; `pkg/drbd/typed_resolver_test.go` pins this.

## Wire shape and CRD shape

Two shapes coexist:

1. The wire shape in `pkg/api/v1`, identical to upstream LINSTOR's REST API, with property bags as `map[string]string`.
2. The CRD shape in `api/v1alpha1`, with typed `Spec.DRBDOptions` and `Spec.ExtraProps` for keys not typed yet.

`pkg/store/k8s/` is the boundary. `drbd_transcode.go` parses wire properties into typed fields on write and emits them back on read, so `golinstor` sees the shape it expects. Unknown `DrbdOptions/*` keys round-trip through `ExtraProps`.

## Skipping the initial sync

Adding a third replica to a two-replica volume would otherwise copy the whole device. The skip pipeline:

1. The satellite's `events2` observer parses `current-uuid` and publishes it as `Resource.Status.Volumes[i].CurrentGi`.
2. When it allocates a new replica, the controller picks the lowest-named `UpToDate` peer's `CurrentGi` and stamps it on the new replica's `Spec.Volumes[i].SeedFromGi`.
3. On first activation the satellite runs `drbdmeta ... set-gi` between `drbdadm create-md` and `drbdadm adjust`, so the first handshake sees the new replica as already in sync.

`tests/e2e/replica-add-no-resync.sh` gates it end to end. Promoting a tiebreaker to a replica with data deliberately does a full sync instead; see row 84 of the [parity deltas](https://github.com/cozystack/blockstor/blob/main/docs/cli-parity-known-deltas.md).

## Reconcilers

Controller side, in `internal/controller/`:

- `ResourceReconciler` allocates the DRBD node ID, port and minor, picks `SeedFromGi`, and promotes a diskless replica that is in active use.
- `ResourceDefinitionReconciler` adds a diskless tiebreaker when a definition has an even number of replicas with data, and sets the quorum policy.
- `NodeReconciler`, `ResourceGroupReconciler`, `StoragePoolReconciler` and `SnapshotReconciler` own the invariants of their kinds.
- Cross-cutting controllers: the node heartbeat watchdog, node label sync, auto-diskful, auto-evict, auto-snapshot, group rebalancing and replica migration.

Satellite side, in `pkg/satellite/controllers/`: each satellite runs controller-runtime reconcilers that watch only the objects whose `spec.nodeName` is its own node, drive the DRBD, LUKS and storage layers, and write observed state back through Status. `pkg/dispatcher/` translates the CRDs into the desired state they apply.

There is no satellite to controller connection. Both sides talk only to the Kubernetes API.
