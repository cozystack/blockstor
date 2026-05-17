# Blockstor architecture

This document captures load-bearing invariants the codebase relies
on. It is NOT a tour of the code — for that, follow the call graph
from `cmd/controller/main.go` and `cmd/satellite/main.go`
(satellite). The pieces below are the rules whose violation would
quietly corrupt cluster state.

## Spec / Status discipline

A blockstor CRD has two halves:

* **Spec** is _desired state_. Operators, REST handlers, and the
  controller's own placement logic write here. The satellite
  **never writes Spec**.
* **Status** is _observed state_. The satellite (and the
  controller's allocators that derive values from Spec) write here.
  The user-facing REST API never writes Status — it returns it
  read-only.

The rule:

> Anything the satellite reads from the kernel (`drbdsetup events2`,
> `drbdadm status`, `lvs`, …) lives on **Status**. Anything the
> operator or controller asks the satellite to do lives on **Spec**.

A naive whole-object `Update` is unsafe whenever both halves are
written by different actors — a Spec mutation in flight would
clobber a concurrent Status write and vice versa. Status writes
**must** go through the Status subresource (`Status().Update()`);
Spec writes use the regular `Update()` path.

This rule is enforced by code review for now; with Phase 10.2
landed, the satellite-side reconciler's writes naturally route
through Status only, making the invariant mechanical.

### Field placement cheatsheet

| Field | Half | Rationale |
|---|---|---|
| `Resource.Spec.NodeName` | Spec | Operator-chosen placement target. |
| `Resource.Spec.Flags` | Spec | DISKLESS / TIE_BREAKER are operator-controlled. |
| `Resource.Spec.StoragePool` | Spec | Allocator output written by controller. |
| `Resource.Spec.Volumes[i].SeedFromGi` | Spec | Controller-picked DRBD GI to stamp on first activation. |
| `Resource.Status.InUse` | Status | Reflects `drbdsetup status` role. |
| `Resource.Status.DrbdState` | Status | Reflects `events2` connection state. |
| `Resource.Status.DRBDPort` / `DRBDMinor` / `DRBDNodeID` | Status | Allocator-derived; immutable per replica once set. |
| `Resource.Status.Volumes[i].DiskState` | Status | Per-volume kernel state. |
| `Resource.Status.Volumes[i].CurrentGi` | Status | DRBD generation identifier observed by the satellite. |
| `Node.Status.ConnectionStatus` | Status | Set on Hello — reflects whether the satellite has dialled in. |

A field that fits neither half (rare — typically a transient
debounce hint) lives in an annotation, not Spec or Status.

### Multi-writer Status (server-side apply)

Some Status fields are written by **both** the controller (e.g.
allocator output → `DRBDPort`) and the satellite (e.g.
`DiskState`, `CurrentGi`). A regular `.Status().Update()` from
either side rewrites the **whole** Status subresource, which can
clobber the other side's writes that landed between Get and
Update.

Phase 10.2 routed those writes through Kubernetes Server-Side
Apply with distinct field managers (`blockstor-controller`,
`blockstor-satellite`) so each side only touches the fields it
owns. See `pkg/store/k8s/resources.go` `SetState` for the
satellite-side writer; the controller-side writer lives in
`internal/controller`.

## Hierarchy resolver

DRBD configuration follows an upstream-LINSTOR-shaped override
chain:

```
Controller → ResourceGroup → ResourceDefinition → Resource
   (broadest)                                      (narrowest)
```

Lower scopes override higher scopes per non-nil field. The typed
implementation lives in `pkg/drbd/typed_resolver.go`
(`ResolveDRBDOptions`); the legacy string-keyed implementation
(`ResolveOptions`) is still used as a fallback for any
`Spec.Props` data not yet migrated to the typed `DRBDOptions`
struct. See `internal/controller/resource_controller.go`'s
`resolveEffectiveProps` for the merge.

`*int32` and `*bool` use nil-vs-set discipline:

* `nil` means "not overridden at this scope, inherit from parent".
* Any non-nil value (including the zero value) means "explicitly
  set, do not inherit".

A regression that did `if *src.X { out.X = src.X }` would silently
drop explicit-`false` overrides, e.g. an RD that intentionally
sets `AllowTwoPrimaries=false` would inherit a parent RG's `true`.
The pinning tests for this are in `pkg/drbd/typed_resolver_test.go`.

## Wire format vs CRD storage

Two shapes coexist in the codebase:

1. **Wire shape** — `pkg/api/v1` types, identical to upstream
   LINSTOR's REST API. golinstor and external callers see this
   verbatim. Property bags live as `props map[string]string`.
2. **CRD shape** — `api/v1alpha1` types, the typed structures
   blockstor persists in Kubernetes. DRBD configuration lives in
   `Spec.DRBDOptions` (typed) + `Spec.ExtraProps` (forward-compat
   for keys we haven't typed yet).

The k8s store (`pkg/store/k8s/`) is the boundary. Its
`drbd_transcode.go` parses the wire `props` bag into typed CRD
fields on Create/Update; the inverse direction re-emits typed
fields back into `props` on GET so golinstor sees the unchanged
shape. Unknown DrbdOptions/* keys round-trip through ExtraProps
without loss.

## DRBD initial-sync skip

Adding a third replica to a 2-replica resource without
intervention would trigger a full resync of the entire backing
device — hours on multi-TiB volumes. The skip pipeline (Phase
8.1):

1. Satellite's `events2` observer parses `current-uuid` from
   each device frame and surfaces it in Status as
   `Resource.Status.Volumes[i].CurrentGi`.
2. Controller's `ensureSeedFromGi` picks the lowest-named
   UpToDate peer's CurrentGi when allocating a new replica and
   stamps it on the new replica's `Spec.Volumes[i].SeedFromGi`.
3. Dispatcher threads SeedFromGi through the satellite gRPC
   contract (`DesiredVolume.seed_from_gi`).
4. Satellite reconciler's `applyDRBD` runs

   ```
   drbdmeta --force <res>/<vol> v09 <device> internal set-gi <gi>:<gi>:0:0
   ```

   between `drbdadm create-md` and `drbdadm adjust` on first
   activation. With matching `current_uuid`+`bitmap_uuid` the
   GI handshake on first connect sees the new peer as
   already-in-sync and skips the full sync.

The pipeline is end-to-end gated by
`tests/e2e/replica-add-no-resync.sh`.

## Controllers and reconcilers

Controller-side reconcilers (`internal/controller/`) — primary
CRD owners:

* `ResourceReconciler` (`resource_controller.go`, ~1245 LOC) —
  Resource CRDs. Allocates DRBD node-id / port / minor; picks
  SeedFromGi; promotes DISKLESS to diskful when actively used;
  writes desired-state to the CRD for the satellite to pick up.
* `ResourceDefinitionReconciler`
  (`resourcedefinition_controller.go`, ~917 LOC) — RD CRDs.
  Auto-creates `DISKLESS+TIE_BREAKER` witnesses when an RD has
  even diskful replicas; sets the resource-level quorum policy.
* `NodeReconciler` (`node_controller.go`, ~239 LOC) — Node
  CRDs. Owns the Node bookkeeping that the heartbeat watchdog
  (Phase 10.6) relies on.
* `ResourceGroupReconciler` (`resourcegroup_controller.go`,
  ~132 LOC), `StoragePoolReconciler`
  (`storagepool_controller.go`, ~68 LOC),
  `SnapshotReconciler` (`snapshot_controller.go`, ~60 LOC) —
  smaller reconcilers that own CRD-level invariants for their
  kind. The bulk of placement / capacity / snapshot-ship logic
  lives in the dispatcher + satellite controllers below.

Sibling controllers in the same package handle cross-cutting
concerns:

* `node_heartbeat_controller.go` (~243 LOC) — Phase 10.6
  heartbeat watchdog; flips Node `ConnectionStatus` to OFFLINE
  when satellites stop refreshing their lease.
* `node_label_sync_controller.go` (~326 LOC) — propagates
  selected k8s Node labels to the blockstor Node CRD.
* `auto_diskful_controller.go` (~493 LOC) — DISKLESS → diskful
  promotion when a resource is actively used.
* `auto_evict_controller.go` (~452 LOC) — evicts replicas from
  unhealthy / drained nodes.
* `autosnapshot_controller.go` (~536 LOC) — periodic snapshot
  scheduling per RG/RD policy.
* `rg_rebalance_controller.go` (~539 LOC) — rebalances RG
  membership / spawns shortfall replicas.
* `resource_migration_controller.go` (~217 LOC) — migrates a
  Resource between nodes.

Satellite-side reconcilers live in `pkg/satellite/controllers/`
and watch the apiserver directly via controller-runtime (Phase
10.1). They drive DRBD / LUKS / STORAGE layers and write
observed state back via Status SSA.

Phase 10.1 lifted the satellite's gRPC-driven reconciler logic
into those `pkg/satellite/controllers/` reconcilers. That
change retired `pkg/satellitecontroller/`, but `pkg/dispatcher/`
is still live: it provides the CRD → DesiredResource translator
(resolves layer_stack, options, passphrases) and is imported by
the satellite-side reconciler at
`pkg/satellite/controllers/resource.go` (and exercised by
`pkg/dispatcher/dispatcher_test.go`).
