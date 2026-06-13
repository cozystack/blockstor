// SPDX-License-Identifier: Apache-2.0

/*
Copyright 2026 Cozystack contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package store defines the persistence interface used by REST handlers.
//
// In Phase 1 we use an in-memory implementation under InMemory{} so endpoints
// can be wired and tested without bringing up Kubernetes. In Phase 2 a
// CRD-backed implementation lives next to it and the controller switches to
// it via flag. The interface is the seam.
package store

import (
	"context"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// Sentinel errors. REST handlers map these to HTTP statuses (404, 409, …).
var (
	// ErrNotFound is returned when the requested object does not exist.
	ErrNotFound = errors.New("object not found")
	// ErrAlreadyExists is returned when creating an object that already exists.
	ErrAlreadyExists = errors.New("object already exists")
)

// NodeStore persists Node objects. Create/Update take pointers so callers
// don't pay for a copy of the (~100-byte) Node value through the interface
// boundary; the implementation must defensively copy if it stores a long
// reference.
type NodeStore interface {
	List(ctx context.Context) ([]apiv1.Node, error)
	Get(ctx context.Context, name string) (apiv1.Node, error)
	Create(ctx context.Context, n *apiv1.Node) error
	Update(ctx context.Context, n *apiv1.Node) error
	Delete(ctx context.Context, name string) error

	// SetConnectionStatus writes node.Status.ConnectionStatus directly
	// via the Status subresource so it survives a subsequent Spec
	// Update. linstor-csi's `linstor-wait-node-online` init container
	// polls /v1/nodes/<name> for connection_status:"ONLINE"; this is
	// where the satellite's gRPC Hello surfaces that state.
	SetConnectionStatus(ctx context.Context, name, status string) error

	// PatchNetInterfaces runs `mutate` against the freshly-fetched
	// current NetInterface list and persists the returned slice. The
	// store handles Get + retry-on-conflict; `mutate` is re-invoked
	// against a re-fetched current state on every conflict retry, so
	// disjoint concurrent edits all converge (Bug 201).
	//
	// Distinct from `Update(...)` which applies a wire-side snapshot
	// wholesale and silently drops concurrent peer additions.
	PatchNetInterfaces(ctx context.Context, name string, mutate func([]apiv1.NetInterface) ([]apiv1.NetInterface, error)) error

	// PatchProps runs `mutate` against the freshly-fetched current
	// Props map and persists the mutated map. Same retry-on-conflict
	// semantics as PatchNetInterfaces; `mutate` is re-applied to the
	// re-fetched current state on every retry (Bug 201).
	PatchProps(ctx context.Context, name string, mutate func(map[string]string) error) error

	// PatchNodeSpec runs `mutate` against the freshly-fetched Node
	// wire value and persists the result. On 409 the
	// fetch+mutate+patch cycle re-runs against fresh state — so
	// disjoint concurrent edits (re-evacuate loop adding EVICTED, an
	// operator stamping a different flag, etc.) all converge instead
	// of being silently lost by the wholesale `Update`'s un-retried
	// wire-snapshot replace (Bug 205).
	//
	// Distinct from `PatchNetInterfaces` / `PatchProps` (which take
	// narrowly-typed mutators on a single sub-slice / sub-map); this
	// hands the closure the whole wire Node so multi-field
	// mutations stay atomic.
	PatchNodeSpec(ctx context.Context, name string, mutate func(*apiv1.Node) error) error
}

// StoragePoolStore persists StoragePool objects. The composite key is
// (node name, pool name); the same pool name can co-exist on different nodes.
type StoragePoolStore interface {
	List(ctx context.Context) ([]apiv1.StoragePool, error)
	ListByNode(ctx context.Context, node string) ([]apiv1.StoragePool, error)
	Get(ctx context.Context, node, pool string) (apiv1.StoragePool, error)
	Create(ctx context.Context, sp *apiv1.StoragePool) error
	Update(ctx context.Context, sp *apiv1.StoragePool) error
	Delete(ctx context.Context, node, pool string) error

	// SetCapacity writes free/total via the Status subresource without
	// touching Spec — keeps periodic capacity pushes from racing with
	// ProviderKind / Props edits. linstor-csi GetCapacity reads the
	// FreeCapacity field; the autoplacer's free-space ranking too.
	SetCapacity(ctx context.Context, node, pool string, freeKib, totalKib int64, supportsSnap bool) error

	// PatchStoragePoolSpec runs `mutate` against the freshly-fetched
	// StoragePool wire value and persists the result. On 409 the
	// fetch+mutate+patch cycle re-runs against fresh state — so
	// disjoint concurrent edits (sp set-property / racing Hello /
	// satellite Status pushes) all converge instead of being lost by
	// the wholesale `Update`'s un-retried wire-snapshot replace
	// (Bug 204b).
	PatchStoragePoolSpec(ctx context.Context, node, pool string, mutate func(*apiv1.StoragePool) error) error
}

// ResourceGroupStore persists ResourceGroup objects. Keyed by name.
//
// Annotation contract (Bug-021; shared by every annotation-carrying
// store — RG, RD, Resource, Snapshot): on Update/Patch a NIL wire
// `Annotations` map means "leave the stored user annotations
// untouched", while a NON-NIL map — including an empty one — means
// "replace the user-annotation set with exactly this set". Callers
// that delete the last remaining annotation must therefore keep the
// emptied map non-nil, or the deletion is silently dropped. Pinned
// by storetest's UpdateAnnotationContract suites against both the
// in-memory and the CRD-backed implementations.
type ResourceGroupStore interface {
	List(ctx context.Context) ([]apiv1.ResourceGroup, error)
	Get(ctx context.Context, name string) (apiv1.ResourceGroup, error)
	Create(ctx context.Context, rg *apiv1.ResourceGroup) error
	Update(ctx context.Context, rg *apiv1.ResourceGroup) error
	Delete(ctx context.Context, name string) error

	// PatchResourceGroup runs `mutate` against the freshly-fetched
	// current ResourceGroup wire value and persists the result. The
	// store handles Get + retry-on-conflict; `mutate` is re-invoked
	// against a re-fetched current state on every conflict retry, so
	// disjoint concurrent edits (RG modify + per-key prop delete)
	// all converge (Bug 201).
	//
	// Distinct from `Update(...)` which applies the wire snapshot
	// wholesale and silently drops concurrent peer mutations.
	PatchResourceGroup(ctx context.Context, name string, mutate func(*apiv1.ResourceGroup) error) error
}

// ResourceDefinitionStore persists ResourceDefinition objects. Keyed by name.
// Update/Patch follow the annotation contract documented on
// ResourceGroupStore (nil = untouched, empty = clear).
type ResourceDefinitionStore interface {
	List(ctx context.Context) ([]apiv1.ResourceDefinition, error)
	Get(ctx context.Context, name string) (apiv1.ResourceDefinition, error)
	Create(ctx context.Context, rd *apiv1.ResourceDefinition) error
	Update(ctx context.Context, rd *apiv1.ResourceDefinition) error
	Delete(ctx context.Context, name string) error

	// PatchResourceDefinitionSpec runs `mutate` against the freshly-fetched
	// ResourceDefinition wire value and persists the result. On 409 the
	// fetch+mutate+patch cycle re-runs against fresh state — so disjoint
	// concurrent edits (RD modify, drbd-passphrase, autoplace LayerStack,
	// r-conn) all converge instead of clobbering one another via stale
	// wire snapshots that the wholesale `Update`'s retry loop replays
	// verbatim (Bug 204b).
	PatchResourceDefinitionSpec(ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error) error
}

// ResourceStore persists Resource (replica placement) objects. The
// composite key is (resource_definition_name, node_name).
// Update/Patch follow the annotation contract documented on
// ResourceGroupStore (nil = untouched, empty = clear).
type ResourceStore interface {
	List(ctx context.Context) ([]apiv1.Resource, error)
	ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Resource, error)
	Get(ctx context.Context, rdName, node string) (apiv1.Resource, error)
	Create(ctx context.Context, r *apiv1.Resource) error
	Update(ctx context.Context, r *apiv1.Resource) error
	Delete(ctx context.Context, rdName, node string) error

	// DeleteIfTieBreaker atomically deletes the named replica ONLY if,
	// at the observed object version, it still carries the TIE_BREAKER
	// flag (i.e. it is still an auto-tiebreaker witness). The delete is
	// guarded by an optimistic-concurrency precondition on the backing
	// object's identity: a concurrent promotion of that same (rd, node)
	// row from witness/diskless to a diskful backfill replica (the
	// placer's redundancy backfill — pkg/placer.promoteWitness flips the
	// flags via PatchResourceSpec, bumping the object version) makes the
	// delete a no-op instead of clobbering the freshly-promoted replica.
	//
	// This closes the Bug 393 / inactive-return-backfills-redundancy
	// race: the witness reaper's plain Get-then-Delete was non-atomic —
	// it re-read the flags but the Delete had no version precondition, so
	// a promotion landing in the gap between the read and the Delete was
	// silently deleted by name and redundancy was never restored.
	//
	// Returns (true, nil) when the witness was deleted; (false, nil) when
	// it was skipped because it is no longer a witness (promoted, or the
	// flags otherwise changed under us) or already gone — both are benign
	// convergence outcomes the reaper swallows. A genuine error (lost
	// connectivity, etc.) is returned as (false, err).
	DeleteIfTieBreaker(ctx context.Context, rdName, node string) (bool, error)

	// SetState writes the runtime-observed state subresource (InUse,
	// DrbdState, per-volume observations) without touching Spec.
	// Required because the satellite's events2 observer can race a
	// Spec mutation (auto-diskful, resize) and naive whole-object
	// Updates would either lose State or clobber Spec. The k8s
	// store routes this through .Status().Update().
	//
	// state carries resource-level observed state (InUse, DrbdState).
	// volumes carries per-volume observed state (DiskState, CurrentGI)
	// the controller's seed-from-peer path reads to skip the full
	// initial-sync on replica-add (Phase 8.1). Empty slice is fine —
	// only resource-level state gets updated. Phase 10.2: DrbdState
	// moved from Spec.Props onto Status; the legacy drbdProps map
	// parameter is gone.
	SetState(ctx context.Context, rdName, node string, state apiv1.ResourceState, volumes []apiv1.VolumeObservation) error

	// ClearDRBDPort drops the persisted DRBD TCP port allocation on
	// the named replica. The controller's allocator (resource_controller.go
	// allocateDRBDFields) gates on `Status.DRBDPort == nil` and will
	// pick a fresh free port on its next reconcile. The activate REST
	// handler invokes this when the operator passes
	// `?reallocate-port=true` to recover from a port collision via
	// the deactivate + activate recipe — without it the original port
	// is preserved verbatim, making the recipe ineffective (Bug 46).
	//
	// Status.DRBDMinor and Status.DRBDNodeID are intentionally left
	// alone — neither participates in TCP-port collisions, and the
	// minor in particular is baked into device-mapper paths that
	// in-flight I/O is wired to. Returns ErrNotFound when the named
	// replica doesn't exist.
	ClearDRBDPort(ctx context.Context, rdName, node string) error

	// PatchResourceSpec runs `mutate` against the freshly-fetched
	// Resource wire value and persists the result. On 409 the
	// fetch+mutate+patch cycle re-runs against fresh state — so
	// disjoint concurrent edits (r modify, toggle-disk, r activate,
	// autoplace) all converge instead of being silently lost by the
	// wholesale `Update`'s un-retried wire-snapshot replace (Bug 204b).
	PatchResourceSpec(ctx context.Context, rdName, node string, mutate func(*apiv1.Resource) error) error
}

// VolumeDefinitionStore persists VolumeDefinition objects. The composite
// key is (resource_definition_name, volume_number); upstream LINSTOR keeps
// VolumeDefinitions inline on the ResourceDefinition, and so do we (the CRD
// has spec.volumeDefinitions). The interface gives REST handlers a clean
// surface; the implementation stitches it onto the RD CRD.
type VolumeDefinitionStore interface {
	List(ctx context.Context, rdName string) ([]apiv1.VolumeDefinition, error)
	Get(ctx context.Context, rdName string, volumeNumber int32) (apiv1.VolumeDefinition, error)
	Create(ctx context.Context, rdName string, vd *apiv1.VolumeDefinition) error
	Update(ctx context.Context, rdName string, vd *apiv1.VolumeDefinition) error
	Delete(ctx context.Context, rdName string, volumeNumber int32) error

	// PatchVolumeDefinitionSpec runs `mutate` against the freshly-
	// fetched VolumeDefinition wire value (resolved out of the parent
	// RD's spec.volumeDefinitions[] by volumeNumber) and persists the
	// result via a JSON-merge-patch on the parent RD under
	// `RetryOnConflict`. On 409 the entire fetch+mutate+patch cycle
	// re-runs against fresh RD state — so concurrent VD modifies on
	// disjoint props converge instead of clobbering each other via
	// the stale-wire-snapshot the wholesale `Update` retry loop
	// replays verbatim (Bug 204b).
	PatchVolumeDefinitionSpec(ctx context.Context, rdName string, volumeNumber int32, mutate func(*apiv1.VolumeDefinition) error) error
}

// SnapshotStore persists Snapshot objects. The composite key is
// (resource definition, snapshot name).
// Update follows the annotation contract documented on
// ResourceGroupStore (nil = untouched, empty = clear).
type SnapshotStore interface {
	List(ctx context.Context) ([]apiv1.Snapshot, error)
	ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Snapshot, error)
	Get(ctx context.Context, rdName, snapName string) (apiv1.Snapshot, error)
	Create(ctx context.Context, snap *apiv1.Snapshot) error
	Update(ctx context.Context, snap *apiv1.Snapshot) error
	Delete(ctx context.Context, rdName, snapName string) error
}

// PhysicalDeviceStore persists PhysicalDevice objects (Phase 10.7).
// The discovery loop on each satellite Creates / Updates / Deletes
// rows here; the REST shim Reads them to surface upstream
// `linstor physical-storage list`; the controller-side reconciler
// (or operator via `kubectl edit`) sets Spec.AttachTo by Updating.
//
// The composite key is just `name` — the CRD already encodes
// (node, stable-id) via its metadata.name, and the store passes
// that through unchanged.
type PhysicalDeviceStore interface {
	List(ctx context.Context) ([]apiv1.PhysicalDevice, error)
	ListForNode(ctx context.Context, nodeName string) ([]apiv1.PhysicalDevice, error)
	Get(ctx context.Context, name string) (apiv1.PhysicalDevice, error)
	Create(ctx context.Context, dev *apiv1.PhysicalDevice) error
	Update(ctx context.Context, dev *apiv1.PhysicalDevice) error
	Delete(ctx context.Context, name string) error
}

// ControllerPropsStore persists the singleton controller-scope props
// bag (keyed by apiv1.ControllerPropsName, "default"). Upstream LINSTOR
// stores `Autoplacer/Weights/*` and other cluster-wide tunables on the
// Controller pseudo-object; we mirror that with a single-row store so
// the autoplacer (and any future cluster-tunable consumer) can read /
// write the knobs through one well-typed surface. Missing keys are
// returned as empty strings — the placer treats that as "use default".
type ControllerPropsStore interface {
	// Get returns the current props map. An empty map (not nil) is
	// returned when no value has been written yet, so callers can do
	// `props[key]` lookups without nil-checks.
	Get(ctx context.Context) (map[string]string, error)
	// Set replaces the entire props map atomically. Callers that want
	// merge semantics must Get → mutate → Set; the store does no
	// per-key merging on the assumption that the operator-visible
	// patch surface (REST) is the right place for partial updates.
	Set(ctx context.Context, props map[string]string) error
}

// StoragePoolDefinition is the controller-scope row that registers a
// StoragePool *name* (e.g. "zfs-thin") independent of which nodes
// host it. Upstream LINSTOR keeps a dedicated `StorPoolDfn` table
// next to per-node StoragePools; the `MaxOversubscriptionRatio`
// and similar cluster-wide knobs hang off this row. Blockstor folds
// the per-node and the definition surface into one CRD (StoragePool),
// so a definition without any per-node pool is purely a name+props
// registration, captured here in a small process-local table.
//
// The row survives only for the lifetime of the controller process —
// no CRD or Secret backs it yet. This is the same compromise
// ControllerPropsStore makes; operators can re-create definitions
// after a controller restart, and the four upstream-defined props
// (`MaxOversubscriptionRatio` etc.) are pure scoring multipliers
// with no persisted state depending on them.
type StoragePoolDefinition struct {
	Name  string
	Props map[string]string
}

// StoragePoolDefinitionStore persists the controller-scope storage
// pool definition registry. The REST handlers at
// `/v1/storage-pool-definitions[/{name}]` are the operator surface;
// the store is intentionally minimal (no patch helpers, no status
// subresource) because the definition row carries no state beyond
// its name + props.
type StoragePoolDefinitionStore interface {
	List(ctx context.Context) ([]StoragePoolDefinition, error)
	Get(ctx context.Context, name string) (StoragePoolDefinition, error)
	Create(ctx context.Context, def *StoragePoolDefinition) error
	Update(ctx context.Context, def *StoragePoolDefinition) error
	Delete(ctx context.Context, name string) error
}

// Store aggregates per-resource stores.
type Store interface {
	Nodes() NodeStore
	StoragePools() StoragePoolStore
	ResourceGroups() ResourceGroupStore
	ResourceDefinitions() ResourceDefinitionStore
	Resources() ResourceStore
	VolumeDefinitions() VolumeDefinitionStore
	Snapshots() SnapshotStore
	PhysicalDevices() PhysicalDeviceStore
	ControllerProps() ControllerPropsStore
	StoragePoolDefinitions() StoragePoolDefinitionStore
}
