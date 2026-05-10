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
}

// ResourceGroupStore persists ResourceGroup objects. Keyed by name.
type ResourceGroupStore interface {
	List(ctx context.Context) ([]apiv1.ResourceGroup, error)
	Get(ctx context.Context, name string) (apiv1.ResourceGroup, error)
	Create(ctx context.Context, rg *apiv1.ResourceGroup) error
	Update(ctx context.Context, rg *apiv1.ResourceGroup) error
	Delete(ctx context.Context, name string) error
}

// ResourceDefinitionStore persists ResourceDefinition objects. Keyed by name.
type ResourceDefinitionStore interface {
	List(ctx context.Context) ([]apiv1.ResourceDefinition, error)
	Get(ctx context.Context, name string) (apiv1.ResourceDefinition, error)
	Create(ctx context.Context, rd *apiv1.ResourceDefinition) error
	Update(ctx context.Context, rd *apiv1.ResourceDefinition) error
	Delete(ctx context.Context, name string) error
}

// ResourceStore persists Resource (replica placement) objects. The
// composite key is (resource_definition_name, node_name).
type ResourceStore interface {
	List(ctx context.Context) ([]apiv1.Resource, error)
	ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Resource, error)
	Get(ctx context.Context, rdName, node string) (apiv1.Resource, error)
	Create(ctx context.Context, r *apiv1.Resource) error
	Update(ctx context.Context, r *apiv1.Resource) error
	Delete(ctx context.Context, rdName, node string) error

	// SetState writes the runtime-observed state subresource (InUse,
	// DrbdState, per-volume observations) without touching Spec.
	// Required because the satellite's events2 observer can race a
	// Spec mutation (auto-diskful, resize) and naive whole-object
	// Updates would either lose State or clobber Spec. The k8s
	// store routes this through .Status().Update().
	//
	// state carries resource-level observed state (InUse, DrbdState).
	// volumes carries per-volume observed state (DiskState, CurrentGi)
	// the controller's seed-from-peer path reads to skip the full
	// initial-sync on replica-add (Phase 8.1). Empty slice is fine —
	// only resource-level state gets updated. Phase 10.2: DrbdState
	// moved from Spec.Props onto Status; the legacy drbdProps map
	// parameter is gone.
	SetState(ctx context.Context, rdName, node string, state apiv1.ResourceState, volumes []apiv1.VolumeObservation) error
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
}

// KeyValueStore persists arbitrary instance/key/value triples. linstor-csi
// uses this for its own per-volume bookkeeping (CSI snapshots, parameters
// ...). The (instance, key) pair is the composite identity.
type KeyValueStore interface {
	ListInstances(ctx context.Context) ([]string, error)
	GetInstance(ctx context.Context, instance string) (map[string]string, error)
	SetKeys(ctx context.Context, instance string, modify apiv1.GenericPropsModify) error
	DeleteInstance(ctx context.Context, instance string) error
}

// SnapshotStore persists Snapshot objects. The composite key is
// (resource definition, snapshot name).
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

// Store aggregates per-resource stores.
type Store interface {
	Nodes() NodeStore
	StoragePools() StoragePoolStore
	ResourceGroups() ResourceGroupStore
	ResourceDefinitions() ResourceDefinitionStore
	Resources() ResourceStore
	VolumeDefinitions() VolumeDefinitionStore
	KeyValueStore() KeyValueStore
	Snapshots() SnapshotStore
	PhysicalDevices() PhysicalDeviceStore
}
