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

// Package k8s provides a CRD-backed implementation of pkg/store.Store.
//
// Phase 2 swaps the InMemory store for this one in cmd/controller/main.go (default).
// Both implementations satisfy the same interface and are exercised by the
// same test suite, so behavioural drift is caught immediately.
package k8s

import (
	"context"
	"maps"
	"sync"

	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/cozystack/blockstor/pkg/store"
)

// Labels used to index StoragePool CRDs by (node, pool). LINSTOR's
// (node_name, pool_name) composite key does not survive a single
// metadata.name, so we encode it via labels for fast list queries.
const (
	LabelNodeName = "blockstor.io/node-name"
	LabelPoolName = "blockstor.io/pool-name"
)

// Store is a controller-runtime-client-backed store.
type Store struct {
	c ctrlclient.Client

	nodes               *nodes
	storagePools        *storagePools
	resourceGroups      *resourceGroups
	resourceDefinitions *resourceDefinitions
	resources           *resources
	volumeDefinitions   *volumeDefinitions
	snapshots           *snapshots
	physicalDevices     *physicalDevices
	controllerProps     *controllerProps
}

// New wraps a controller-runtime client and returns a store.Store.
func New(c ctrlclient.Client) *Store {
	s := &Store{c: c}
	s.nodes = &nodes{c: c}
	s.storagePools = &storagePools{c: c}
	s.resourceGroups = &resourceGroups{c: c}
	s.resourceDefinitions = &resourceDefinitions{c: c}
	s.resources = &resources{c: c}
	s.volumeDefinitions = &volumeDefinitions{c: c}
	s.snapshots = &snapshots{c: c}
	s.physicalDevices = &physicalDevices{c: c}
	s.controllerProps = &controllerProps{props: map[string]string{}}

	return s
}

// controllerProps is a process-local stand-in for the singleton
// controller-scope props bag. A future iteration will swap this for a
// dedicated CRD (or a ConfigMap-backed shim) so the value survives
// controller restarts; until then the autoplacer's weights revert to
// defaults across restarts, which is acceptable because the four
// `Autoplacer/Weights/*` knobs are pure scoring multipliers — no
// persisted state depends on them and operators that set them today
// can re-set after a restart with no data risk.
type controllerProps struct {
	mu    sync.RWMutex
	props map[string]string
}

// Get returns a defensive copy (never nil).
func (s *controllerProps) Get(_ context.Context) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]string, len(s.props))
	maps.Copy(out, s.props)

	return out, nil
}

// Set replaces the props map atomically.
func (s *controllerProps) Set(_ context.Context, props map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	next := make(map[string]string, len(props))
	maps.Copy(next, props)

	s.props = next

	return nil
}

// Nodes returns the NodeStore view of this store.
func (s *Store) Nodes() store.NodeStore { return s.nodes }

// StoragePools returns the StoragePoolStore view of this store.
func (s *Store) StoragePools() store.StoragePoolStore { return s.storagePools }

// ResourceGroups returns the ResourceGroupStore view of this store.
func (s *Store) ResourceGroups() store.ResourceGroupStore { return s.resourceGroups }

// ResourceDefinitions returns the ResourceDefinitionStore view.
func (s *Store) ResourceDefinitions() store.ResourceDefinitionStore { return s.resourceDefinitions }

// Resources returns the ResourceStore view of this store.
func (s *Store) Resources() store.ResourceStore { return s.resources }

// VolumeDefinitions returns the VolumeDefinitionStore view.
func (s *Store) VolumeDefinitions() store.VolumeDefinitionStore { return s.volumeDefinitions }

// Snapshots returns the SnapshotStore view.
func (s *Store) Snapshots() store.SnapshotStore { return s.snapshots }

// PhysicalDevices returns the PhysicalDeviceStore view.
func (s *Store) PhysicalDevices() store.PhysicalDeviceStore { return s.physicalDevices }

// ControllerProps returns the singleton controller-scope props bag.
func (s *Store) ControllerProps() store.ControllerPropsStore { return s.controllerProps }
