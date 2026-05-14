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

package store

import (
	"context"
	"maps"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// InMemory is the Phase 1 store: thread-safe, RAM-backed, lost on restart.
// Phase 2 introduces a CRD-backed store that satisfies the same interface.
type InMemory struct {
	nodes               *inMemoryNodes
	storagePools        *inMemoryStoragePools
	resourceGroups      *inMemoryResourceGroups
	resourceDefinitions *inMemoryResourceDefinitions
	resources           *inMemoryResources
	volumeDefinitions   *inMemoryVolumeDefinitions
	snapshots           *inMemorySnapshots
	physicalDevices     *inMemoryPhysicalDevices
	controllerProps     *inMemoryControllerProps
}

// NewInMemory constructs an InMemory store with empty per-resource maps.
func NewInMemory() *InMemory {
	return &InMemory{
		nodes:               &inMemoryNodes{m: map[string]apiv1.Node{}},
		storagePools:        &inMemoryStoragePools{m: map[spKey]apiv1.StoragePool{}},
		resourceGroups:      &inMemoryResourceGroups{m: map[string]apiv1.ResourceGroup{}},
		resourceDefinitions: &inMemoryResourceDefinitions{m: map[string]apiv1.ResourceDefinition{}},
		resources:           &inMemoryResources{m: map[rKey]apiv1.Resource{}},
		volumeDefinitions:   &inMemoryVolumeDefinitions{m: map[vdKey]apiv1.VolumeDefinition{}},
		snapshots:           &inMemorySnapshots{m: map[snapKey]apiv1.Snapshot{}},
		physicalDevices:     &inMemoryPhysicalDevices{m: map[string]apiv1.PhysicalDevice{}},
		controllerProps:     &inMemoryControllerProps{props: map[string]string{}},
	}
}

// Nodes returns the NodeStore view of this store.
func (s *InMemory) Nodes() NodeStore { return s.nodes }

// StoragePools returns the StoragePoolStore view of this store.
func (s *InMemory) StoragePools() StoragePoolStore { return s.storagePools }

// ResourceGroups returns the ResourceGroupStore view of this store.
func (s *InMemory) ResourceGroups() ResourceGroupStore { return s.resourceGroups }

// ResourceDefinitions returns the ResourceDefinitionStore view.
func (s *InMemory) ResourceDefinitions() ResourceDefinitionStore { return s.resourceDefinitions }

// Resources returns the ResourceStore view.
func (s *InMemory) Resources() ResourceStore { return s.resources }

// VolumeDefinitions returns the VolumeDefinitionStore view.
func (s *InMemory) VolumeDefinitions() VolumeDefinitionStore { return s.volumeDefinitions }

// Snapshots returns the SnapshotStore view.
func (s *InMemory) Snapshots() SnapshotStore { return s.snapshots }

// PhysicalDevices returns the PhysicalDeviceStore view.
func (s *InMemory) PhysicalDevices() PhysicalDeviceStore { return s.physicalDevices }

// ControllerProps returns the singleton controller-scope props bag.
func (s *InMemory) ControllerProps() ControllerPropsStore { return s.controllerProps }

// inMemoryControllerProps is a single-row bag protected by an RWMutex.
// The k8s store will back the same shape with a singleton CRD; here we
// hold the map straight so unit tests can prime weights without touching
// any k8s plumbing.
type inMemoryControllerProps struct {
	mu    sync.RWMutex
	props map[string]string
}

// Get returns a defensive copy of the props map (never nil — callers
// can do `m[key]` directly).
func (s *inMemoryControllerProps) Get(_ context.Context) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]string, len(s.props))
	maps.Copy(out, s.props)

	return out, nil
}

// Set replaces the props map atomically. A nil input clears the bag.
func (s *inMemoryControllerProps) Set(_ context.Context, props map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	next := make(map[string]string, len(props))
	maps.Copy(next, props)

	s.props = next

	return nil
}

type inMemoryNodes struct {
	mu sync.RWMutex
	m  map[string]apiv1.Node
}

// List returns all nodes sorted by name (deterministic order is part of the
// contract so callers and tests don't have to sort).
func (s *inMemoryNodes) List(_ context.Context) ([]apiv1.Node, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]apiv1.Node, 0, len(s.m))
	for _, n := range s.m {
		if len(n.NetInterfaces) > 0 {
			n.NetInterfaces = apiv1.DefaultNetInterfaceFields(append([]apiv1.NetInterface(nil), n.NetInterfaces...))
		}

		out = append(out, n)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

// Get returns the named node or ErrNotFound. NetInterface defaults
// are applied on read so the in-memory store mirrors the K8s
// backend's wire shape — both surfaces emit `satellite_port=3366` /
// `satellite_encryption_type=PLAIN` when the caller omitted them at
// Create time.
func (s *inMemoryNodes) Get(_ context.Context, name string) (apiv1.Node, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	n, ok := s.m[name]
	if !ok {
		return apiv1.Node{}, errors.Wrapf(ErrNotFound, "node %q", name)
	}

	if len(n.NetInterfaces) > 0 {
		n.NetInterfaces = apiv1.DefaultNetInterfaceFields(append([]apiv1.NetInterface(nil), n.NetInterfaces...))
	}

	return n, nil
}

// Create inserts a new node. Returns ErrAlreadyExists if the name is taken.
func (s *inMemoryNodes) Create(_ context.Context, n *apiv1.Node) error {
	if n == nil {
		return errors.New("nil Node")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.m[n.Name]; exists {
		return errors.Wrapf(ErrAlreadyExists, "node %q", n.Name)
	}

	s.m[n.Name] = *n

	return nil
}

// Update overwrites an existing node. Returns ErrNotFound if the node does
// not yet exist (this is not an upsert — callers must Create first).
func (s *inMemoryNodes) Update(_ context.Context, n *apiv1.Node) error {
	if n == nil {
		return errors.New("nil Node")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.m[n.Name]; !exists {
		return errors.Wrapf(ErrNotFound, "node %q", n.Name)
	}

	s.m[n.Name] = *n

	return nil
}

// Delete removes a node by name. Returns ErrNotFound if absent.
func (s *inMemoryNodes) Delete(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.m[name]; !exists {
		return errors.Wrapf(ErrNotFound, "node %q", name)
	}

	delete(s.m, name)

	return nil
}

// SetConnectionStatus mutates only the ConnectionStatus field on the
// in-memory copy. Returns ErrNotFound if the node hasn't been
// Create'd yet.
func (s *inMemoryNodes) SetConnectionStatus(_ context.Context, name, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, ok := s.m[name]
	if !ok {
		return errors.Wrapf(ErrNotFound, "node %q", name)
	}

	node.ConnectionStatus = status
	s.m[name] = node

	return nil
}
