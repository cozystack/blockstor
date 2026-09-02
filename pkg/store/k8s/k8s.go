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

// Package k8s provides a CRD-backed implementation of pkg/store.Store.
//
// Phase 2 swaps the InMemory store for this one in cmd/controller/main.go (default).
// Both implementations satisfy the same interface and are exercised by the
// same test suite, so behavioural drift is caught immediately.
package k8s

import (
	"context"
	"maps"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
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

	nodes                  *nodes
	storagePools           *storagePools
	resourceGroups         *resourceGroups
	resourceDefinitions    *resourceDefinitions
	resources              *resources
	volumeDefinitions      *volumeDefinitions
	snapshots              *snapshots
	physicalDevices        *physicalDevices
	controllerProps        *controllerProps
	storagePoolDefinitions *storagePoolDefinitions
}

// New wraps a controller-runtime client and returns a store.Store.
func New(c ctrlclient.Client) *Store {
	return NewWithAPIReader(c, nil)
}

// NewWithAPIReader is New plus a direct (uncached) API reader. The
// reader is used ONLY where a cache-lag read would be incorrect — the
// BUG-048 atomic VolumeNumber allocation, where retrying an optimistic-
// lock conflict against a stale informer cache re-derives the SAME
// number and the create-loop never converges (the second of two
// concurrent `vd c` is dropped), and the RD/VD GET cache-miss fallback
// (a multi-replica apiserver GET can land on a replica whose cache has
// not observed a just-committed RD yet; VDs are embedded in the RD, so
// both paths must answer consistently). Pass mgr.GetAPIReader() in
// production; nil is accepted and every path falls back to the cached
// client (in-memory / unit harnesses that have no informer).
//
// Do NOT extend the uncached fallback to the other stores' Gets: raw
// store Gets are cache-only by design. REST existence probes rely on a
// fast cached NotFound, and cross-replica lag on read endpoints is
// absorbed at the REST layer by get*WithCacheRetry, which POLLS the
// cached read so subsequent cached Lists stay read-your-writes
// coherent. A store-level fallback short-circuits that convergence
// wait (a CI snapshot-pagination regression demonstrated this when it
// was tried: the create flow's read-back resolved uncached while the
// cached List still under-reported).
func NewWithAPIReader(c ctrlclient.Client, apiReader ctrlclient.Reader) *Store {
	s := &Store{c: c}
	s.nodes = &nodes{c: c}
	s.storagePools = &storagePools{c: c}
	s.resourceGroups = &resourceGroups{c: c}
	s.resourceDefinitions = &resourceDefinitions{c: c, apiReader: apiReader}
	s.resources = &resources{c: c}
	s.volumeDefinitions = &volumeDefinitions{c: c, apiReader: apiReader}
	s.snapshots = &snapshots{c: c}
	s.physicalDevices = &physicalDevices{c: c}
	s.controllerProps = &controllerProps{c: c}
	s.storagePoolDefinitions = &storagePoolDefinitions{m: map[string]store.StoragePoolDefinition{}}

	return s
}

// controllerProps is the CRD-backed view of the singleton
// controller-scope props bag: `ControllerConfig.Spec.ExtraProps` on
// the cluster-wide `default` instance — the same record the REST
// shim mutates on `linstor controller set-property`.
//
// BUG-022: this used to be a process-local map populated only at
// construction, so every controller-scope knob written via the CLI
// (BalanceResources*, Autoplacer/Weights/*, ...) was invisible to
// running reconcilers and the placer until a process restart. Reading
// through the client keeps the bag fresh: in production the client is
// the manager's informer-cached client, so Get is a cache hit kept in
// sync by the ControllerConfig watch — the same mechanism every other
// kind in this store relies on. Thread-safety comes from the client
// (concurrent reconcilers call Get; the cache reader is safe for
// concurrent use).
type controllerProps struct {
	c ctrlclient.Client
}

// Get returns a defensive copy of `Spec.ExtraProps` (never nil). A
// missing ControllerConfig CRD — fresh cluster, nothing written yet —
// folds into an empty map, mirroring the InMemory store's "no value
// written yet" contract.
func (s *controllerProps) Get(ctx context.Context) (map[string]string, error) {
	var cfg crdv1alpha1.ControllerConfig

	err := s.c.Get(ctx, ctrlclient.ObjectKey{Name: crdv1alpha1.ControllerConfigName}, &cfg)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return map[string]string{}, nil
		}

		return nil, errors.Wrap(err, "get ControllerConfig")
	}

	out := make(map[string]string, len(cfg.Spec.ExtraProps))
	maps.Copy(out, cfg.Spec.ExtraProps)

	return out, nil
}

// Set replaces `Spec.ExtraProps` atomically, routed through
// PatchControllerExtraProps so it inherits the auto-create-on-NotFound
// and retry-on-conflict semantics the REST write path already uses —
// a Set racing a `linstor controller set-property` converges instead
// of clobbering.
func (s *controllerProps) Set(ctx context.Context, props map[string]string) error {
	return PatchControllerExtraProps(ctx, s.c, func(m map[string]string) error {
		clear(m)
		maps.Copy(m, props)

		return nil
	})
}

// PatchProps merges one caller's edit into Spec.ExtraProps without
// disturbing keys it did not name, inheriting the auto-create and
// retry-on-conflict semantics of the REST write path.
func (s *controllerProps) PatchProps(ctx context.Context, mutate func(map[string]string) error) error {
	if mutate == nil {
		return errors.New("nil mutate")
	}

	return PatchControllerExtraProps(ctx, s.c, mutate)
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

// StoragePoolDefinitions returns the controller-scope storage pool
// definition registry. Process-local for now (same compromise as
// controllerProps above) — a future iteration backs it with a
// dedicated CRD so definitions survive controller restarts.
func (s *Store) StoragePoolDefinitions() store.StoragePoolDefinitionStore {
	return s.storagePoolDefinitions
}

// storagePoolDefinitions is the process-local stand-in for the
// controller-scope storage pool definition registry. Same compromise
// as `controllerProps` above: process-local map protected by an
// RWMutex; a future iteration swaps this for a dedicated CRD.
type storagePoolDefinitions struct {
	mu sync.RWMutex
	m  map[string]store.StoragePoolDefinition
}

// List returns every definition sorted by name. Deterministic order
// matches the InMemory store so test expectations stay aligned.
func (s *storagePoolDefinitions) List(_ context.Context) ([]store.StoragePoolDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]store.StoragePoolDefinition, 0, len(s.m))
	for k := range s.m {
		out = append(out, copyStoragePoolDefinition(s.m[k]))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

// Get returns the definition or ErrNotFound.
func (s *storagePoolDefinitions) Get(_ context.Context, name string) (store.StoragePoolDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	def, ok := s.m[name]
	if !ok {
		return store.StoragePoolDefinition{}, errors.Wrapf(store.ErrNotFound, "storage pool definition %q", name)
	}

	return copyStoragePoolDefinition(def), nil
}

// Create inserts a new definition. Returns ErrAlreadyExists when the
// name is already taken.
func (s *storagePoolDefinitions) Create(_ context.Context, def *store.StoragePoolDefinition) error {
	if def == nil {
		return errors.New("nil StoragePoolDefinition")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.m[def.Name]; exists {
		return errors.Wrapf(store.ErrAlreadyExists, "storage pool definition %q", def.Name)
	}

	s.m[def.Name] = copyStoragePoolDefinition(*def)

	return nil
}

// Update overwrites an existing definition. Returns ErrNotFound when
// the row is absent.
func (s *storagePoolDefinitions) Update(_ context.Context, def *store.StoragePoolDefinition) error {
	if def == nil {
		return errors.New("nil StoragePoolDefinition")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.m[def.Name]; !exists {
		return errors.Wrapf(store.ErrNotFound, "storage pool definition %q", def.Name)
	}

	s.m[def.Name] = copyStoragePoolDefinition(*def)

	return nil
}

// Delete removes the named definition. Returns ErrNotFound when
// absent.
func (s *storagePoolDefinitions) Delete(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.m[name]; !exists {
		return errors.Wrapf(store.ErrNotFound, "storage pool definition %q", name)
	}

	delete(s.m, name)

	return nil
}

// copyStoragePoolDefinition deep-copies the props map so callers can
// mutate the returned value without racing the store.
func copyStoragePoolDefinition(def store.StoragePoolDefinition) store.StoragePoolDefinition {
	out := store.StoragePoolDefinition{Name: def.Name}
	if def.Props != nil {
		out.Props = make(map[string]string, len(def.Props))
		maps.Copy(out.Props, def.Props)
	}

	return out
}
