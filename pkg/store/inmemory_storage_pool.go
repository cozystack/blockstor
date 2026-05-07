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
	"sort"
	"sync"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// spKey is the composite map key for the in-memory storage-pool index.
type spKey struct {
	node string
	pool string
}

type inMemoryStoragePools struct {
	mu sync.RWMutex
	m  map[spKey]apiv1.StoragePool
}

// List returns all pools sorted (node, pool).
func (s *inMemoryStoragePools) List(_ context.Context) ([]apiv1.StoragePool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]apiv1.StoragePool, 0, len(s.m))
	for k := range s.m {
		out = append(out, s.m[k])
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].NodeName != out[j].NodeName {
			return out[i].NodeName < out[j].NodeName
		}

		return out[i].StoragePoolName < out[j].StoragePoolName
	})

	return out, nil
}

// ListByNode returns all pools on the named node, sorted by pool name.
func (s *inMemoryStoragePools) ListByNode(_ context.Context, node string) ([]apiv1.StoragePool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]apiv1.StoragePool, 0)

	for k := range s.m {
		if s.m[k].NodeName == node {
			out = append(out, s.m[k])
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].StoragePoolName < out[j].StoragePoolName
	})

	return out, nil
}

// Get returns the named pool on the named node, or ErrNotFound.
func (s *inMemoryStoragePools) Get(_ context.Context, node, pool string) (apiv1.StoragePool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sp, ok := s.m[spKey{node, pool}]
	if !ok {
		return apiv1.StoragePool{}, errors.Wrapf(ErrNotFound, "storage pool %q on node %q", pool, node)
	}

	return sp, nil
}

// Create inserts a new pool. Conflict on (node, name).
func (s *inMemoryStoragePools) Create(_ context.Context, sp *apiv1.StoragePool) error {
	if sp == nil {
		return errors.New("nil StoragePool")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	k := spKey{sp.NodeName, sp.StoragePoolName}
	if _, exists := s.m[k]; exists {
		return errors.Wrapf(ErrAlreadyExists, "storage pool %q on node %q", sp.StoragePoolName, sp.NodeName)
	}

	s.m[k] = *sp

	return nil
}

// Update overwrites an existing pool. Returns ErrNotFound if absent.
func (s *inMemoryStoragePools) Update(_ context.Context, sp *apiv1.StoragePool) error {
	if sp == nil {
		return errors.New("nil StoragePool")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	k := spKey{sp.NodeName, sp.StoragePoolName}
	if _, exists := s.m[k]; !exists {
		return errors.Wrapf(ErrNotFound, "storage pool %q on node %q", sp.StoragePoolName, sp.NodeName)
	}

	s.m[k] = *sp

	return nil
}

// Delete removes a pool by (node, name). Returns ErrNotFound if absent.
func (s *inMemoryStoragePools) Delete(_ context.Context, node, pool string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := spKey{node, pool}
	if _, exists := s.m[k]; !exists {
		return errors.Wrapf(ErrNotFound, "storage pool %q on node %q", pool, node)
	}

	delete(s.m, k)

	return nil
}
