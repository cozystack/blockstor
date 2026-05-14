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
		sp := s.m[k]
		withDerivedState(&sp)

		out = append(out, sp)
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
			sp := s.m[k]
			withDerivedState(&sp)

			out = append(out, sp)
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

	withDerivedState(&sp)

	return sp, nil
}

// withDerivedState fills the wire `state` field from the in-memory
// PoolMissing carrier and synthesises `free_space_mgr_name` for
// non-shared pools when missing. Mirrors the k8s store's
// crdToWireStoragePool — keeping the two backends in lockstep so the
// rest tests that use the in-memory store still see "Ok" / "Faulty"
// AND the per-pool free-space-manager name (`<node>:<pool>` for
// non-shared pools, the shared-space identifier for shared ones) the
// same way real clusters do.
//
// The Python CLI's `storpool_cmds.py` does `':' not in
// free_space_mgr_name` and crashes with TypeError when the field is
// null — so filling this defensively is also a CLI-stability fix.
//
// Bug 83: when PoolMissing=true we ALSO synthesise an ERROR-severity
// reports[] entry, because python-linstor renders the State column
// from `get_replies_state(storpool.reports)` rather than the bare
// `state` string. Without the reports entry, `linstor sp l` shows Ok
// against a Faulty pool. Mirrors the k8s store's poolMissingReport.
func withDerivedState(sp *apiv1.StoragePool) {
	if sp.State == "" {
		if sp.PoolMissing {
			sp.State = "Faulty"
		} else {
			sp.State = "Ok"
		}
	}

	if sp.PoolMissing && len(sp.Reports) == 0 {
		sp.Reports = []apiv1.APICallRc{poolMissingReport(sp.NodeName, sp.StoragePoolName)}
	}

	if sp.FreeSpaceMgrName == "" {
		if sp.SharedSpaceID != "" {
			sp.FreeSpaceMgrName = sp.SharedSpaceID
		} else if sp.NodeName != "" && sp.StoragePoolName != "" {
			sp.FreeSpaceMgrName = sp.NodeName + ":" + sp.StoragePoolName
		}
	}
}

// poolMissingReport builds the same ERROR-severity ApiCallRc entry the k8s
// store stamps onto a Faulty pool. Duplicated here (rather than importing
// from pkg/store/k8s) to avoid a cyclic import — k8s already imports
// pkg/store. Bug 83.
func poolMissingReport(node, pool string) apiv1.APICallRc {
	return apiv1.APICallRc{
		RetCode: apiv1.APICallRcMaskError |
			apiv1.APICallRcMaskStorPool |
			apiv1.APICallRcFailStorPoolConfigurationError,
		Message: "pool backing storage missing on node " + node +
			": storage pool " + pool + " is not present",
		Cause: "The satellite probed the backing volume group / zpool / " +
			"directory for this storage pool and found it absent — " +
			"typically the result of an operator `zpool destroy`, " +
			"`vgremove`, or an unmounted filesystem.",
		Correc: "Re-discover the pool via `linstor ps cdp <provider> " +
			"<node> <pool>` once the backing storage is restored, or " +
			"recreate the pool with `linstor sp c`.",
		ObjRefs: map[string]string{
			"Node":     node,
			"StorPool": pool,
		},
	}
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

// SetCapacity mutates only the capacity fields on the in-memory copy.
// Returns ErrNotFound if the pool isn't present.
func (s *inMemoryStoragePools) SetCapacity(_ context.Context, node, pool string, freeKib, totalKib int64, supportsSnap bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := spKey{node, pool}

	existing, ok := s.m[key]
	if !ok {
		return errors.Wrapf(ErrNotFound, "storage pool %q on node %q", pool, node)
	}

	existing.FreeCapacity = freeKib
	existing.TotalCapacity = totalKib
	existing.SupportsSnapshot = supportsSnap
	s.m[key] = existing

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
