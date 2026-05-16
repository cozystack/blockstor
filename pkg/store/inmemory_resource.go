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

// rKey is the composite key (resource definition, node) for the in-memory
// resource index.
type rKey struct {
	rd   string
	node string
}

type inMemoryResources struct {
	mu        sync.RWMutex
	m         map[rKey]apiv1.Resource
	volStates map[rKey]map[int32]apiv1.VolumeState
}

func (s *inMemoryResources) List(_ context.Context) ([]apiv1.Resource, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]apiv1.Resource, 0, len(s.m))
	for k := range s.m {
		out = append(out, s.m[k])
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}

		return out[i].NodeName < out[j].NodeName
	})

	return out, nil
}

func (s *inMemoryResources) ListByDefinition(_ context.Context, rdName string) ([]apiv1.Resource, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]apiv1.Resource, 0)

	for k := range s.m {
		if s.m[k].Name == rdName {
			out = append(out, s.m[k])
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].NodeName < out[j].NodeName })

	return out, nil
}

func (s *inMemoryResources) Get(_ context.Context, rdName, node string) (apiv1.Resource, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	r, ok := s.m[rKey{rdName, node}]
	if !ok {
		return apiv1.Resource{}, errors.Wrapf(ErrNotFound, "resource %q on node %q", rdName, node)
	}

	return r, nil
}

func (s *inMemoryResources) Create(_ context.Context, r *apiv1.Resource) error {
	if r == nil {
		return errors.New("nil Resource")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	k := rKey{r.Name, r.NodeName}
	if _, exists := s.m[k]; exists {
		return errors.Wrapf(ErrAlreadyExists, "resource %q on node %q", r.Name, r.NodeName)
	}

	s.m[k] = *r

	return nil
}

func (s *inMemoryResources) Update(_ context.Context, r *apiv1.Resource) error {
	if r == nil {
		return errors.New("nil Resource")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	k := rKey{r.Name, r.NodeName}
	if _, exists := s.m[k]; !exists {
		return errors.Wrapf(ErrNotFound, "resource %q on node %q", r.Name, r.NodeName)
	}

	s.m[k] = *r

	return nil
}

// PatchResourceSpec runs `mutate` atomically against the live entry
// under the write lock. The InMemory store has no resourceVersion
// surface, so a lock-held single-shot mutate covers what
// RetryOnConflict does on the k8s backend. Bug 204b shim.
func (s *inMemoryResources) PatchResourceSpec(_ context.Context, rdName, node string, mutate func(*apiv1.Resource) error) error {
	if mutate == nil {
		return errors.New("nil mutate")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	k := rKey{rdName, node}

	r, ok := s.m[k]
	if !ok {
		return errors.Wrapf(ErrNotFound, "resource %q on node %q", rdName, node)
	}

	if err := mutate(&r); err != nil {
		return errors.Wrapf(err, "patch resource %q on node %q", rdName, node)
	}

	s.m[k] = r

	return nil
}

func (s *inMemoryResources) Delete(_ context.Context, rdName, node string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := rKey{rdName, node}
	if _, exists := s.m[k]; !exists {
		return errors.Wrapf(ErrNotFound, "resource %q on node %q", rdName, node)
	}

	delete(s.m, k)

	return nil
}

// SetState mutates the runtime-observed state without touching Spec.
// In the in-memory store there's no Status subresource, so we just
// merge the runtime fields onto the stored value.
//
// volumes carries per-volume observed state (DiskState, CurrentGi);
// stashed in a side-map keyed by (rd, node) so consumers that need
// per-volume CurrentGi readback can ask. The wire-shape `Resource`
// type intentionally has no Volumes slice — `ResourceWithVolumes`
// at the REST boundary stitches per-volume state separately — so
// the side-map is the InMemory equivalent.
func (s *inMemoryResources) SetState(_ context.Context, rdName, node string, state apiv1.ResourceState, volumes []apiv1.VolumeObservation) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := rKey{rdName, node}

	existing, exists := s.m[key]
	if !exists {
		return errors.Wrapf(ErrNotFound, "resource %q on node %q", rdName, node)
	}

	existing.State = state

	s.m[key] = existing

	if len(volumes) > 0 {
		if s.volStates == nil {
			s.volStates = map[rKey]map[int32]apiv1.VolumeState{}
		}

		if s.volStates[key] == nil {
			s.volStates[key] = map[int32]apiv1.VolumeState{}
		}

		for _, vol := range volumes {
			s.volStates[key][vol.VolumeNumber] = vol.State
		}
	}

	return nil
}

// ClearDRBDPort drops the recorded DRBD TCP port allocation on the
// named replica. Wire-shape Resource has no Status.DRBDPort field —
// the port surfaces via LayerObject.Drbd.TCPPorts (the k8s store
// projects Status.DRBDPort onto that slice). Clearing TCPPorts keeps
// the in-memory store behaviourally equivalent to the k8s store for
// REST handlers and tests that round-trip through both backends.
func (s *inMemoryResources) ClearDRBDPort(_ context.Context, rdName, node string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := rKey{rdName, node}

	existing, ok := s.m[key]
	if !ok {
		return errors.Wrapf(ErrNotFound, "resource %q on node %q", rdName, node)
	}

	if existing.LayerObject != nil && existing.LayerObject.Drbd != nil {
		existing.LayerObject.Drbd.TCPPorts = nil
	}

	s.m[key] = existing

	return nil
}

// VolumeStateForTest exposes per-volume state for the in-memory store
// to the shared storetest suite. Production callers read per-volume
// state via the K8s store's `Resource.Status.Volumes[i]` (CRD Status
// subresource) — InMemory mirrors the same data via this helper so
// the lock-step test suite can assert both stores agree.
func (s *inMemoryResources) VolumeStateForTest(rdName, node string, volumeNumber int32) (apiv1.VolumeState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.volStates == nil {
		return apiv1.VolumeState{}, false
	}

	st, ok := s.volStates[rKey{rdName, node}][volumeNumber]

	return st, ok
}
