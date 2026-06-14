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

package store

import (
	"context"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// vdKey is the composite key (resource definition, volume number).
type vdKey struct {
	rd  string
	vol int32
}

type inMemoryVolumeDefinitions struct {
	mu sync.RWMutex
	m  map[vdKey]apiv1.VolumeDefinition
}

func (s *inMemoryVolumeDefinitions) List(_ context.Context, rdName string) ([]apiv1.VolumeDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]apiv1.VolumeDefinition, 0)

	for k := range s.m {
		if k.rd == rdName {
			out = append(out, s.m[k])
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].VolumeNumber < out[j].VolumeNumber })

	return out, nil
}

func (s *inMemoryVolumeDefinitions) Get(_ context.Context, rdName string, volumeNumber int32) (apiv1.VolumeDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	vd, ok := s.m[vdKey{rdName, volumeNumber}]
	if !ok {
		return apiv1.VolumeDefinition{}, errors.Wrapf(ErrNotFound, "volume %d on resource definition %q", volumeNumber, rdName)
	}

	return vd, nil
}

func (s *inMemoryVolumeDefinitions) Create(_ context.Context, rdName string, vd *apiv1.VolumeDefinition) error {
	if vd == nil {
		return errors.New("nil VolumeDefinition")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	k := vdKey{rdName, vd.VolumeNumber}
	if _, exists := s.m[k]; exists {
		return errors.Wrapf(ErrAlreadyExists, "volume %d on resource definition %q", vd.VolumeNumber, rdName)
	}

	s.m[k] = *vd

	return nil
}

// CreateAutoNumbered allocates the smallest free non-negative
// VolumeNumber under the write lock so the read of the existing set and
// the insert are a single atomic step — the InMemory equivalent of the
// k8s backend's allocate-inside-RetryOnConflict. BUG-048: a REST-side
// "List then Create" sequence has a TOCTOU window two concurrent
// `vd c` calls fall into (both pick the same hole, the loser is
// rejected and its volume silently lost); doing the allocation here
// closes it.
func (s *inMemoryVolumeDefinitions) CreateAutoNumbered(_ context.Context, rdName string, vd *apiv1.VolumeDefinition) (int32, error) {
	if vd == nil {
		return 0, errors.New("nil VolumeDefinition")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Smallest-hole walk under the lock so the read of the used set and
	// the insert are a single atomic step. Mirrors upstream LINSTOR's
	// rule (VDs 0 and 2 present → 1, not 3).
	used := make(map[int32]bool)

	for k := range s.m {
		if k.rd == rdName {
			used[k.vol] = true
		}
	}

	var assigned int32
	for assigned = 0; used[assigned]; assigned++ { //nolint:revive // empty body: the increment IS the body
	}

	vd.VolumeNumber = assigned
	s.m[vdKey{rdName, assigned}] = *vd

	return assigned, nil
}

func (s *inMemoryVolumeDefinitions) Update(_ context.Context, rdName string, vd *apiv1.VolumeDefinition) error {
	if vd == nil {
		return errors.New("nil VolumeDefinition")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	k := vdKey{rdName, vd.VolumeNumber}
	if _, exists := s.m[k]; !exists {
		return errors.Wrapf(ErrNotFound, "volume %d on resource definition %q", vd.VolumeNumber, rdName)
	}

	s.m[k] = *vd

	return nil
}

// PatchVolumeDefinitionSpec runs `mutate` atomically against the live
// entry under the write lock. The InMemory store has no
// resourceVersion surface, so a lock-held single-shot mutate covers
// what RetryOnConflict does on the k8s backend. Bug 204b shim.
func (s *inMemoryVolumeDefinitions) PatchVolumeDefinitionSpec(_ context.Context, rdName string, volumeNumber int32, mutate func(*apiv1.VolumeDefinition) error) error {
	if mutate == nil {
		return errors.New("nil mutate")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := vdKey{rdName, volumeNumber}

	vd, ok := s.m[key]
	if !ok {
		return errors.Wrapf(ErrNotFound, "volume %d on resource definition %q", volumeNumber, rdName)
	}

	err := mutate(&vd)
	if err != nil {
		return errors.Wrapf(err, "patch volume %d on resource definition %q", volumeNumber, rdName)
	}

	s.m[key] = vd

	return nil
}

func (s *inMemoryVolumeDefinitions) Delete(_ context.Context, rdName string, volumeNumber int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := vdKey{rdName, volumeNumber}
	if _, exists := s.m[k]; !exists {
		return errors.Wrapf(ErrNotFound, "volume %d on resource definition %q", volumeNumber, rdName)
	}

	delete(s.m, k)

	return nil
}
