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

	k := vdKey{rdName, volumeNumber}

	vd, ok := s.m[k]
	if !ok {
		return errors.Wrapf(ErrNotFound, "volume %d on resource definition %q", volumeNumber, rdName)
	}

	if err := mutate(&vd); err != nil {
		return errors.Wrapf(err, "patch volume %d on resource definition %q", volumeNumber, rdName)
	}

	s.m[k] = vd

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
