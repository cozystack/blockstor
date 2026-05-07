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

type snapKey struct {
	rd   string
	snap string
}

type inMemorySnapshots struct {
	mu sync.RWMutex
	m  map[snapKey]apiv1.Snapshot
}

func (s *inMemorySnapshots) List(_ context.Context) ([]apiv1.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]apiv1.Snapshot, 0, len(s.m))
	for k := range s.m {
		out = append(out, s.m[k])
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].ResourceName != out[j].ResourceName {
			return out[i].ResourceName < out[j].ResourceName
		}

		return out[i].Name < out[j].Name
	})

	return out, nil
}

func (s *inMemorySnapshots) ListByDefinition(_ context.Context, rdName string) ([]apiv1.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]apiv1.Snapshot, 0)

	for k := range s.m {
		if s.m[k].ResourceName == rdName {
			out = append(out, s.m[k])
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

func (s *inMemorySnapshots) Get(_ context.Context, rdName, snapName string) (apiv1.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap, ok := s.m[snapKey{rdName, snapName}]
	if !ok {
		return apiv1.Snapshot{}, errors.Wrapf(ErrNotFound, "snapshot %q on resource definition %q", snapName, rdName)
	}

	return snap, nil
}

func (s *inMemorySnapshots) Create(_ context.Context, snap *apiv1.Snapshot) error {
	if snap == nil {
		return errors.New("nil Snapshot")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	k := snapKey{snap.ResourceName, snap.Name}
	if _, exists := s.m[k]; exists {
		return errors.Wrapf(ErrAlreadyExists, "snapshot %q on resource definition %q", snap.Name, snap.ResourceName)
	}

	s.m[k] = *snap

	return nil
}

func (s *inMemorySnapshots) Update(_ context.Context, snap *apiv1.Snapshot) error {
	if snap == nil {
		return errors.New("nil Snapshot")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	k := snapKey{snap.ResourceName, snap.Name}
	if _, exists := s.m[k]; !exists {
		return errors.Wrapf(ErrNotFound, "snapshot %q on resource definition %q", snap.Name, snap.ResourceName)
	}

	s.m[k] = *snap

	return nil
}

func (s *inMemorySnapshots) Delete(_ context.Context, rdName, snapName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := snapKey{rdName, snapName}
	if _, exists := s.m[k]; !exists {
		return errors.Wrapf(ErrNotFound, "snapshot %q on resource definition %q", snapName, rdName)
	}

	delete(s.m, k)

	return nil
}
