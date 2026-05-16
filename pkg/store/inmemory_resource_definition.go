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

type inMemoryResourceDefinitions struct {
	mu sync.RWMutex
	m  map[string]apiv1.ResourceDefinition
}

func (s *inMemoryResourceDefinitions) List(_ context.Context) ([]apiv1.ResourceDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]apiv1.ResourceDefinition, 0, len(s.m))
	for k := range s.m {
		out = append(out, s.m[k])
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

func (s *inMemoryResourceDefinitions) Get(_ context.Context, name string) (apiv1.ResourceDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rd, ok := s.m[name]
	if !ok {
		return apiv1.ResourceDefinition{}, errors.Wrapf(ErrNotFound, "resource definition %q", name)
	}

	return rd, nil
}

func (s *inMemoryResourceDefinitions) Create(_ context.Context, rd *apiv1.ResourceDefinition) error {
	if rd == nil {
		return errors.New("nil ResourceDefinition")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.m[rd.Name]; exists {
		return errors.Wrapf(ErrAlreadyExists, "resource definition %q", rd.Name)
	}

	s.m[rd.Name] = *rd

	return nil
}

func (s *inMemoryResourceDefinitions) Update(_ context.Context, rd *apiv1.ResourceDefinition) error {
	if rd == nil {
		return errors.New("nil ResourceDefinition")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.m[rd.Name]; !exists {
		return errors.Wrapf(ErrNotFound, "resource definition %q", rd.Name)
	}

	s.m[rd.Name] = *rd

	return nil
}

// PatchResourceDefinitionSpec runs `mutate` atomically against the live
// entry under the write lock. The InMemory store has no resourceVersion
// surface, so a lock-held single-shot mutate covers what RetryOnConflict
// does on the k8s backend. Bug 204b shim.
func (s *inMemoryResourceDefinitions) PatchResourceDefinitionSpec(_ context.Context, name string, mutate func(*apiv1.ResourceDefinition) error) error {
	if mutate == nil {
		return errors.New("nil mutate")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rd, ok := s.m[name]
	if !ok {
		return errors.Wrapf(ErrNotFound, "resource definition %q", name)
	}

	if err := mutate(&rd); err != nil {
		return errors.Wrapf(err, "patch resource definition %q", name)
	}

	s.m[name] = rd

	return nil
}

func (s *inMemoryResourceDefinitions) Delete(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.m[name]; !exists {
		return errors.Wrapf(ErrNotFound, "resource definition %q", name)
	}

	delete(s.m, name)

	return nil
}
