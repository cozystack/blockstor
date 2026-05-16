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

type inMemoryResourceGroups struct {
	mu sync.RWMutex
	m  map[string]apiv1.ResourceGroup
}

func (s *inMemoryResourceGroups) List(_ context.Context) ([]apiv1.ResourceGroup, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]apiv1.ResourceGroup, 0, len(s.m))
	for k := range s.m {
		out = append(out, s.m[k])
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

func (s *inMemoryResourceGroups) Get(_ context.Context, name string) (apiv1.ResourceGroup, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rg, ok := s.m[name]
	if !ok {
		return apiv1.ResourceGroup{}, errors.Wrapf(ErrNotFound, "resource group %q", name)
	}

	return rg, nil
}

func (s *inMemoryResourceGroups) Create(_ context.Context, rg *apiv1.ResourceGroup) error {
	if rg == nil {
		return errors.New("nil ResourceGroup")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.m[rg.Name]; exists {
		return errors.Wrapf(ErrAlreadyExists, "resource group %q", rg.Name)
	}

	s.m[rg.Name] = *rg

	return nil
}

func (s *inMemoryResourceGroups) Update(_ context.Context, rg *apiv1.ResourceGroup) error {
	if rg == nil {
		return errors.New("nil ResourceGroup")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.m[rg.Name]; !exists {
		return errors.Wrapf(ErrNotFound, "resource group %q", rg.Name)
	}

	s.m[rg.Name] = *rg

	return nil
}

// PatchResourceGroup runs `mutate` atomically against the live entry
// under the write lock — the InMemory store has no resourceVersion
// surface so a single lock-held mutate doubles as the retry loop the
// k8s backend needs. Disjoint concurrent edits all converge.
func (s *inMemoryResourceGroups) PatchResourceGroup(_ context.Context, name string, mutate func(*apiv1.ResourceGroup) error) error {
	if mutate == nil {
		return errors.New("nil mutate")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rg, ok := s.m[name]
	if !ok {
		return errors.Wrapf(ErrNotFound, "resource group %q", name)
	}

	if err := mutate(&rg); err != nil {
		return errors.Wrapf(err, "patch ResourceGroup %q", name)
	}

	s.m[name] = rg

	return nil
}

func (s *inMemoryResourceGroups) Delete(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.m[name]; !exists {
		return errors.Wrapf(ErrNotFound, "resource group %q", name)
	}

	delete(s.m, name)

	return nil
}
