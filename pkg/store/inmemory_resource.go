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

// rKey is the composite key (resource definition, node) for the in-memory
// resource index.
type rKey struct {
	rd   string
	node string
}

type inMemoryResources struct {
	mu sync.RWMutex
	m  map[rKey]apiv1.Resource
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
func (s *inMemoryResources) SetState(_ context.Context, rdName, node string, state apiv1.ResourceState, drbdProps map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := rKey{rdName, node}

	existing, exists := s.m[key]
	if !exists {
		return errors.Wrapf(ErrNotFound, "resource %q on node %q", rdName, node)
	}

	existing.State = state

	if len(drbdProps) > 0 {
		if existing.Props == nil {
			existing.Props = map[string]string{}
		}

		maps.Copy(existing.Props, drbdProps)
	}

	s.m[key] = existing

	return nil
}
