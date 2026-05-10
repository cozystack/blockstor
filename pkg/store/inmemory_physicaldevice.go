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

// inMemoryPhysicalDevices is the InMemory store's PhysicalDeviceStore
// implementation. The map is keyed by PhysicalDevice name (the same
// name the CRD's metadata.name carries — `<node>-<stable-id-slug>`).
type inMemoryPhysicalDevices struct {
	mu sync.RWMutex
	m  map[string]apiv1.PhysicalDevice
}

func (s *inMemoryPhysicalDevices) List(_ context.Context) ([]apiv1.PhysicalDevice, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]apiv1.PhysicalDevice, 0, len(s.m))
	for k := range s.m {
		out = append(out, s.m[k])
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

func (s *inMemoryPhysicalDevices) ListForNode(_ context.Context, nodeName string) ([]apiv1.PhysicalDevice, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]apiv1.PhysicalDevice, 0)

	for k := range s.m {
		if s.m[k].NodeName == nodeName {
			out = append(out, s.m[k])
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

func (s *inMemoryPhysicalDevices) Get(_ context.Context, name string) (apiv1.PhysicalDevice, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	dev, ok := s.m[name]
	if !ok {
		return apiv1.PhysicalDevice{}, errors.Wrapf(ErrNotFound, "physical device %q", name)
	}

	return dev, nil
}

func (s *inMemoryPhysicalDevices) Create(_ context.Context, dev *apiv1.PhysicalDevice) error {
	if dev == nil {
		return errors.New("nil PhysicalDevice")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.m[dev.Name]; exists {
		return errors.Wrapf(ErrAlreadyExists, "physical device %q", dev.Name)
	}

	s.m[dev.Name] = *dev

	return nil
}

func (s *inMemoryPhysicalDevices) Update(_ context.Context, dev *apiv1.PhysicalDevice) error {
	if dev == nil {
		return errors.New("nil PhysicalDevice")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.m[dev.Name]
	if !exists {
		return errors.Wrapf(ErrNotFound, "physical device %q", dev.Name)
	}

	// CAS guard for the attach race — see the same check in the
	// k8s store's Update for the why. Two concurrent attach
	// requests both pass `pickFreeDeviceForAttach`; the second
	// to land must lose with ErrAlreadyExists.
	if dev.AttachTo != nil && existing.AttachTo != nil {
		return errors.Wrapf(ErrAlreadyExists, "physical device %q already attached", dev.Name)
	}

	s.m[dev.Name] = *dev

	return nil
}

func (s *inMemoryPhysicalDevices) Delete(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.m[name]; !exists {
		return errors.Wrapf(ErrNotFound, "physical device %q", name)
	}

	delete(s.m, name)

	return nil
}
