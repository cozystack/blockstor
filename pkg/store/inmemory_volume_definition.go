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

type inMemoryKVStore struct {
	mu sync.RWMutex
	m  map[string]map[string]string
}

func (s *inMemoryKVStore) ListInstances(_ context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]string, 0, len(s.m))
	for k := range s.m {
		out = append(out, k)
	}

	sort.Strings(out)

	return out, nil
}

func (s *inMemoryKVStore) GetInstance(_ context.Context, instance string) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	props, ok := s.m[instance]
	if !ok {
		return nil, errors.Wrapf(ErrNotFound, "kv instance %q", instance)
	}

	out := make(map[string]string, len(props))
	maps.Copy(out, props)

	return out, nil
}

// SetKeys applies the upstream override/delete payload atomically. Creates
// the instance on first set so callers don't have to do a separate Create.
func (s *inMemoryKVStore) SetKeys(_ context.Context, instance string, modify apiv1.GenericPropsModify) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	props, ok := s.m[instance]
	if !ok {
		props = map[string]string{}
		s.m[instance] = props
	}

	maps.Copy(props, modify.OverrideProps)

	for _, k := range modify.DeleteProps {
		delete(props, k)
	}

	for _, ns := range modify.DeleteNamespace {
		for k := range props {
			if hasPrefixSegment(k, ns) {
				delete(props, k)
			}
		}
	}

	return nil
}

func (s *inMemoryKVStore) DeleteInstance(_ context.Context, instance string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.m[instance]; !ok {
		return errors.Wrapf(ErrNotFound, "kv instance %q", instance)
	}

	delete(s.m, instance)

	return nil
}

// hasPrefixSegment reports whether key sits under the namespace path prefix.
// LINSTOR namespaces are slash-separated, so we anchor on `<ns>` or `<ns>/`.
func hasPrefixSegment(key, ns string) bool {
	if key == ns {
		return true
	}

	if len(key) > len(ns) && key[:len(ns)] == ns && key[len(ns)] == '/' {
		return true
	}

	return false
}
