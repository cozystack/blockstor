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

package k8s

import (
	"net/http"

	"github.com/cockroachdb/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// mapperProvider is the signature of ctrl.Options.MapperProvider.
type mapperProvider = func(*rest.Config, *http.Client) (meta.RESTMapper, error)

// ownKindsFirst wraps a manager's RESTMapper so blockstor's own kinds are
// mapped without asking the API server.
//
// NewManager registers the field indexes before the manager starts, and
// registering one creates the informer for its kind, which resolves the kind's
// REST mapping. The default mapper resolves it by discovery, so construction
// used to need a reachable API server, and both binaries exit when it fails: a
// server briefly unreachable at pod start became a crash loop, where
// ctrl.NewManager on its own leaves the pod waiting on cache sync.
//
// Every blockstor kind is a cluster-scoped CRD in one group version, with the
// plural the default guess produces, so the mapping is known in process. The
// test that holds this compares it with the CRDs under config/crd/bases, so a
// new kind or a changed scope cannot drift from it silently. Anything outside
// the group goes to the wrapped mapper unchanged.
func ownKindsFirst(next mapperProvider) (mapperProvider, error) {
	own, err := ownKindsRESTMapper()
	if err != nil {
		return nil, err
	}

	if next == nil {
		next = apiutil.NewDynamicRESTMapper
	}

	return func(cfg *rest.Config, httpClient *http.Client) (meta.RESTMapper, error) {
		wrapped, err := next(cfg, httpClient)
		if err != nil {
			return nil, err
		}

		return &groupFirstMapper{group: crdv1alpha1.GroupVersion.Group, own: own, rest: wrapped}, nil
	}, nil
}

// ownKindsRESTMapper maps every object kind the API package registers: a kind
// counts when its List kind is registered too, which leaves out the option and
// watch-event types AddToScheme puts in the same group version.
func ownKindsRESTMapper() (meta.RESTMapper, error) {
	scheme := runtime.NewScheme()

	err := crdv1alpha1.AddToScheme(scheme)
	if err != nil {
		return nil, errors.Wrap(err, "register blockstor kinds")
	}

	groupVersion := crdv1alpha1.GroupVersion
	known := scheme.KnownTypes(groupVersion)
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{groupVersion})

	for kind := range known {
		if _, listed := known[kind+"List"]; listed {
			mapper.Add(groupVersion.WithKind(kind), meta.RESTScopeRoot)
		}
	}

	return mapper, nil
}

// groupFirstMapper answers for one API group from its own mapper and asks the
// rest mapper for every other group, and for anything in the group its own
// mapper does not know.
type groupFirstMapper struct {
	group string
	own   meta.RESTMapper
	rest  meta.RESTMapper
}

func (m *groupFirstMapper) KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	if resource.Group == m.group {
		gvk, err := m.own.KindFor(resource)
		if !meta.IsNoMatchError(err) {
			return gvk, err //nolint:wrapcheck // typed RESTMapper errors are matched by callers
		}
	}

	return m.rest.KindFor(resource) //nolint:wrapcheck // typed RESTMapper errors are matched by callers
}

func (m *groupFirstMapper) KindsFor(resource schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	if resource.Group == m.group {
		gvks, err := m.own.KindsFor(resource)
		if !meta.IsNoMatchError(err) {
			return gvks, err //nolint:wrapcheck // typed RESTMapper errors are matched by callers
		}
	}

	return m.rest.KindsFor(resource) //nolint:wrapcheck // typed RESTMapper errors are matched by callers
}

func (m *groupFirstMapper) ResourceFor(input schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	if input.Group == m.group {
		gvr, err := m.own.ResourceFor(input)
		if !meta.IsNoMatchError(err) {
			return gvr, err //nolint:wrapcheck // typed RESTMapper errors are matched by callers
		}
	}

	return m.rest.ResourceFor(input) //nolint:wrapcheck // typed RESTMapper errors are matched by callers
}

func (m *groupFirstMapper) ResourcesFor(input schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	if input.Group == m.group {
		gvrs, err := m.own.ResourcesFor(input)
		if !meta.IsNoMatchError(err) {
			return gvrs, err //nolint:wrapcheck // typed RESTMapper errors are matched by callers
		}
	}

	return m.rest.ResourcesFor(input) //nolint:wrapcheck // typed RESTMapper errors are matched by callers
}

func (m *groupFirstMapper) RESTMapping(groupKind schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	if groupKind.Group == m.group {
		mapping, err := m.own.RESTMapping(groupKind, versions...)
		if !meta.IsNoMatchError(err) {
			return mapping, err //nolint:wrapcheck // typed RESTMapper errors are matched by callers
		}
	}

	return m.rest.RESTMapping(groupKind, versions...) //nolint:wrapcheck // typed RESTMapper errors are matched by callers
}

func (m *groupFirstMapper) RESTMappings(groupKind schema.GroupKind, versions ...string) ([]*meta.RESTMapping, error) {
	if groupKind.Group == m.group {
		mappings, err := m.own.RESTMappings(groupKind, versions...)
		if !meta.IsNoMatchError(err) {
			return mappings, err //nolint:wrapcheck // typed RESTMapper errors are matched by callers
		}
	}

	return m.rest.RESTMappings(groupKind, versions...) //nolint:wrapcheck // typed RESTMapper errors are matched by callers
}

func (m *groupFirstMapper) ResourceSingularizer(resource string) (string, error) {
	singular, err := m.own.ResourceSingularizer(resource)
	if err == nil {
		return singular, nil
	}

	return m.rest.ResourceSingularizer(resource) //nolint:wrapcheck // typed RESTMapper errors are matched by callers
}
