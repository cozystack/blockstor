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
	"context"
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestResourceDefinitionGetCacheMissFallsBackToAPIReader pins the RD-404
// attach race. The apiserver runs multi-replica with no leader election, so a
// GET /v1/resource-definitions/{rd} can load-balance to a replica whose
// informer cache has not yet observed a just-committed RD create. Before the
// fix resourceDefinitions.Get read only the cached client and returned a
// spurious store.ErrNotFound, which the REST layer surfaces as a 404 to
// linstor-csi — ControllerPublishVolume then fails ("404 Not Found") and backs
// off, slowing bulk attach of freshly-provisioned PVCs. Get must fall back to
// the direct (uncached) API reader and resolve the RD that already exists.
func TestResourceDefinitionGetCacheMissFallsBackToAPIReader(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	const name = "pvc-rd404"

	crd := wireToCRDRD(&apiv1.ResourceDefinition{Name: name})

	// cached: this replica's informer cache trails the create — RD absent.
	cached := fake.NewClientBuilder().WithScheme(scheme).Build()
	// apiReader: the direct, uncached read sees the committed RD.
	apiReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(crd).Build()

	rd := &resourceDefinitions{c: cached, apiReader: apiReader}

	got, err := rd.Get(context.Background(), name)
	if err != nil {
		t.Fatalf("Get with apiReader fallback: unexpected error: %v", err)
	}

	if got.Name != name {
		t.Fatalf("got RD name %q, want %q", got.Name, name)
	}
}

// TestResourceDefinitionGetNilAPIReaderKeepsCachedNotFound pins that the
// fallback is gated on a non-nil apiReader: in-memory / unit stores (which
// have no informer and pass a nil reader) must preserve the cached NotFound
// rather than dereference a nil reader.
func TestResourceDefinitionGetNilAPIReaderKeepsCachedNotFound(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	cached := fake.NewClientBuilder().WithScheme(scheme).Build()

	rd := &resourceDefinitions{c: cached}

	if _, err := rd.Get(context.Background(), "pvc-absent"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Get with nil apiReader: got %v, want store.ErrNotFound", err)
	}
}
