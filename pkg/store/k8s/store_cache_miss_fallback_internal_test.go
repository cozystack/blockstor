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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store"
)

// These tests align volumeDefinitions.Get with the released
// resourceDefinitions.Get cache-miss behaviour (multi-replica
// apiserver, no leader election). VDs are EMBEDDED in the RD CRD
// spec, so before this fix the SAME committed RD answered differently
// by path: `GET /v1/resource-definitions/{rd}` resolved through the
// RD store's uncached fallback while
// `GET .../volume-definitions/{vn}` read only the lagging cache and
// 404'd (observed live on the cozystack e2e stand as
// `GET .../volume-definitions/0 -> 404` with the REST-layer
// cache-retry budget exhausted).
//
// Deliberately NOT extended to the other stores: raw store Gets are
// cache-only by design — REST existence probes rely on a fast cached
// NotFound, and the REST layer's get*WithCacheRetry helpers already
// absorb cross-replica lag while preserving read-your-writes for
// subsequent cached Lists. A store-level uncached fallback
// short-circuits that convergence wait (demonstrated by the CI
// snapshot-pagination regression when it was tried).

func cacheMissScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	return scheme
}

func emptyClient(t *testing.T, scheme *runtime.Scheme) ctrlclient.Client {
	t.Helper()

	return fake.NewClientBuilder().WithScheme(scheme).Build()
}

func clientWith(t *testing.T, scheme *runtime.Scheme, objs ...ctrlclient.Object) ctrlclient.Client {
	t.Helper()

	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

// TestVolumeDefinitionsGetStaleRDFallsBackToLiveRead pins the subtle VD
// staleness mode: the lagging replica's cache can hold the RD itself
// while still missing a just-committed volume-definition. The cached
// read alone can not distinguish "VD absent" from "RD revision stale"
// — Get must re-read the RD live before answering 404.
func TestVolumeDefinitionsGetStaleRDFallsBackToLiveRead(t *testing.T) {
	t.Parallel()

	scheme := cacheMissScheme(t)

	stale := &crdv1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: Name("pvc-stale")},
	}
	fresh := &crdv1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: Name("pvc-stale")},
		Spec: crdv1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []crdv1alpha1.ResourceDefinitionVolume{{
				VolumeNumber: 0,
				SizeKib:      1 << 20,
			}},
		},
	}

	st := &volumeDefinitions{c: clientWith(t, scheme, stale), apiReader: clientWith(t, scheme, fresh)}

	got, err := st.Get(context.Background(), "pvc-stale", 0)
	if err != nil {
		t.Fatalf("Get with live re-read fallback: unexpected error: %v", err)
	}

	if got.SizeKib != 1<<20 {
		t.Fatalf("got SizeKib %d, want %d", got.SizeKib, 1<<20)
	}

	// Truly-absent VD must still be NotFound after the live re-read.
	if _, err := st.Get(context.Background(), "pvc-stale", 7); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Get of absent VD: got %v, want store.ErrNotFound", err)
	}
}

// TestVolumeDefinitionsGetCacheMissRDFallsBackToLiveRead pins the
// coarser mode: the RD itself has not reached this replica's cache yet.
func TestVolumeDefinitionsGetCacheMissRDFallsBackToLiveRead(t *testing.T) {
	t.Parallel()

	scheme := cacheMissScheme(t)

	fresh := &crdv1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: Name("pvc-vdmiss")},
		Spec: crdv1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []crdv1alpha1.ResourceDefinitionVolume{{
				VolumeNumber: 0,
				SizeKib:      4096,
			}},
		},
	}

	st := &volumeDefinitions{c: emptyClient(t, scheme), apiReader: clientWith(t, scheme, fresh)}

	got, err := st.Get(context.Background(), "pvc-vdmiss", 0)
	if err != nil {
		t.Fatalf("Get with live re-read fallback: unexpected error: %v", err)
	}

	if got.SizeKib != 4096 {
		t.Fatalf("got SizeKib %d, want %d", got.SizeKib, 4096)
	}

	nilReader := &volumeDefinitions{c: emptyClient(t, scheme)}
	if _, err := nilReader.Get(context.Background(), "pvc-vdmiss", 0); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Get with nil apiReader: got %v, want store.ErrNotFound", err)
	}
}
