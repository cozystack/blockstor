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

package storetest

import (
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// Bug-021 — Update annotation contract, shared by every annotation-
// carrying store (RG, RD, Resource, Snapshot):
//
//   - NIL wire `Annotations` map  → stored user annotations untouched.
//   - NON-NIL map (incl. EMPTY)   → user-annotation set replaced with
//     exactly the wire set; an empty map clears all user annotations.
//
// The k8s backend implements this via mergeUserAnnotationsInto's
// early-return on nil; pre-fix the in-memory backend replaced rows
// wholesale, so "nil = untouched" silently diverged and the
// production-only k8s behaviour (a stripped-to-empty map nil-ed by
// the caller never reaching the CRD) was invisible to unit tests.
// These subtests pin all three transitions against both backends.

// annotationOps adapts one store type to the shared contract walk.
// `update` must send a FULL wire object whose Annotations field is
// exactly the given map (nil stays nil).
type annotationOps struct {
	create func(t *testing.T, annotations map[string]string)
	update func(t *testing.T, annotations map[string]string)
	get    func(t *testing.T) map[string]string
}

const (
	contractMarkerKey   = "blockstor.io/rebalance-pending"
	contractMarkerValue = "2026-06-12T00:00:00Z"
	contractUserKey     = "aux/operator-note"
	contractUserValue   = "keep-me"
)

// runUpdateAnnotationContract drives the three contract transitions:
// nil-is-untouched, non-nil-replaces, empty-clears.
func runUpdateAnnotationContract(t *testing.T, ops annotationOps) {
	t.Helper()

	ops.create(t, map[string]string{
		contractMarkerKey: contractMarkerValue,
		contractUserKey:   contractUserValue,
	})

	// 1) nil wire map: both annotations survive.
	ops.update(t, nil)

	got := ops.get(t)
	if got[contractMarkerKey] != contractMarkerValue || got[contractUserKey] != contractUserValue {
		t.Fatalf("Update(nil annotations) must leave annotations untouched; got %v", got)
	}

	// 2) non-nil map: replaces the full user set — the marker key is
	// dropped, the listed key survives.
	ops.update(t, map[string]string{contractUserKey: contractUserValue})

	got = ops.get(t)
	if _, still := got[contractMarkerKey]; still {
		t.Fatalf("Update(non-nil annotations) must drop unlisted keys; %q survived: %v",
			contractMarkerKey, got)
	}

	if got[contractUserKey] != contractUserValue {
		t.Fatalf("Update(non-nil annotations) must keep listed keys; got %v", got)
	}

	// 3) EMPTY non-nil map: clears every user annotation. This is the
	// Bug-021 load-bearing transition — the rebalance / shortfall
	// strips delete the last marker and must be able to persist the
	// now-empty set.
	ops.update(t, map[string]string{})

	got = ops.get(t)
	if len(got) != 0 {
		t.Fatalf("Update(empty annotations) must clear all user annotations; got %v", got)
	}
}

func testRGUpdateAnnotationContract(t *testing.T, newStore Factory) {
	t.Helper()

	s := newStore(t).ResourceGroups()
	const name = "rg-ann-contract"

	runUpdateAnnotationContract(t, annotationOps{
		create: func(t *testing.T, ann map[string]string) {
			t.Helper()
			if err := s.Create(t.Context(), &apiv1.ResourceGroup{Name: name, Annotations: ann}); err != nil {
				t.Fatalf("Create: %v", err)
			}
		},
		update: func(t *testing.T, ann map[string]string) {
			t.Helper()
			if err := s.Update(t.Context(), &apiv1.ResourceGroup{Name: name, Annotations: ann}); err != nil {
				t.Fatalf("Update: %v", err)
			}
		},
		get: func(t *testing.T) map[string]string {
			t.Helper()
			got, err := s.Get(t.Context(), name)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}

			return got.Annotations
		},
	})
}

func testRDUpdateAnnotationContract(t *testing.T, newStore Factory) {
	t.Helper()

	s := newStore(t).ResourceDefinitions()
	const name = "rd-ann-contract"

	runUpdateAnnotationContract(t, annotationOps{
		create: func(t *testing.T, ann map[string]string) {
			t.Helper()
			if err := s.Create(t.Context(), &apiv1.ResourceDefinition{Name: name, Annotations: ann}); err != nil {
				t.Fatalf("Create: %v", err)
			}
		},
		update: func(t *testing.T, ann map[string]string) {
			t.Helper()
			if err := s.Update(t.Context(), &apiv1.ResourceDefinition{Name: name, Annotations: ann}); err != nil {
				t.Fatalf("Update: %v", err)
			}
		},
		get: func(t *testing.T) map[string]string {
			t.Helper()
			got, err := s.Get(t.Context(), name)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}

			return got.Annotations
		},
	})
}

func testResourceUpdateAnnotationContract(t *testing.T, newStore Factory) {
	t.Helper()

	s := newStore(t).Resources()

	const (
		rdName = "pvc-ann-contract"
		node   = "n1"
	)

	runUpdateAnnotationContract(t, annotationOps{
		create: func(t *testing.T, ann map[string]string) {
			t.Helper()
			if err := s.Create(t.Context(), &apiv1.Resource{Name: rdName, NodeName: node, Annotations: ann}); err != nil {
				t.Fatalf("Create: %v", err)
			}
		},
		update: func(t *testing.T, ann map[string]string) {
			t.Helper()
			if err := s.Update(t.Context(), &apiv1.Resource{Name: rdName, NodeName: node, Annotations: ann}); err != nil {
				t.Fatalf("Update: %v", err)
			}
		},
		get: func(t *testing.T) map[string]string {
			t.Helper()
			got, err := s.Get(t.Context(), rdName, node)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}

			return got.Annotations
		},
	})
}

func testSnapshotUpdateAnnotationContract(t *testing.T, newStore Factory) {
	t.Helper()

	s := newStore(t).Snapshots()

	const (
		rdName   = "pvc-ann-contract"
		snapName = "snap-ann-contract"
	)

	runUpdateAnnotationContract(t, annotationOps{
		create: func(t *testing.T, ann map[string]string) {
			t.Helper()
			if err := s.Create(t.Context(), &apiv1.Snapshot{Name: snapName, ResourceName: rdName, Annotations: ann}); err != nil {
				t.Fatalf("Create: %v", err)
			}
		},
		update: func(t *testing.T, ann map[string]string) {
			t.Helper()
			if err := s.Update(t.Context(), &apiv1.Snapshot{Name: snapName, ResourceName: rdName, Annotations: ann}); err != nil {
				t.Fatalf("Update: %v", err)
			}
		},
		get: func(t *testing.T) map[string]string {
			t.Helper()
			got, err := s.Get(t.Context(), rdName, snapName)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}

			return got.Annotations
		},
	})
}
