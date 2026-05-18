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

package controllers

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// primaryWatchPredicate mirrors the exact composition the
// ResourceReconciler's `For` watch installs in SetupWithManager:
// nodeNamePredicate AND GenerationChangedPredicate. Kept here as
// a constructor so the unit tests assert on the COMPOSED filter,
// not on either ingredient in isolation — the regression of
// Bug 313 was about how these two interact at the watch edge.
func primaryWatchPredicate(nodeName string) predicate.Predicate {
	return predicate.And(
		nodeNamePredicate(nodeName),
		predicate.GenerationChangedPredicate{},
	)
}

// TestPrimaryWatchFiltersStatusOnlyUpdate pins the central P0-1
// claim: a Status-only write from the observer (Generation does
// NOT bump because the CRD enables `subresources: { status: {} }`)
// must be dropped at the watch layer, so `drbdadm adjust` is not
// re-fired mid-sync. Without this filter the observer's 5s
// resync + per-events2 frame would re-enqueue every Resource on
// the node, interfering with active replication.
func TestPrimaryWatchFiltersStatusOnlyUpdate(t *testing.T) {
	t.Parallel()

	p := primaryWatchPredicate("n1")
	old := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.n1", Generation: 7},
		Spec:       blockstoriov1alpha1.ResourceSpec{NodeName: "n1"},
	}
	// Same Generation: only Status changed. Generation is the
	// only signal GenerationChangedPredicate uses.
	updated := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.n1", Generation: 7},
		Spec:       blockstoriov1alpha1.ResourceSpec{NodeName: "n1"},
	}

	if p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: updated}) {
		t.Errorf("Status-only Update on own-node Resource: predicate let it through (must filter)")
	}
}

// TestPrimaryWatchFiresOnSpecChange pins the dual invariant: a
// real Spec edit (Generation bumped) MUST pass. Otherwise we'd
// stop reacting to e.g. volume resize, layerStack edits, or
// migration moves — silent data-plane drift.
func TestPrimaryWatchFiresOnSpecChange(t *testing.T) {
	t.Parallel()

	p := primaryWatchPredicate("n1")
	old := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.n1", Generation: 7},
		Spec:       blockstoriov1alpha1.ResourceSpec{NodeName: "n1"},
	}
	updated := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.n1", Generation: 8},
		Spec:       blockstoriov1alpha1.ResourceSpec{NodeName: "n1"},
	}

	if !p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: updated}) {
		t.Errorf("Spec Update (Generation bumped) on own-node Resource: predicate dropped it")
	}
}

// TestPrimaryWatchFiresOnCreate pins that GenerationChangedPredicate
// does NOT short-circuit Create — c-r's GenerationChangedPredicate
// only filters Updates. Initial Create must reach Reconcile so we
// render the first .res and bring the resource up.
func TestPrimaryWatchFiresOnCreate(t *testing.T) {
	t.Parallel()

	p := primaryWatchPredicate("n1")
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.n1", Generation: 1},
		Spec:       blockstoriov1alpha1.ResourceSpec{NodeName: "n1"},
	}

	if !p.Create(event.CreateEvent{Object: res}) {
		t.Errorf("CreateEvent on own-node Resource: predicate dropped it (initial reconcile would never fire)")
	}
}

// TestPrimaryWatchFiresOnDelete pins the same for Delete — the
// CRD finalizer dance relies on a Delete event reaching the
// reconciler so local DRBD state gets torn down. A predicate
// that swallowed Deletes would leak kernel resources.
func TestPrimaryWatchFiresOnDelete(t *testing.T) {
	t.Parallel()

	p := primaryWatchPredicate("n1")
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.n1", Generation: 9},
		Spec:       blockstoriov1alpha1.ResourceSpec{NodeName: "n1"},
	}

	if !p.Delete(event.DeleteEvent{Object: res}) {
		t.Errorf("DeleteEvent on own-node Resource: predicate dropped it (teardown would leak)")
	}
}
