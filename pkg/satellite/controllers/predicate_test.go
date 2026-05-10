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

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// TestNodeNamePredicateMatchesResourceForSelf pins the watch-
// layer filter: a Resource whose Spec.NodeName equals the
// satellite's own name MUST pass the predicate, so its
// reconciler fires. Without this filter every satellite would
// reconcile every Resource in the cluster — N² blow-up on
// big clusters.
func TestNodeNamePredicateMatchesResourceForSelf(t *testing.T) {
	t.Parallel()

	p := nodeNamePredicate("n1")
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.n1"},
		Spec:       blockstoriov1alpha1.ResourceSpec{NodeName: "n1"},
	}

	if !p.Create(event.CreateEvent{Object: res}) {
		t.Errorf("CreateEvent on own-node Resource: predicate dropped it")
	}

	if !p.Delete(event.DeleteEvent{Object: res}) {
		t.Errorf("DeleteEvent on own-node Resource: predicate dropped it")
	}
}

// TestNodeNamePredicateRejectsResourceForOtherNode pins the
// inverse: a Resource on a different node MUST be filtered out.
func TestNodeNamePredicateRejectsResourceForOtherNode(t *testing.T) {
	t.Parallel()

	p := nodeNamePredicate("n1")
	res := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{NodeName: "n2"},
	}

	if p.Create(event.CreateEvent{Object: res}) {
		t.Errorf("CreateEvent on foreign-node Resource: predicate let it through")
	}
}

// TestNodeNamePredicateMatchesStoragePoolForSelf: same filter
// reused for StoragePool. The predicate uses a type switch
// because StoragePool and Resource both carry Spec.NodeName but
// the field's owning type differs.
func TestNodeNamePredicateMatchesStoragePoolForSelf(t *testing.T) {
	t.Parallel()

	p := nodeNamePredicate("n1")
	pool := &blockstoriov1alpha1.StoragePool{
		Spec: blockstoriov1alpha1.StoragePoolSpec{NodeName: "n1"},
	}

	if !p.Create(event.CreateEvent{Object: pool}) {
		t.Errorf("CreateEvent on own-node StoragePool: predicate dropped it")
	}
}

// TestNodeNamePredicateUpdateMatchesEitherSide: an Update event
// whose OLD or NEW state has the right node name must pass —
// this catches the case where a Resource gets MIGRATED off this
// node (the satellite still needs to tear down its local copy)
// or migrates ONTO this node.
func TestNodeNamePredicateUpdateMatchesEitherSide(t *testing.T) {
	t.Parallel()

	p := nodeNamePredicate("n1")
	oldRes := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{NodeName: "n1"},
	}
	newRes := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{NodeName: "n2"}, // migrated away
	}

	if !p.Update(event.UpdateEvent{ObjectOld: oldRes, ObjectNew: newRes}) {
		t.Errorf("UpdateEvent old-was-ours, new-isn't: predicate dropped it (must keep for teardown)")
	}
}

// TestSnapshotNodePredicateMatchesMembership pins the
// list-membership filter: Snapshot.Spec.Nodes is a list of
// node names; the predicate passes when our name is in the
// list. Pinned because the snapshot semantic is per-node — one
// Snapshot CRD triggers CreateSnapshot on EACH node in its
// Nodes list, and the predicate is how each satellite decides
// "this one's mine".
func TestSnapshotNodePredicateMatchesMembership(t *testing.T) {
	t.Parallel()

	p := snapshotNodePredicate("n2")
	snap := &blockstoriov1alpha1.Snapshot{
		Spec: blockstoriov1alpha1.SnapshotSpec{
			SnapshotName: "snap-1",
			Nodes:        []string{"n1", "n2", "n3"},
		},
	}

	if !p.Create(event.CreateEvent{Object: snap}) {
		t.Errorf("CreateEvent on Snapshot with our node in Nodes: predicate dropped it")
	}
}

// TestSnapshotNodePredicateRejectsAbsentNode: when our name is
// NOT in Spec.Nodes the predicate filters the event out.
func TestSnapshotNodePredicateRejectsAbsentNode(t *testing.T) {
	t.Parallel()

	p := snapshotNodePredicate("n4")
	snap := &blockstoriov1alpha1.Snapshot{
		Spec: blockstoriov1alpha1.SnapshotSpec{
			Nodes: []string{"n1", "n2", "n3"},
		},
	}

	if p.Create(event.CreateEvent{Object: snap}) {
		t.Errorf("CreateEvent on Snapshot without our node: predicate let it through")
	}
}

// TestDropAllEventsPredicateAlwaysFalse pins the cache-warming
// invariant: the ResourceDefinitionReconciler's predicate
// MUST drop every event so the cache fills (via the watch the
// manager runs unconditionally) but the reconciler body never
// runs. A regression that let events through would create
// O(RDs)-per-tick reconciles for nothing.
func TestDropAllEventsPredicateAlwaysFalse(t *testing.T) {
	t.Parallel()

	p := dropAllEventsPredicate()
	rd := &blockstoriov1alpha1.ResourceDefinition{}

	if p.Create(event.CreateEvent{Object: rd}) {
		t.Errorf("Create: must always drop")
	}

	if p.Update(event.UpdateEvent{ObjectNew: rd, ObjectOld: rd}) {
		t.Errorf("Update: must always drop")
	}

	if p.Delete(event.DeleteEvent{Object: rd}) {
		t.Errorf("Delete: must always drop")
	}

	if p.Generic(event.GenericEvent{Object: rd}) {
		t.Errorf("Generic: must always drop")
	}
}
