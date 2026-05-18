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

// TestSiblingResourceChangedPredicateDropsStatusOnlyUpdate pins
// P0-1's sibling-watch filter: when a peer Resource gets a
// Status-only update (the observer's per-tick SSA patches for
// DiskState / OutOfSyncKib / Connections) the predicate MUST drop
// the event. Without this filter, the local satellite's reconcile
// fires on every peer's status tick — re-rendering the .res and
// running `drbdadm adjust` 1× per second per peer, which during a
// SyncTarget can roll the bitmap edge back and produce the
// state-auto-resync flake.
//
// The fake oldRes and newRes share the same .Generation and
// .Status.DRBDNodeID — only Status.InUse changes, simulating an
// observer push.
func TestSiblingResourceChangedPredicateDropsStatusOnlyUpdate(t *testing.T) {
	t.Parallel()

	p := siblingResourceChangedPredicate()

	nodeID := int32(1)
	oldRes := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.n2", Generation: 5},
		Status: blockstoriov1alpha1.ResourceStatus{
			InUse:       false,
			DRBDNodeID:  &nodeID,
			DrbdState:   "UpToDate",
		},
	}
	newRes := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.n2", Generation: 5},
		Status: blockstoriov1alpha1.ResourceStatus{
			InUse:       true, // <- only Status field that moved
			DRBDNodeID:  &nodeID,
			DrbdState:   "UpToDate",
		},
	}

	if p.Update(event.UpdateEvent{ObjectOld: oldRes, ObjectNew: newRes}) {
		t.Errorf("predicate let a Status-only Update through; P0-1 broken")
	}
}

// TestSiblingResourceChangedPredicatePassesSpecChange pins the
// inverse: an Update that bumps .Generation (a Spec edit) MUST
// pass. The dispatcher's peer-block emission depends on peer Spec
// (NodeName, StoragePool, Flags), so a Spec change has to wake the
// local reconcile.
func TestSiblingResourceChangedPredicatePassesSpecChange(t *testing.T) {
	t.Parallel()

	p := siblingResourceChangedPredicate()

	oldRes := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.n2", Generation: 5},
	}
	newRes := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.n2", Generation: 6},
	}

	if !p.Update(event.UpdateEvent{ObjectOld: oldRes, ObjectNew: newRes}) {
		t.Errorf("predicate dropped a Generation-bumped Update; Spec changes must pass")
	}
}

// TestSiblingResourceChangedPredicatePassesDRBDNodeIDTransition
// pins the second half of P0-1's filter: a Status update that
// transitions DRBDNodeID (nil → set, set → different) MUST pass.
// The local satellite's .res renders the peer's `node-id` from
// peer.Status.DRBDNodeID; the controller-side allocator stamps
// the value out-of-band of Spec, and dropping the transition
// freezes the local reconcile on a stale peer node-id.
func TestSiblingResourceChangedPredicatePassesDRBDNodeIDTransition(t *testing.T) {
	t.Parallel()

	p := siblingResourceChangedPredicate()

	newID := int32(2)
	oldRes := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.n2", Generation: 5},
		Status:     blockstoriov1alpha1.ResourceStatus{DRBDNodeID: nil},
	}
	newRes := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.n2", Generation: 5},
		Status:     blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &newID},
	}

	if !p.Update(event.UpdateEvent{ObjectOld: oldRes, ObjectNew: newRes}) {
		t.Errorf("predicate dropped a Status.DRBDNodeID transition; P0-1 must let it through")
	}
}

// TestSiblingResourceChangedPredicateAlwaysAcceptsCreateDelete:
// Create / Delete / Generic events MUST always pass through —
// the local satellite's .res depends on the full peer set, so
// adding or removing a peer-Resource always re-renders.
func TestSiblingResourceChangedPredicateAlwaysAcceptsCreateDelete(t *testing.T) {
	t.Parallel()

	p := siblingResourceChangedPredicate()
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.n2"},
	}

	if !p.Create(event.CreateEvent{Object: res}) {
		t.Errorf("Create event dropped — must always pass")
	}

	if !p.Delete(event.DeleteEvent{Object: res}) {
		t.Errorf("Delete event dropped — must always pass")
	}

	if !p.Generic(event.GenericEvent{Object: res}) {
		t.Errorf("Generic event dropped — must always pass")
	}
}

// TestObserverEnqueuesOnSlotDestroy pins P0-4's closed-loop
// re-trigger: when handleObservation sees an observation with
// ResourceRemoved=true (translateResourceEvent for a kernel-
// emitted `destroy resource <name>` frame), it pushes a
// GenericEvent for the matching Resource name onto the
// ReconcileTrigger channel. Without this hook P0-1's
// Generation-only predicate would leave a `drbdadm down`-d
// resource invisible to the reconciler — recovery-down-reverses
// would wedge.
//
// Asserts the GenericEvent's Object carries the
// `<resource>.<node>` name shape the Resource reconciler expects
// (Resources are named after the DRBD slot, not the bare RD).
func TestObserverEnqueuesOnSlotDestroy(t *testing.T) {
	t.Parallel()

	trigger := make(chan event.GenericEvent, 1)
	obs := &ObserverRunnable{
		NodeName:         "n1",
		ReconcileTrigger: trigger,
	}

	obs.enqueueReconcileTrigger("pvc-down")

	select {
	case ev := <-trigger:
		if ev.Object == nil {
			t.Fatalf("GenericEvent.Object is nil")
		}

		got := ev.Object.GetName()
		want := "pvc-down.n1"

		if got != want {
			t.Errorf("GenericEvent name = %q, want %q", got, want)
		}
	default:
		t.Fatalf("no GenericEvent pushed onto ReconcileTrigger after destroy resource event")
	}
}

// TestObserverTriggerHandlesNilChannel pins the unit-test path
// (and any production satellite that wires the observer without
// the manager): a nil ReconcileTrigger is a no-op, NOT a panic.
// Without this guard, every observer constructed by an isolated
// test would panic on the first destroy-resource event.
func TestObserverTriggerHandlesNilChannel(t *testing.T) {
	t.Parallel()

	obs := &ObserverRunnable{
		NodeName:         "n1",
		ReconcileTrigger: nil,
	}

	// Must not panic.
	obs.enqueueReconcileTrigger("pvc-x")
}

// TestObserverTriggerSkipsEmptyResourceName pins the empty-name
// branch: a malformed observation (resource name missing) must
// not push a GenericEvent with a blank Object name — the
// reconciler would Get/Reconcile on "" and either NotFound-spam
// or, worse, list-match a wrong namespace.
func TestObserverTriggerSkipsEmptyResourceName(t *testing.T) {
	t.Parallel()

	trigger := make(chan event.GenericEvent, 1)
	obs := &ObserverRunnable{
		NodeName:         "n1",
		ReconcileTrigger: trigger,
	}

	obs.enqueueReconcileTrigger("")

	select {
	case ev := <-trigger:
		t.Fatalf("empty resource name produced GenericEvent name=%q (must be skipped)",
			ev.Object.GetName())
	default:
		// expected
	}
}
