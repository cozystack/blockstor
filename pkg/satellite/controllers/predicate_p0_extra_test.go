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

// TestSiblingPredicateDropsMultipleStatusOnlyFields pins the
// noise-filter half of Bug 313: an Update that bumps multiple Status
// fields simultaneously (InUse + DrbdState + Volumes[i].DiskState all
// change as the observer flushes a backlog) MUST still be dropped when
// Generation and DRBDNodeID stay stable. The observer's resyncLoop can
// burst-emit these aggregated updates during reconnect storms; without
// the filter, every per-second resync tick would re-fire the local
// reconcile and serialize peer Spec changes.
func TestSiblingPredicateDropsMultipleStatusOnlyFields(t *testing.T) {
	t.Parallel()

	p := siblingResourceChangedPredicate()
	nodeID := int32(1)

	oldRes := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.n2", Generation: 5},
		Status: blockstoriov1alpha1.ResourceStatus{
			InUse:      false,
			DRBDNodeID: &nodeID,
			DrbdState:  "Inconsistent",
			Volumes: []blockstoriov1alpha1.ResourceVolumeStatus{
				{VolumeNumber: 0, DiskState: "Inconsistent", OutOfSyncKib: 1024},
			},
		},
	}

	newRes := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.n2", Generation: 5},
		Status: blockstoriov1alpha1.ResourceStatus{
			InUse:      true,
			DRBDNodeID: &nodeID,
			DrbdState:  "UpToDate",
			Volumes: []blockstoriov1alpha1.ResourceVolumeStatus{
				{VolumeNumber: 0, DiskState: "UpToDate", OutOfSyncKib: 0},
			},
		},
	}

	if p.Update(event.UpdateEvent{ObjectOld: oldRes, ObjectNew: newRes}) {
		t.Errorf("predicate let a multi-field Status-only Update through; " +
			"Bug 313 noise gate broken (observer resync bursts will spam reconcile)")
	}
}

// TestSiblingPredicatePassesDRBDNodeIDFromSetToDifferent pins the
// rare-but-real cluster-reset path: the controller's allocator
// reclaimed and re-assigned a peer's DRBDNodeID (e.g. after a peer
// re-creation following a node decommission). Bug 313's filter must
// still let this Update through — the local .res render depends on
// peer node-ids being correct, and a stale value pins the wrong slot
// in metadata.
func TestSiblingPredicatePassesDRBDNodeIDFromSetToDifferent(t *testing.T) {
	t.Parallel()

	p := siblingResourceChangedPredicate()
	oldID := int32(1)
	newID := int32(2)

	oldRes := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.n2", Generation: 5},
		Status:     blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &oldID},
	}
	newRes := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.n2", Generation: 5},
		Status:     blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &newID},
	}

	if !p.Update(event.UpdateEvent{ObjectOld: oldRes, ObjectNew: newRes}) {
		t.Errorf("predicate dropped a DRBDNodeID re-assignment; "+
			"old=%d new=%d (Bug 313 must let this through)", oldID, newID)
	}
}

// TestSiblingPredicatePassesDRBDNodeIDSetToNil pins the deallocation
// path: the controller cleared a peer's Status.DRBDNodeID (e.g. after
// the peer was marked for removal but before the Resource CRD itself
// was deleted). The local satellite needs to re-render .res without
// the peer's node-id block, so the Update must pass.
func TestSiblingPredicatePassesDRBDNodeIDSetToNil(t *testing.T) {
	t.Parallel()

	p := siblingResourceChangedPredicate()
	oldID := int32(1)

	oldRes := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.n2", Generation: 5},
		Status:     blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &oldID},
	}
	newRes := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.n2", Generation: 5},
		Status:     blockstoriov1alpha1.ResourceStatus{DRBDNodeID: nil},
	}

	if !p.Update(event.UpdateEvent{ObjectOld: oldRes, ObjectNew: newRes}) {
		t.Errorf("predicate dropped a DRBDNodeID set→nil transition; " +
			"peer-removal flows would skip the .res re-render")
	}
}

// TestSiblingPredicatePassesUnknownTypes pins the fail-open behaviour:
// when the predicate sees an Update event whose objects aren't
// Resources (cache informer mix-up, custom event source) it MUST
// default to passing the event through. Filtering an unknown type
// silently could black-hole legitimate signals; the reconciler's
// inner type-switch will reject anything it can't handle.
func TestSiblingPredicatePassesUnknownTypes(t *testing.T) {
	t.Parallel()

	p := siblingResourceChangedPredicate()

	oldObj := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "rd"},
	}
	newObj := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "rd"},
	}

	if !p.Update(event.UpdateEvent{ObjectOld: oldObj, ObjectNew: newObj}) {
		t.Errorf("predicate filtered an unknown-type Update; " +
			"fail-open required so type mismatches don't black-hole signals")
	}
}

// TestSiblingPredicateDropsIdenticalSpecAndStatus pins the no-op
// case: an Update where neither Generation nor any Status fields the
// reconciler reads have moved (a noisy informer relist re-emitting the
// same object) MUST be dropped. Otherwise every cache resync would
// re-fire every local reconcile across the cluster.
func TestSiblingPredicateDropsIdenticalSpecAndStatus(t *testing.T) {
	t.Parallel()

	p := siblingResourceChangedPredicate()
	nodeID := int32(1)

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.n2", Generation: 5},
		Status: blockstoriov1alpha1.ResourceStatus{
			InUse:      true,
			DRBDNodeID: &nodeID,
			DrbdState:  "UpToDate",
		},
	}

	if p.Update(event.UpdateEvent{ObjectOld: res, ObjectNew: res.DeepCopy()}) {
		t.Errorf("predicate let an identical-object Update through; " +
			"informer relists would spam reconcile")
	}
}
