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

package controllers

import (
	"testing"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
)

// TestAllocationWaitTakesTheStampedPeerSlots replays the first replica of
// a spawned definition: its satellite sees the Resource before the
// controller's identities write reaches the cache, re-reads it uncached
// and goes on with the ids it finds there. The replica's peer slots and
// skip-initial-sync decision come in the same controller write, so the
// apply has to carry them too. With the ids alone the cached spec has no
// count, the replica reads as one created before counts were recorded,
// and its lower disk is sized for 15 slots while create-md writes the 7
// stamped on it a pass later: a device larger than the definition, and
// larger than the replicas whose satellites saw the write in one piece,
// which DRBD then refuses to connect.
func TestAllocationWaitTakesTheStampedPeerSlots(t *testing.T) {
	t.Parallel()

	scheme := newToggleDiskTestScheme(t)

	nodeID, port, minor, seven := int32(0), int32(20077), int32(1000), int32(7)
	skip := true

	stamped := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "spawned.node-a"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               "node-a",
			ResourceDefinitionName: "spawned",
			DRBDNodeID:             &nodeID,
			DRBDPort:               &port,
			PeerSlots:              &seven,
			SkipInitialSync:        &skip,
		},
		Status: blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &nodeID, DRBDPort: &port, DRBDMinor: &minor},
	}

	// The cache still holds the revision the REST create wrote.
	cached := stamped.DeepCopy()
	cached.Spec.DRBDNodeID = nil
	cached.Spec.DRBDPort = nil
	cached.Spec.PeerSlots = nil
	cached.Spec.SkipInitialSync = nil
	cached.Status = blockstoriov1alpha1.ResourceStatus{}

	reconciler := &ResourceReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cached.DeepCopy()).Build(),
		Config: Config{
			NodeName:  "node-a",
			APIReader: fake.NewClientBuilder().WithScheme(scheme).WithObjects(stamped).Build(),
		},
	}

	res := cached.DeepCopy()

	if _, ok := reconciler.waitForControllerAllocation(t.Context(), res, nil, logr.Discard()); !ok {
		t.Fatalf("the apiserver holds the controller's write, yet the apply still waits")
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "spawned"}}
	rd.Spec.PeerSlots = &seven

	desired := dispatcher.BuildDesired(res, nil, nil, nil, rd, nil)

	if got := desired.GetPeerSlots(); got == nil || *got != seven {
		t.Errorf("peer slots the lower disk and create-md use = %v, want the %d the controller stamped", got, seven)
	}

	if desired.GetSkipInitialSync() == nil {
		t.Errorf("skip-initial-sync decision lost: the apply would wait a pass for a stamp it was handed")
	}
}
