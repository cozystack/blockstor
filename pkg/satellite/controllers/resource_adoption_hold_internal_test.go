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
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

// adoptedMesh is worker-0's kernel view of a three-replica resource
// LINSTOR brought up: both peers Connected.
const adoptedMesh = `[{"name": "pvc-1", "connections": [
  {"peer-node-id": 1, "name": "worker-1", "connection-state": "Connected", "peer_devices": [{"volume": 0}]},
  {"peer-node-id": 2, "name": "worker-2", "connection-state": "Connected", "peer_devices": [{"volume": 0}]}
]}]`

func adoptedReplica(node string, id int32) blockstoriov1alpha1.Resource {
	port, minor := int32(7000), int32(1000)

	return blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1." + node},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               node,
			DRBDNodeID:             &id,
		},
		Status: blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &id, DRBDPort: &port, DRBDMinor: &minor},
	}
}

func newAdoptionHoldReconciler(t *testing.T) (*ResourceReconciler, *events.FakeRecorder) {
	t.Helper()

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status -j pvc-1", storage.FakeResponse{Stdout: []byte(adoptedMesh)})

	recorder := events.NewFakeRecorder(4)

	return &ResourceReconciler{Config: Config{
		NodeName: "worker-0",
		Recorder: recorder,
		Apply: satellite.NewReconciler(satellite.ReconcilerConfig{
			NodeName: "worker-0",
			Adm:      drbd.NewAdm(fx),
			StateDir: t.TempDir(),
		}),
	}}, recorder
}

// TestPrepareDRBDApply_HoldsAdoptedRenderForLatePeer: every replica
// observed so far is allocated, so the allocation gate lets the apply
// through, but worker-2's Resource has not arrived while the adopted
// kernel slot is connected to it. Rendering now would let adjust drop
// that connection, so the apply waits; once worker-2 is observed it
// goes ahead.
func TestPrepareDRBDApply_HoldsAdoptedRenderForLatePeer(t *testing.T) {
	r, _ := newAdoptionHoldReconciler(t)
	rd := &blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"}}
	target := adoptedReplica("worker-0", 0)
	partial := []blockstoriov1alpha1.Resource{target, adoptedReplica("worker-1", 1)}
	full := []blockstoriov1alpha1.Resource{target, adoptedReplica("worker-1", 1), adoptedReplica("worker-2", 2)}

	result, ok := r.prepareDRBDApply(t.Context(), &target, rd, partial, logr.Discard())
	if ok || result.RequeueAfter == 0 {
		t.Fatalf("late peer: prepareDRBDApply = (%+v, %v), want a held apply with a requeue", result, ok)
	}

	if _, ok := r.prepareDRBDApply(t.Context(), &target, rd, full, logr.Discard()); !ok {
		t.Fatal("every peer observed: prepareDRBDApply held the apply")
	}
}

// TestPrepareDRBDApply_ExpiredAdoptionHoldWarns: when the late peer's
// Resource never comes, the apply goes ahead after the wait and says,
// as a Warning Event on the Resource, which peers it disconnects.
func TestPrepareDRBDApply_ExpiredAdoptionHoldWarns(t *testing.T) {
	t.Setenv("BSTOR_ADOPTION_PEER_WAIT_S", "1")

	r, recorder := newAdoptionHoldReconciler(t)
	rd := &blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1"}}
	target := adoptedReplica("worker-0", 0)
	partial := []blockstoriov1alpha1.Resource{target, adoptedReplica("worker-1", 1)}

	if _, ok := r.prepareDRBDApply(t.Context(), &target, rd, partial, logr.Discard()); ok {
		t.Fatal("late peer: the first reconcile did not hold the apply")
	}

	time.Sleep(1100 * time.Millisecond)

	if _, ok := r.prepareDRBDApply(t.Context(), &target, rd, partial, logr.Discard()); !ok {
		t.Fatal("wait elapsed: the apply is still held")
	}

	select {
	case ev := <-recorder.Events:
		if !strings.Contains(ev, "Warning AdoptedPeerWithoutResource") || !strings.Contains(ev, "worker-2") {
			t.Errorf("Event = %q, want a Warning AdoptedPeerWithoutResource naming worker-2", ev)
		}
	default:
		t.Error("no Event when the apply went ahead without an adopted peer")
	}
}
