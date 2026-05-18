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
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

// Phase 11.7 pins the load-bearing combination of:
//  1. A custom primary-watch predicate (primaryWatchPredicate) that
//     drops pure observer Status noise but PASSES controller-allocator
//     Status writes (Status.DRBDNodeID/Port/Minor transitions) plus
//     every Spec change (Generation bump) and every Create/Delete/
//     Generic event.
//  2. An observer-trigger channel the ObserverRunnable emits a
//     GenericEvent onto for every kernel-state lifecycle change
//     (resource/role/device/conn/peer-device frames) so the
//     ResourceReconciler wakes on observed state even when no
//     apiserver write fires a Generation bump.
//  3. A safety-net RequeueAfter on active-DRBD resources so the
//     reconciler self-evaluates periodically even when the events2
//     stream misses a frame.
//
// The three legs together let us drop the pulse-of-Status-writes
// dependency that previously kept the satellite re-reconciling on
// every observer tick. Prior attempts (Bug 313, 316, 318) reverted
// because tests had hidden dependency on observer's Status writes
// re-triggering Reconcile — now mitigated by Phase 11.5.b (Status
// schema comprehensive; tests migrated to k8s-native reads) +
// Bug 315 (idempotent .res).

// triggerTestTimeout caps the per-test wait on a buffered channel
// send/receive so a regression that breaks emit / the non-blocking
// select can't hang the test runner.
const triggerTestTimeout = 250 * time.Millisecond

// newTriggerObserver constructs an ObserverRunnable wired to a fresh
// in-memory fake client + a buffered trigger channel. Shared by the
// observer-trigger tests so each one declares only the bits it
// exercises.
func newTriggerObserver(t *testing.T, bufferSize int) (*ObserverRunnable, chan event.GenericEvent, *drbd.Adm) {
	t.Helper()

	scheme := newToggleDiskTestScheme(t)

	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	fx := storage.NewFakeExec()
	trigger := make(chan event.GenericEvent, bufferSize)

	observer := &ObserverRunnable{
		Client:           cli,
		Exec:             fx,
		NodeName:         "n1",
		ReconcileTrigger: trigger,
	}

	return observer, trigger, drbd.NewAdm(fx)
}

// readOneTrigger pulls one GenericEvent off the channel within the
// test timeout. Failure is fatal — the caller asserts emit happened.
func readOneTrigger(t *testing.T, ch <-chan event.GenericEvent) event.GenericEvent {
	t.Helper()

	select {
	case ev := <-ch:
		return ev
	case <-time.After(triggerTestTimeout):
		t.Fatalf("expected GenericEvent on trigger channel within %v; got none", triggerTestTimeout)
	}

	return event.GenericEvent{}
}

// assertNoTrigger pins the inverse: when emit MUST not fire we wait
// briefly to confirm nothing lands.
func assertNoTrigger(t *testing.T, ch <-chan event.GenericEvent) {
	t.Helper()

	select {
	case ev := <-ch:
		name := "<nil object>"
		if ev.Object != nil {
			name = ev.Object.GetName()
		}

		t.Fatalf("unexpected GenericEvent on trigger channel: %s", name)
	case <-time.After(50 * time.Millisecond):
	}
}

// int32Ptr is a one-line helper so the tests below read more like
// the production allocator's `Status.DRBDNodeID = &id` shape.
func int32Ptr(value int32) *int32 { return &value }

// -----------------------------------------------------------------
// primaryWatchPredicate tests (Phase 11.7 first leg).
// -----------------------------------------------------------------

// TestPrimaryWatchFiltersPureStatusUpdate pins the noise-drop
// invariant: a pure Status.Volumes update (observer round-trip) with
// the allocator IDs unchanged MUST be filtered out. Without this
// drop, every observer Status SSA write would re-fire the reconciler
// — the very feedback loop Phase 11.7 is designed to break.
func TestPrimaryWatchFiltersPureStatusUpdate(t *testing.T) {
	t.Parallel()

	pred := primaryWatchPredicate()

	allocated := blockstoriov1alpha1.ResourceStatus{
		DRBDNodeID: int32Ptr(1),
		DRBDPort:   int32Ptr(7000),
		DRBDMinor:  int32Ptr(1000),
	}

	oldR := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1", Generation: 1},
		Status: blockstoriov1alpha1.ResourceStatus{
			DRBDNodeID: allocated.DRBDNodeID,
			DRBDPort:   allocated.DRBDPort,
			DRBDMinor:  allocated.DRBDMinor,
			Volumes: []blockstoriov1alpha1.ResourceVolumeStatus{
				{VolumeNumber: 0, DiskState: "Inconsistent"},
			},
		},
	}
	newR := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1", Generation: 1},
		Status: blockstoriov1alpha1.ResourceStatus{
			DRBDNodeID: allocated.DRBDNodeID,
			DRBDPort:   allocated.DRBDPort,
			DRBDMinor:  allocated.DRBDMinor,
			Volumes: []blockstoriov1alpha1.ResourceVolumeStatus{
				{VolumeNumber: 0, DiskState: "UpToDate"},
			},
			InUse:     true,
			DrbdState: "UpToDate",
			Connections: []blockstoriov1alpha1.ResourceConnectionStatus{
				{PeerNodeName: "n2", Connected: true, Message: "Connected"},
			},
		},
	}

	if pred.Update(event.UpdateEvent{ObjectOld: oldR, ObjectNew: newR}) {
		t.Errorf("pure observer Status update (allocator IDs unchanged): predicate let it through")
	}
}

// TestPrimaryWatchFiresOnSpecChange pins the Spec-change passthrough:
// a Generation bump (operator re-spec) MUST fire even when the
// allocator IDs and Status are otherwise unchanged. The PUT
// toggle-disk / resize / spec-edit paths all rely on this.
func TestPrimaryWatchFiresOnSpecChange(t *testing.T) {
	t.Parallel()

	pred := primaryWatchPredicate()

	oldR := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1", Generation: 1},
		Status: blockstoriov1alpha1.ResourceStatus{
			DRBDNodeID: int32Ptr(1),
		},
	}
	newR := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1", Generation: 2},
		Status: blockstoriov1alpha1.ResourceStatus{
			DRBDNodeID: int32Ptr(1),
		},
	}

	if !pred.Update(event.UpdateEvent{ObjectOld: oldR, ObjectNew: newR}) {
		t.Errorf("Generation bump (Spec change): predicate dropped it")
	}
}

// TestPrimaryWatchFiresOnAllocatorStamp pins the load-bearing
// allocator whitelist: when Status.DRBDNodeID transitions nil→1 (the
// controller-side allocator's stamp), the predicate MUST fire even
// though Generation is unchanged. Without the whitelist the
// satellite's waitForControllerAllocation gate never wakes after the
// Status PATCH — recovery-down-reverses scenario hangs (Bug 318
// lesson).
func TestPrimaryWatchFiresOnAllocatorStamp(t *testing.T) {
	t.Parallel()

	pred := primaryWatchPredicate()

	oldR := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1", Generation: 1},
		Status:     blockstoriov1alpha1.ResourceStatus{},
	}
	newR := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1", Generation: 1},
		Status: blockstoriov1alpha1.ResourceStatus{
			DRBDNodeID: int32Ptr(1),
		},
	}

	if !pred.Update(event.UpdateEvent{ObjectOld: oldR, ObjectNew: newR}) {
		t.Errorf("allocator stamped DRBDNodeID nil→1 with no Generation bump: predicate dropped it")
	}

	// Same invariant for DRBDPort + DRBDMinor — all three are part
	// of the same allocator's commit; missing any of them would let
	// a Status-only Reconcile slip through if Generation matches.
	oldR2 := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1", Generation: 1},
	}
	newR2 := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1", Generation: 1},
		Status:     blockstoriov1alpha1.ResourceStatus{DRBDPort: int32Ptr(7000)},
	}

	if !pred.Update(event.UpdateEvent{ObjectOld: oldR2, ObjectNew: newR2}) {
		t.Errorf("allocator stamped DRBDPort nil→7000 with no Generation bump: predicate dropped it")
	}

	oldR3 := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1", Generation: 1},
	}
	newR3 := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1", Generation: 1},
		Status:     blockstoriov1alpha1.ResourceStatus{DRBDMinor: int32Ptr(1000)},
	}

	if !pred.Update(event.UpdateEvent{ObjectOld: oldR3, ObjectNew: newR3}) {
		t.Errorf("allocator stamped DRBDMinor nil→1000 with no Generation bump: predicate dropped it")
	}
}

// -----------------------------------------------------------------
// Observer-trigger tests (Phase 11.7 second leg).
// -----------------------------------------------------------------

// TestObserverEmitsTriggerOnRoleChange pins the resource-kind path: a
// role transition (Secondary → Primary) emits a GenericEvent onto
// the trigger channel. This is the kernel-event the auto-diskful
// promotion path keys off — without the trigger the reconciler
// would wait on a Generation bump that never comes.
func TestObserverEmitsTriggerOnRoleChange(t *testing.T) {
	t.Parallel()

	observer, ch, adm := newTriggerObserver(t, 4)

	observer.handleObservation(context.Background(), adm, &observation{
		ResourceName: "pvc-1",
		InUse:        true,
		Role:         "Primary",
		HasResource:  true,
	})

	got := readOneTrigger(t, ch)
	if got.Object == nil || got.Object.GetName() != "pvc-1.n1" {
		t.Errorf("trigger object name: got %v, want pvc-1.n1", got.Object)
	}
}

// TestObserverEmitsTriggerOnDiskChange pins the device-kind path: a
// per-volume disk-state frame (UpToDate / Inconsistent / Diskless)
// emits a trigger. Without the wake-up the satellite's recovery
// branches on disk-state transitions never run.
func TestObserverEmitsTriggerOnDiskChange(t *testing.T) {
	t.Parallel()

	observer, ch, adm := newTriggerObserver(t, 4)

	observer.handleObservation(context.Background(), adm, &observation{
		ResourceName: "pvc-1",
		DrbdState:    "UpToDate",
		Volumes: []volumeObservation{
			{VolumeNumber: 0, DiskState: "UpToDate"},
		},
	})

	got := readOneTrigger(t, ch)
	if got.Object.GetName() != "pvc-1.n1" {
		t.Errorf("trigger object name: got %q, want pvc-1.n1", got.Object.GetName())
	}
}

// TestObserverDoesNotEmitOnPureOutOfSyncDelta pins the noise-filter
// invariant: a peer-device statistics frame that ONLY updates the
// out-of-sync byte counter (no DiskState / Role / Quorum / Connection
// transition) MUST NOT emit a trigger. Statistics frames fire at
// ~1Hz per peer; without this filter the trigger channel would
// re-add the very Reconcile noise the primary-watch predicate just
// removed.
func TestObserverDoesNotEmitOnPureOutOfSyncDelta(t *testing.T) {
	t.Parallel()

	observer, ch, adm := newTriggerObserver(t, 4)

	// Seed cache with a complete lifecycle frame so the next
	// statistics-only update has something to NO-OP against.
	observer.handleObservation(context.Background(), adm, &observation{
		ResourceName: "pvc-1",
		DrbdState:    "UpToDate",
		Volumes: []volumeObservation{
			{VolumeNumber: 0, DiskState: "UpToDate", HasSync: true, OutOfSyncKib: 0},
		},
		Connections: []connectionObservation{
			{PeerNodeName: "n2", Connected: true, Message: "Connected", ReplicationState: "Established"},
		},
	})

	// Drain the seed emit.
	readOneTrigger(t, ch)

	// Statistics-only frame: only OutOfSyncKib moves. No DiskState
	// change, no Role flip, no Connection transition.
	observer.handleObservation(context.Background(), adm, &observation{
		ResourceName: "pvc-1",
		Volumes: []volumeObservation{
			{VolumeNumber: 0, HasSync: true, OutOfSyncKib: 1024},
		},
	})

	assertNoTrigger(t, ch)
}

// TestObserverHandlesNilTriggerChannel pins the unit-test path: emit
// is a no-op when the channel is nil. Without this guard, the
// reconciler-direct-construction tests would panic on nil-channel
// send.
func TestObserverHandlesNilTriggerChannel(t *testing.T) {
	t.Parallel()

	scheme := newToggleDiskTestScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	fx := storage.NewFakeExec()

	observer := &ObserverRunnable{
		Client:   cli,
		Exec:     fx,
		NodeName: "n1",
		// ReconcileTrigger left nil.
	}

	done := make(chan struct{})

	go func() {
		observer.handleObservation(context.Background(), drbd.NewAdm(fx), &observation{
			ResourceName: "pvc-1",
			InUse:        true,
			Role:         "Primary",
			HasResource:  true,
		})

		close(done)
	}()

	select {
	case <-done:
	case <-time.After(triggerTestTimeout):
		t.Fatalf("handleObservation blocked with nil trigger channel; nil emit must be a no-op")
	}
}

// -----------------------------------------------------------------
// RequeueAfter safety-net test (Phase 11.7 third leg).
// -----------------------------------------------------------------

// TestRunApplyReturnsRequeueAfterForActiveDrbd pins the safety-net
// invariant: a successful apply on a DRBD-stacked Resource MUST
// schedule a periodic re-eval via RequeueAfter so the reconciler
// self-evaluates against current kernel state even when the events2
// stream misses a frame. Without this, the predicate's noise filter
// + the trigger channel's lifecycle-only emit could leave an active
// DRBD resource un-re-eval'd for arbitrarily long after a frame
// drop.
func TestRunApplyReturnsRequeueAfterForActiveDrbd(t *testing.T) {
	t.Parallel()

	const (
		node   = "n1"
		rdName = "pvc-active-drbd"
		pool   = "lvm-thin"
	)

	scheme := newToggleDiskTestScheme(t)

	// Build a DRBD-stacked RD with the allocator already stamped
	// (Status.DRBDNodeID / DRBDPort / DRBDMinor populated) so
	// runApply falls through the waitForControllerAllocation gate
	// and reaches the post-apply terminal path.
	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack: []string{"DRBD", "STORAGE"},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024},
			},
		},
	}

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:       rdName + "." + node,
			Finalizers: []string{SatelliteResourceFinalizer},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               node,
			ResourceDefinitionName: rdName,
			StoragePool:            pool,
		},
		Status: blockstoriov1alpha1.ResourceStatus{
			DRBDNodeID: int32Ptr(0),
			DRBDPort:   int32Ptr(7000),
			DRBDMinor:  int32Ptr(1000),
		},
	}

	storagePool := &blockstoriov1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: pool + "." + node},
		Spec: blockstoriov1alpha1.StoragePoolSpec{
			NodeName:     node,
			PoolName:     pool,
			ProviderKind: "LVM_THIN",
		},
	}

	nodeObj := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: node},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(rd, res, storagePool, nodeObj).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{pool: &noopProvider{}},
		NodeName:  node,
	})

	reconciler := &ResourceReconciler{
		Client: cli,
		Config: Config{
			NodeName:  node,
			Apply:     rec,
			Exec:      storage.NewFakeExec(),
			APIReader: cli,
		},
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: rdName + "." + node},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if result.RequeueAfter != activeDRBDRequeue {
		t.Errorf("RequeueAfter: got %v, want %v (Phase 11.7 safety net)",
			result.RequeueAfter, activeDRBDRequeue)
	}
}

// noopProvider is the smallest storage.Provider implementation that
// satisfies the satellite reconciler's contract for the
// "happy-path apply" tests: every per-volume operation is a no-op.
// Used by the active-DRBD-RequeueAfter test where we care only
// about the post-apply control flow, not the underlying storage
// provider's behaviour.
type noopProvider struct{}

func (noopProvider) Kind() string { return "LVM_THIN" }

func (noopProvider) PoolStatus(_ context.Context) (storage.PoolStatus, error) {
	return storage.PoolStatus{TotalCapacityKib: 1 << 20, FreeCapacityKib: 1 << 20}, nil
}

func (noopProvider) CreateVolume(_ context.Context, _ storage.Volume) error {
	return nil
}

func (noopProvider) DeleteVolume(_ context.Context, _ storage.Volume) error {
	return nil
}

func (noopProvider) ResizeVolume(_ context.Context, _ storage.Volume) error {
	return nil
}

func (noopProvider) VolumeStatus(_ context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	return storage.VolumeStatus{DevicePath: "/dev/noop/" + vol.ResourceName, UsableKib: vol.SizeKib}, nil
}

func (noopProvider) CreateSnapshot(_ context.Context, _ storage.Snapshot) error {
	return nil
}

func (noopProvider) DeleteSnapshot(_ context.Context, _ storage.Snapshot) error {
	return nil
}

func (noopProvider) RestoreVolumeFromSnapshot(_ context.Context, _ storage.Volume, _ storage.Snapshot) error {
	return nil
}
