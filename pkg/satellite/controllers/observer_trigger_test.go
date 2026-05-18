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
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// Bug 318 closes the satellite's observer → reconciler loop with two
// independent legs:
//
//  1. A custom primary-watch predicate (primaryWatchPredicate) that
//     drops pure observer Status noise but PASSES controller-allocator
//     Status writes (Status.DRBDNodeID/Port/Minor transitions) plus
//     every Spec change (Generation bump) and every Create/Delete/
//     Generic event.
//  2. An observer-trigger channel the ObserverRunnable emits a
//     GenericEvent onto for every kernel-state change
//     (resource/role/device/conn/peer-device frames) so the
//     ResourceReconciler wakes on observed state even when no
//     apiserver write fires a Generation bump.
//
// The two legs are load-bearing for distinct scenarios — the predicate
// whitelist for `waitForControllerAllocation` (without it, the
// satellite hangs on the Status PATCH that wakes recovery-down-
// reverses); the trigger channel for state changes that produce no
// Spec or whitelisted-Status write at all (peer flapping to
// StandAlone, the local disk transitioning Failed → Diskless without
// a follow-up reconciler stamp). This file pins both invariants with
// 13 tests (8 trigger + 5 predicate).

// observerTriggerTestTimeout caps the per-test wait on a buffered
// channel send/receive so a regression that breaks emit / the
// non-blocking select can't hang the test runner. 250 ms is
// comfortably faster than `go test`'s default 10-minute panic
// timeout and slow enough that a CI-busy box still sees the
// send-or-receive land.
const observerTriggerTestTimeout = 250 * time.Millisecond

// newTestObserverWithTrigger constructs an ObserverRunnable wired to
// a fresh in-memory fake client + a buffered trigger channel. Shared
// by the trigger tests so each one declares only the bits it
// exercises (event kind, expected outcome).
func newTestObserverWithTrigger(t *testing.T, bufferSize int) (*ObserverRunnable, chan event.GenericEvent, *drbd.Adm) {
	t.Helper()

	scheme := runtime.NewScheme()

	err := blockstoriov1alpha1.AddToScheme(scheme)
	if err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	fx := storage.NewFakeExec()
	trigger := make(chan event.GenericEvent, bufferSize)

	o := &ObserverRunnable{
		Client:           cli,
		Exec:             fx,
		NodeName:         "n1",
		ReconcileTrigger: trigger,
	}

	return o, trigger, drbd.NewAdm(fx)
}

// readOneTrigger pulls one GenericEvent off the channel within the
// test timeout. Failure is fatal — the caller asserts emit happened.
func readOneTrigger(t *testing.T, ch <-chan event.GenericEvent) event.GenericEvent {
	t.Helper()

	select {
	case ev := <-ch:
		return ev
	case <-time.After(observerTriggerTestTimeout):
		t.Fatalf("expected GenericEvent on trigger channel within %v; got none", observerTriggerTestTimeout)
	}

	return event.GenericEvent{}
}

// assertNoTrigger pins the inverse: when emit MUST not fire (nil
// channel, empty resource name, etc.) we wait briefly to confirm
// nothing lands.
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

// TestObserverTriggerFiresOnResourceRoleChange pins the resource-kind
// path: a role transition (Secondary → Primary) emits a GenericEvent
// onto the trigger channel. This is the kernel-event the auto-
// diskful promotion path keys off — without the trigger the
// reconciler would wait on a Generation bump that never comes.
func TestObserverTriggerFiresOnResourceRoleChange(t *testing.T) {
	t.Parallel()

	o, ch, adm := newTestObserverWithTrigger(t, 1)

	o.handleObservation(context.Background(), adm, &observation{
		ResourceName: "pvc-1",
		InUse:        true,
		HasResource:  true,
	})

	got := readOneTrigger(t, ch)
	if got.Object == nil || got.Object.GetName() != "pvc-1.n1" {
		t.Errorf("trigger object name: got %v, want pvc-1.n1", got.Object)
	}
}

// TestObserverTriggerFiresOnDeviceDiskStateChange pins the device-kind
// path: a per-volume disk-state frame (UpToDate, Diskless, Failed,
// …) emits a trigger. Without the wake-up the satellite's recovery
// branches on disk:Diskless never run.
func TestObserverTriggerFiresOnDeviceDiskStateChange(t *testing.T) {
	t.Parallel()

	o, ch, adm := newTestObserverWithTrigger(t, 1)

	o.handleObservation(context.Background(), adm, &observation{
		ResourceName: "pvc-1",
		DrbdState:    drbdDiskStateUpToDate,
		Volumes: []volumeObservation{
			{VolumeNumber: 0, DiskState: drbdDiskStateUpToDate},
		},
	})

	got := readOneTrigger(t, ch)
	if got.Object.GetName() != "pvc-1.n1" {
		t.Errorf("trigger object name: got %q, want pvc-1.n1", got.Object.GetName())
	}
}

// TestObserverTriggerFiresOnConnectionState pins the connection-kind
// path: a peer flapping to StandAlone / BrokenPipe emits a trigger.
// This is the load-bearing wake-up for the auto-disconnect-recovery
// branch — peer-state changes generate no apiserver Spec writes and
// the controller-allocator Status whitelist won't catch them, so
// the trigger channel is the ONLY way the reconciler hears about
// them.
func TestObserverTriggerFiresOnConnectionState(t *testing.T) {
	t.Parallel()

	o, ch, adm := newTestObserverWithTrigger(t, 1)

	o.handleObservation(context.Background(), adm, &observation{
		ResourceName: "pvc-1",
		Connections: []connectionObservation{
			{PeerNodeName: "n2", Connected: false, Message: "StandAlone"},
		},
	})

	got := readOneTrigger(t, ch)
	if got.Object.GetName() != "pvc-1.n1" {
		t.Errorf("trigger object name: got %q, want pvc-1.n1", got.Object.GetName())
	}
}

// TestObserverTriggerFiresOnPeerDeviceReplicationState pins the
// peer-device-kind path: a replication-state frame (SyncSource,
// SyncTarget, Established, PausedSyncS) emits a trigger. The
// state-inconsistent-mid-sync scenario depends on the reconciler
// seeing the sync-finishing frame land — without the wake-up the
// satellite would keep its `Inconsistent` view until the next
// 5-second observer resync, drifting the stand's tight-timing
// assertions.
func TestObserverTriggerFiresOnPeerDeviceReplicationState(t *testing.T) {
	t.Parallel()

	o, ch, adm := newTestObserverWithTrigger(t, 1)

	o.handleObservation(context.Background(), adm, &observation{
		ResourceName: "pvc-1",
		Connections: []connectionObservation{
			{PeerNodeName: "n2", ReplicationState: "Established"},
		},
	})

	got := readOneTrigger(t, ch)
	if got.Object.GetName() != "pvc-1.n1" {
		t.Errorf("trigger object name: got %q, want pvc-1.n1", got.Object.GetName())
	}
}

// TestObserverTriggerDropsOnFullChannel pins the non-blocking-send
// invariant: a saturated trigger channel must NOT back-pressure the
// events2 loop. Drops are non-fatal — the 5-second observer resync
// re-emits cached state via writeStatus and the reconciler's
// RequeueAfter covers the missed wake-up.
func TestObserverTriggerDropsOnFullChannel(t *testing.T) {
	t.Parallel()

	o, ch, adm := newTestObserverWithTrigger(t, 1)

	// Pre-fill the channel so the next emit must drop.
	ch <- event.GenericEvent{Object: &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "preload"},
	}}

	done := make(chan struct{})

	go func() {
		o.handleObservation(context.Background(), adm, &observation{
			ResourceName: "pvc-1",
			InUse:        true,
			HasResource:  true,
		})

		close(done)
	}()

	select {
	case <-done:
	case <-time.After(observerTriggerTestTimeout):
		t.Fatalf("handleObservation blocked on full trigger channel; emit must be non-blocking")
	}

	// The pre-loaded "preload" event must still be queued (we never
	// drained); the new emit was dropped.
	select {
	case ev := <-ch:
		if ev.Object.GetName() != "preload" {
			t.Errorf("channel content: got %q, want preload (emit must drop, not displace)",
				ev.Object.GetName())
		}
	default:
		t.Fatalf("expected the preload event still queued after a dropped emit")
	}
}

// TestObserverTriggerSkipsWhenChannelNil pins the unit-test path:
// emit is a no-op when the channel is nil. Without this guard, the
// reconciler-direct-construction tests would panic on nil-channel
// send.
func TestObserverTriggerSkipsWhenChannelNil(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	fx := storage.NewFakeExec()

	o := &ObserverRunnable{
		Client:   cli,
		Exec:     fx,
		NodeName: "n1",
		// ReconcileTrigger left nil.
	}

	adm := drbd.NewAdm(fx)

	// Should not panic, should not block.
	done := make(chan struct{})

	go func() {
		o.handleObservation(context.Background(), adm, &observation{
			ResourceName: "pvc-1",
			InUse:        true,
			HasResource:  true,
		})

		close(done)
	}()

	select {
	case <-done:
	case <-time.After(observerTriggerTestTimeout):
		t.Fatalf("handleObservation blocked with nil trigger channel; nil emit must be a no-op")
	}
}

// TestObserverTriggerSkipsWhenResourceNameEmpty pins the guard against
// empty-name emits. A malformed events2 frame (missing `name` field)
// is silenced upstream by translateEvent, but defensively the emit
// helper itself drops empty names so a future translateEvent
// regression doesn't poison the reconcile queue with a blank-name
// wake-up.
func TestObserverTriggerSkipsWhenResourceNameEmpty(t *testing.T) {
	t.Parallel()

	o, ch, adm := newTestObserverWithTrigger(t, 1)

	o.handleObservation(context.Background(), adm, &observation{
		ResourceName: "", // intentionally empty
		InUse:        true,
		HasResource:  true,
	})

	assertNoTrigger(t, ch)
}

// TestObserverTriggerNameEncodesNodeIdentity pins the wire format:
// the emitted GenericEvent's Object.Name is the
// `<resource>.<node>` k8s-safe name the SSA writeStatus path uses.
// The ResourceReconciler's WatchesRawSource handler enqueues that
// name directly, so a mismatch would silently route the wake-up to a
// non-existent reconcile.Request and the recovery branch would
// observe a stale cache.
func TestObserverTriggerNameEncodesNodeIdentity(t *testing.T) {
	t.Parallel()

	o, ch, adm := newTestObserverWithTrigger(t, 1)

	o.handleObservation(context.Background(), adm, &observation{
		ResourceName: "pvc-99",
		InUse:        true,
		HasResource:  true,
	})

	got := readOneTrigger(t, ch)
	if got.Object.GetName() != "pvc-99.n1" {
		t.Errorf("trigger name: got %q, want pvc-99.n1", got.Object.GetName())
	}
}

// ---------------------------------------------------------------------
// primaryWatchPredicate tests (Bug 318 first leg).
//
// These pin the load-bearing whitelist: pure observer Status updates
// must drop; controller-allocator Status writes
// (Status.DRBDNodeID/Port/Minor transitions) must pass. Without the
// whitelist the satellite's `waitForControllerAllocation` gate never
// wakes after the allocator stamp — the Status PATCH carries no
// Generation bump.
// ---------------------------------------------------------------------

// int32Ptr is a one-line helper so the tests below read more like
// the production allocator's `Status.DRBDNodeID = &id` shape.
func int32Ptr(v int32) *int32 { return &v }

// TestPrimaryWatchFiresOnAllocatorStampOfNodeID pins the load-bearing
// allocator whitelist. The satellite's
// `waitForControllerAllocation` gate hangs on this stamp.
func TestPrimaryWatchFiresOnAllocatorStampOfNodeID(t *testing.T) {
	t.Parallel()

	p := primaryWatchPredicate()

	oldR := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1", Generation: 1},
		Status:     blockstoriov1alpha1.ResourceStatus{}, // no IDs yet
	}
	newR := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1", Generation: 1},
		Status: blockstoriov1alpha1.ResourceStatus{
			DRBDNodeID: int32Ptr(1),
		},
	}

	if !p.Update(event.UpdateEvent{ObjectOld: oldR, ObjectNew: newR}) {
		t.Errorf("allocator stamped DRBDNodeID nil→1 with no Generation bump: predicate dropped it")
	}
}

// TestPrimaryWatchFiresOnAllocatorStampOfPort pins the same invariant
// for the DRBDPort allocator stamp.
func TestPrimaryWatchFiresOnAllocatorStampOfPort(t *testing.T) {
	t.Parallel()

	p := primaryWatchPredicate()

	oldR := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1", Generation: 1},
	}
	newR := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1", Generation: 1},
		Status: blockstoriov1alpha1.ResourceStatus{
			DRBDPort: int32Ptr(7000),
		},
	}

	if !p.Update(event.UpdateEvent{ObjectOld: oldR, ObjectNew: newR}) {
		t.Errorf("allocator stamped DRBDPort nil→7000 with no Generation bump: predicate dropped it")
	}
}

// TestPrimaryWatchFiresOnAllocatorStampOfMinor pins the same invariant
// for the DRBDMinor allocator stamp.
func TestPrimaryWatchFiresOnAllocatorStampOfMinor(t *testing.T) {
	t.Parallel()

	p := primaryWatchPredicate()

	oldR := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1", Generation: 1},
	}
	newR := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1", Generation: 1},
		Status: blockstoriov1alpha1.ResourceStatus{
			DRBDMinor: int32Ptr(1000),
		},
	}

	if !p.Update(event.UpdateEvent{ObjectOld: oldR, ObjectNew: newR}) {
		t.Errorf("allocator stamped DRBDMinor nil→1000 with no Generation bump: predicate dropped it")
	}
}

// TestPrimaryWatchFiltersObserverVolumesUpdate pins the noise-drop
// invariant: a pure Status.Volumes update (observer round-trip) with
// the allocator IDs unchanged MUST be filtered out. A regression here
// re-introduces Bug 316's mid-sync drbdadm-adjust storm where every
// peer-device frame kicks the reconciler.
func TestPrimaryWatchFiltersObserverVolumesUpdate(t *testing.T) {
	t.Parallel()

	p := primaryWatchPredicate()

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
				{VolumeNumber: 0, DiskState: "UpToDate"}, // changed
			},
		},
	}

	if p.Update(event.UpdateEvent{ObjectOld: oldR, ObjectNew: newR}) {
		t.Errorf("pure Status.Volumes update (allocator IDs unchanged): predicate let it through")
	}
}

// TestPrimaryWatchFiltersObserverConditionsUpdate pins the same noise-
// drop invariant for Status.InUse + Status.DrbdState transitions —
// the other observer-owned fields. A connection event lighting up
// Connections / DrbdState / InUse must drop too, because the
// trigger channel (second leg) is the authoritative wake-up path
// for these.
func TestPrimaryWatchFiltersObserverConditionsUpdate(t *testing.T) {
	t.Parallel()

	p := primaryWatchPredicate()

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
			InUse:      false,
			DrbdState:  "Inconsistent",
		},
	}
	newR := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1", Generation: 1},
		Status: blockstoriov1alpha1.ResourceStatus{
			DRBDNodeID: allocated.DRBDNodeID,
			DRBDPort:   allocated.DRBDPort,
			DRBDMinor:  allocated.DRBDMinor,
			InUse:      true,                  // changed (role)
			DrbdState:  drbdDiskStateUpToDate, // changed (disk)
			Connections: []blockstoriov1alpha1.ResourceConnectionStatus{
				{PeerNodeName: "n2", Connected: true, Message: "Connected"},
			},
		},
	}

	if p.Update(event.UpdateEvent{ObjectOld: oldR, ObjectNew: newR}) {
		t.Errorf("pure observer Status conditions update (allocator IDs unchanged): predicate let it through")
	}
}

// TestPrimaryWatchFiresOnGenerationBump pins the Spec-change passthrough:
// a Generation bump (operator re-spec) MUST fire even when the
// allocator IDs and Status are unchanged. This is the path the REST
// shim's PUT toggle-disk / resize / spec-edit operations all rely on.
func TestPrimaryWatchFiresOnGenerationBump(t *testing.T) {
	t.Parallel()

	p := primaryWatchPredicate()

	oldR := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1", Generation: 1},
		Status: blockstoriov1alpha1.ResourceStatus{
			DRBDNodeID: int32Ptr(1),
		},
	}
	newR := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1", Generation: 2}, // bump
		Status: blockstoriov1alpha1.ResourceStatus{
			DRBDNodeID: int32Ptr(1),
		},
	}

	if !p.Update(event.UpdateEvent{ObjectOld: oldR, ObjectNew: newR}) {
		t.Errorf("Generation bump (Spec change): predicate dropped it")
	}
}

// TestPrimaryWatchAlwaysFiresOnCreateDeleteGeneric pins the lifecycle
// passthrough: Create / Delete / Generic events ALWAYS fire so the
// trigger channel's GenericEvents and apiserver Create/Delete frames
// reach the reconciler unchanged.
func TestPrimaryWatchAlwaysFiresOnCreateDeleteGeneric(t *testing.T) {
	t.Parallel()

	p := primaryWatchPredicate()

	res := &blockstoriov1alpha1.Resource{ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1"}}

	if !p.Create(event.CreateEvent{Object: res}) {
		t.Errorf("Create: predicate dropped it")
	}

	if !p.Delete(event.DeleteEvent{Object: res}) {
		t.Errorf("Delete: predicate dropped it")
	}

	if !p.Generic(event.GenericEvent{Object: res}) {
		t.Errorf("Generic: predicate dropped it")
	}
}

// TestInt32PtrEqualHandlesNilBothSides pins the comparator the
// allocator-whitelist relies on: both-nil → equal; one-nil → unequal;
// same-value → equal; different-value → unequal. A regression here
// would mis-fire the allocator whitelist in either direction (false-
// positive: reconciler kicked on every observer Status apply; false-
// negative: reconciler hangs forever on the real allocator stamp).
func TestInt32PtrEqualHandlesNilBothSides(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		a    *int32
		b    *int32
		want bool
	}{
		{"both nil", nil, nil, true},
		{"left nil", nil, int32Ptr(1), false},
		{"right nil", int32Ptr(1), nil, false},
		{"equal values", int32Ptr(7), int32Ptr(7), true},
		{"different values", int32Ptr(1), int32Ptr(2), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := int32PtrEqual(tc.a, tc.b); got != tc.want {
				t.Errorf("int32PtrEqual(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}
