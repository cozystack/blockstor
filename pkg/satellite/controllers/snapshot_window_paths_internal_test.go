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
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

var errAPIDown = errors.New("apiserver unreachable")

const suspendCmd = "drbdadm suspend-io " + windowTestResource

// statusWritesFail makes every status write of the reconciler fail
// while down is set, as an API partition does.
func (rig *windowRig) statusWritesFail(down *atomic.Bool) {
	rig.r.Client = interceptor.NewClient(rig.cli.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object,
			opts ...client.SubResourceUpdateOption,
		) error {
			if down.Load() {
				return errAPIDown
			}

			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
}

func (rig *windowRig) reconcileErr() (ctrl.Result, error) {
	return rig.r.Reconcile(rig.t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: windowTestSnapshot}})
}

// A delete waits for a take that is still running even after the
// watchdog closed its window, which it does within a tick of the
// deletion. A take let run past the delete creates the storage snapshot
// after it, and nothing removes that one once the finalizer is gone.
func TestSnapshotDeleteWaitsForATakeWhoseWindowClosed(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	rig.r.timing.windowDeadline = time.Minute
	rig.provider.hang = make(chan struct{})

	rig.reconcile()
	rig.controllerSets(true, true)
	rig.reconcile()
	eventually(t, "take running", func() bool { return rig.provider.takeCount() == 1 })

	snap := rig.snapshot()

	err := rig.cli.Delete(context.Background(), &snap)
	if err != nil {
		t.Fatal(err)
	}

	// The watchdog sees the deletion first and closes the window.
	err = rig.r.closeWindow(t.Context(), windowTestSnapshot, false, "")
	if err != nil || rig.r.windowsFor().get(windowTestSnapshot) != nil {
		t.Fatalf("watchdog close: err=%v window=%v", err, rig.r.windowsFor().get(windowTestSnapshot))
	}

	res := rig.reconcile2()

	if n := rig.provider.deleteCount(); n != 0 {
		t.Errorf("DeleteSnapshot ran %d time(s) while the take was still running", n)
	}

	if res.RequeueAfter == 0 {
		t.Errorf("delete is not retried after the take: %+v", res)
	}

	if !slices.Contains(rig.snapshot().Finalizers, snapshotNodeFinalizer("n1")) {
		t.Errorf("finalizer removed while the take was still running")
	}

	close(rig.provider.hang)
	eventually(t, "take returned", func() bool { return !rig.r.takeInFlight(windowTestSnapshot) })

	rig.reconcile()

	if n := rig.provider.deleteCount(); n != 1 {
		t.Errorf("DeleteSnapshot ran %d time(s) after the take returned, want 1", n)
	}
}

func (rig *windowRig) reconcile2() ctrl.Result {
	rig.t.Helper()

	return rig.reconcileName(windowTestSnapshot)
}

// A local resource the kernel reports suspended-user, with no window or
// marker on this node to account for it, is resumed, as LINSTOR's
// satellite does on every run. That is the state the satellite before
// this one left: it wrote no markers and never resumed on delete. A
// resource this node holds no replica of is not touched.
func TestSnapshotWatchdogResumesASuspendNothingAccountsFor(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	rig.create(&blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: windowTestResource + ".n1"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: windowTestResource, NodeName: "n1"},
	})
	rig.kernel.addResource("foreign", 1500)
	rig.kernel.suspendResource(windowTestResource)
	rig.kernel.suspendResource("foreign")

	rig.startWatchdog()

	eventually(t, "unaccounted suspend resumed", func() bool { return !rig.kernel.isSuspended() })

	time.Sleep(100 * time.Millisecond)

	if !rig.kernel.resourceSuspended("foreign") {
		t.Errorf("resumed a resource this node holds no replica of")
	}
}

// The sweep leaves alone a suspend an open window accounts for.
func TestSnapshotWatchdogSweepLeavesAnOpenWindowAlone(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	rig.r.timing.windowDeadline = time.Minute
	rig.create(&blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: windowTestResource + ".n1"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: windowTestResource, NodeName: "n1"},
	})
	rig.startWatchdog()

	rig.reconcile()
	time.Sleep(150 * time.Millisecond)

	if !rig.kernel.isSuspended() {
		t.Errorf("the sweep resumed a resource an open window holds")
	}
}

// A window this node closed at its deadline is not opened again for the
// same snapshot. With the API cut off, the deadline's Failed stamp does
// not land, so the Snapshot still asks for the suspend; suspending again
// would freeze I/O for another window, and again after it, for as long
// as the API stays away.
func TestSnapshotWindowIsNotReopenedAfterTheDeadlineClosedIt(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	rig.r.timing.windowDeadline = 200 * time.Millisecond

	var down atomic.Bool

	down.Store(true)
	rig.statusWritesFail(&down)

	_, err := rig.reconcileErr()
	if err == nil || !rig.kernel.isSuspended() {
		t.Fatalf("suspend with the ack stamp failing: err=%v suspended=%t", err, rig.kernel.isSuspended())
	}

	rig.startWatchdog()
	eventually(t, "resume at the deadline", func() bool {
		return !rig.kernel.isSuspended() && rig.r.windowsFor().get(windowTestSnapshot) == nil
	})

	for range 3 {
		_, _ = rig.reconcileErr()
	}

	if n := rig.kernel.count(suspendCmd); n != 1 {
		t.Errorf("suspended %d times for one snapshot: %v", n, rig.kernel.commands())
	}

	if rig.kernel.isSuspended() {
		t.Errorf("suspended again after the deadline")
	}

	down.Store(false)
	eventually(t, "Failed stamped once the API is back", func() bool {
		return rig.node().Failed && !rig.markerExists()
	})
}

// A suspend-io killed at its timeout can still land in the kernel after
// the resume that followed it read the device as running. The marker
// stays long enough for the sweep to resume that late suspend.
func TestSnapshotWindowResumesASuspendThatLandsAfterItFailed(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	rig.kernel.set(func() { rig.kernel.suspendLandsLate = 100 * time.Millisecond })

	rig.reconcile()

	if !rig.node().Failed {
		t.Fatalf("a failed suspend did not fail the snapshot")
	}

	rig.startWatchdog()

	eventually(t, "the late suspend lands and is resumed", func() bool {
		var landed bool

		rig.kernel.set(func() { landed = rig.kernel.lateSuspendLanded })

		return landed && !rig.kernel.isSuspended() && !rig.markerExists()
	})
}

// Two snapshots of one resource share its one suspended-user flag. The
// second waits while the first holds a window, and while a marker an
// earlier process left on the resource is not resumed yet; and the
// resume of a leftover never ends another snapshot's live window.
func TestSnapshotWindowsOfOneResourceDoNotEndEachOther(t *testing.T) {
	t.Parallel()

	const other = windowTestResource + ".snap-0"

	rig := newWindowRig(t)
	rig.r.timing.windowDeadline = time.Minute
	rig.create(windowSnapshot(other, windowTestResource))

	rig.reconcile()

	if res := rig.reconcileName(other); res.RequeueAfter == 0 {
		t.Errorf("second window on a busy resource not deferred: %+v", res)
	}

	if n := rig.kernel.count(suspendCmd); n != 1 || rig.r.windowsFor().get(other) != nil {
		t.Errorf("second window opened on a busy resource: suspends=%d", n)
	}

	// A leftover for the other snapshot, as a cache still showing its
	// ack produces, is settled without ending the live window.
	err := rig.r.resumeLeftover(t.Context(), &suspendMarker{Resource: windowTestResource, Snapshot: "pvc-1.snap-stale"})
	if err != nil || !rig.kernel.isSuspended() {
		t.Errorf("leftover resume ended a live window: err=%v suspended=%t", err, rig.kernel.isSuspended())
	}

	rig.controllerSets(false, false)
	rig.reconcile()

	// A marker left by an earlier process holds the resource as well.
	err = rig.r.writeMarker(&suspendMarker{Resource: windowTestResource, Snapshot: "pvc-1.snap-old"})
	if err != nil {
		t.Fatal(err)
	}

	if res := rig.reconcileName(other); res.RequeueAfter == 0 || rig.kernel.isSuspended() {
		t.Errorf("window opened over a leftover marker: %+v suspended=%t", res, rig.kernel.isSuspended())
	}

	rig.r.removeMarker("pvc-1.snap-old")
	rig.reconcileName(other)

	if !rig.kernel.isSuspended() || rig.r.windowsFor().get(other) == nil {
		t.Errorf("second window did not open once the resource was free")
	}
}

// A STORAGE-only resource on a satellite that runs DRBD for others has
// nothing to suspend: the take runs without a window, after a flush of
// the storage device, instead of a suspend-io that fails the snapshot.
func TestSnapshotOfAResourceWithoutDRBDOnADRBDSatellite(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	rig.create(&blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: windowTestResource},
		Spec:       blockstoriov1alpha1.ResourceDefinitionSpec{LayerStack: []string{"STORAGE"}},
	})

	rig.reconcile()

	if !rig.node().SuspendIOAcked || rig.node().Failed {
		t.Fatalf("suspend phase: %+v", rig.node())
	}

	rig.controllerSets(true, true)
	rig.reconcile()

	if !rig.node().Ready || rig.provider.takeCount() != 1 {
		t.Errorf("take: node=%+v takes=%d", rig.node(), rig.provider.takeCount())
	}

	cmds := rig.kernel.commands()
	if slices.Contains(cmds, suspendCmd) {
		t.Errorf("suspend-io on a resource without DRBD: %v", cmds)
	}

	if !slices.Contains(cmds, "blockdev --flushbufs /dev/zvol/pool/"+windowTestResource+"_00000") {
		t.Errorf("storage device not flushed before the take: %v", cmds)
	}
}

// A resume whose kernel answer the close could not record keeps the
// marker, so the ack is cleared once the API answers again, instead of
// the IOSuspended condition staying until some unrelated event.
func TestSnapshotWindowClearsTheAckAfterAFailedReadAtClose(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)

	var down atomic.Bool

	rig.r.Client = interceptor.NewClient(rig.cli.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object,
			opts ...client.GetOption,
		) error {
			if _, ok := obj.(*blockstoriov1alpha1.Snapshot); ok && down.Load() {
				return errAPIDown
			}

			return c.Get(ctx, key, obj, opts...)
		},
	})

	rig.reconcile()
	rig.controllerSets(false, false)
	down.Store(true)

	err := rig.r.closeWindow(t.Context(), windowTestSnapshot, false, "")
	if err != nil || rig.kernel.isSuspended() {
		t.Fatalf("close: err=%v suspended=%t", err, rig.kernel.isSuspended())
	}

	down.Store(false)
	rig.startWatchdog()

	eventually(t, "ack cleared after the read recovered", func() bool {
		return !rig.node().SuspendIOAcked && !rig.markerExists()
	})
}

// Each window closes on its own: a resume stuck on one resource does
// not hold up the deadline of a window on another.
func TestSnapshotWatchdogClosesEachWindowOnItsOwn(t *testing.T) {
	t.Parallel()

	const (
		other         = "pvc-2"
		otherSnapshot = other + ".snap-1"
	)

	rig := newWindowRig(t)
	rig.r.timing.commandTimeout = 3 * time.Second
	rig.r.timing.windowDeadline = 400 * time.Millisecond
	rig.addResource(other, 1001, otherSnapshot)

	rig.reconcile()
	rig.reconcileName(otherSnapshot)

	if !rig.kernel.isSuspended() || !rig.kernel.resourceSuspended(other) {
		t.Fatal("not both suspended")
	}

	rig.kernel.set(func() { rig.kernel.resumeHang[windowTestResource] = true })
	rig.controllerSets(false, false)
	rig.startWatchdog()

	start := time.Now()

	eventually(t, "other window resumed at its deadline", func() bool { return !rig.kernel.resourceSuspended(other) })

	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("the other window's resume waited %v behind a stuck one", took)
	}

	rig.kernel.set(func() { rig.kernel.resumeHang[windowTestResource] = false })
}

// A resume that keeps failing is retried every tick, but reaches the
// Snapshot's events once a minute, not once a tick.
func TestSnapshotWindowRateLimitsResumeFailedEvents(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	rig.reconcile()
	rig.kernel.set(func() { rig.kernel.resumeFailures = 1 << 20 })
	rig.controllerSets(false, false)

	stop := rig.startWatchdog()

	time.Sleep(300 * time.Millisecond)
	rig.kernel.set(func() { rig.kernel.resumeFailures = 0 })
	stop()

	events := 0

	for len(rig.recorder.Events) > 0 {
		if ev := <-rig.recorder.Events; strings.Contains(ev, "ResumeIOFailed") {
			events++
		}
	}

	if events != 1 {
		t.Errorf("%d ResumeIOFailed events in 300ms of retries, want 1", events)
	}
}

// Once the watchdog ran its shutdown resume, nothing is suspended any
// more: a Reconcile still in flight would leave a suspend for a next
// satellite that, on an uninstall, never starts.
func TestSnapshotWindowDoesNotOpenAfterTheShutdownResume(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	stop := rig.startWatchdog()
	stop()

	_, err := rig.reconcileErr()
	if !errors.Is(err, errSatelliteStopping) {
		t.Errorf("Reconcile after shutdown: err=%v", err)
	}

	if rig.kernel.isSuspended() || rig.kernel.count(suspendCmd) != 0 {
		t.Errorf("suspended after the shutdown resume: %v", rig.kernel.commands())
	}
}

// A take that stamps Ready right as the deadline passes and the
// deadline's Failed stamp do not both land on this node.
func TestSnapshotTakeAndDeadlineDoNotBothStamp(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	rig.r.timing.windowDeadline = time.Minute

	stamping := make(chan struct{})
	release := make(chan struct{})

	var once sync.Once

	rig.r.Client = interceptor.NewClient(rig.cli.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object,
			opts ...client.SubResourceUpdateOption,
		) error {
			if snap, ok := obj.(*blockstoriov1alpha1.Snapshot); ok && perNodeStatusReady(snap.Status.NodeStatus, "n1") {
				once.Do(func() {
					close(stamping)
					<-release
				})
			}

			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	})

	rig.reconcile()
	rig.controllerSets(true, true)
	rig.reconcile()

	<-stamping

	closed := make(chan error)

	go func() { closed <- rig.r.closeWindow(context.Background(), windowTestSnapshot, true, "deadline") }()

	time.Sleep(50 * time.Millisecond)
	close(release)

	if err := <-closed; err != nil {
		t.Fatal(err)
	}

	if node := rig.node(); node.Ready && node.Failed {
		t.Errorf("this node is both Ready and Failed: %+v", node)
	}
}

// The resume by minor, which a satellite whose .res is gone falls back
// to, covers every volume of the resource.
func TestSnapshotWindowResumesEveryVolumeByMinor(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	rig.kernel.addResource("pvc-multi", 1100, 1101)
	rig.kernel.suspendResource("pvc-multi")
	rig.kernel.set(func() { rig.kernel.resFileGone = true })

	err := rig.r.resumeAndVerify(t.Context(), &suspendMarker{Resource: "pvc-multi"})
	if err != nil {
		t.Fatal(err)
	}

	for _, minor := range []int{1100, 1101} {
		if !slices.Contains(rig.kernel.commands(), fmt.Sprintf("drbdsetup resume-io %d", minor)) {
			t.Errorf("minor %d not resumed: %v", minor, rig.kernel.commands())
		}
	}
}
