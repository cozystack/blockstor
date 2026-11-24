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
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// endTakeWithStaleOrders records an abort the way the controller does,
// while a pass that never saw it left both orders standing.
func (rig *windowRig) endTakeWithStaleOrders() {
	rig.t.Helper()

	var snap blockstoriov1alpha1.Snapshot

	err := rig.cli.Get(context.Background(), types.NamespacedName{Name: windowTestSnapshot}, &snap)
	if err != nil {
		rig.t.Fatalf("get snapshot: %v", err)
	}

	snap.Spec.SuspendIO = true
	snap.Spec.TakeSnapshot = true
	snap.Spec.AbortReason = "snapshot aborted: deadline"

	err = rig.cli.Update(context.Background(), &snap)
	if err != nil {
		rig.t.Fatalf("update snapshot spec: %v", err)
	}
}

// A node that already holds a window for a take the controller ended
// resumes it on the next pass instead of taking its copy: the suspend
// order standing next to the abort record is stale.
func TestSnapshotWindowOfAnEndedTakeIsResumedNotTaken(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	rig.r.timing.windowDeadline = time.Minute

	rig.reconcile()

	if !rig.kernel.isSuspended() || !rig.node().SuspendIOAcked {
		t.Fatalf("first pass did not open the window: %v", rig.kernel.commands())
	}

	rig.endTakeWithStaleOrders()
	rig.reconcile()

	eventually(t, "resume of the ended take's window", func() bool {
		return !rig.kernel.isSuspended() && rig.r.windowsFor().get(windowTestSnapshot) == nil
	})

	if n := rig.provider.takeCount(); n != 0 {
		t.Errorf("took %d copies for a take that ended", n)
	}
}

// The watchdog reads the snapshot on its own, and a stale suspend order
// next to an abort record must not make it start the take either; it
// closes the window like any other ended take.
func TestSnapshotWatchdogClosesTheWindowOfAnEndedTake(t *testing.T) {
	t.Parallel()

	rig := newWindowRig(t)
	rig.r.timing.windowDeadline = time.Minute

	rig.reconcile()

	if !rig.kernel.isSuspended() {
		t.Fatalf("first pass did not open the window: %v", rig.kernel.commands())
	}

	rig.endTakeWithStaleOrders()
	rig.r.watchWindow(context.Background(), windowTestSnapshot)

	eventually(t, "the watchdog closing the ended take's window", func() bool {
		return !rig.kernel.isSuspended() && rig.r.windowsFor().get(windowTestSnapshot) == nil
	})

	if n := rig.provider.takeCount(); n != 0 {
		t.Errorf("the watchdog took %d copies for a take that ended", n)
	}
}
