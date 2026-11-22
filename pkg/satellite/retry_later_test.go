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

package satellite

import (
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// The satellite runs no periodic pass of a converged replica any more.
// Work a pass puts off for a while, a dwell window or a throttle, used
// to come back with the next periodic pass; now the pass has to ask for
// it, or it never comes when nothing else happens to the replica.

// wantRetry fails unless the passes of name asked for another within
// (0, most].
func wantRetry(t *testing.T, r *Reconciler, name string, most time.Duration) {
	t.Helper()

	after, ok := r.TakeRetry(name)
	if !ok {
		t.Fatalf("no retry asked for %s", name)
	}

	if after <= 0 || after > most {
		t.Errorf("retry of %s after %v, want within (0, %v]", name, after, most)
	}

	if _, again := r.TakeRetry(name); again {
		t.Errorf("TakeRetry did not forget the retry of %s", name)
	}
}

func TestStuckSlotGraceAsksForARetry(t *testing.T) {
	r, _ := newKernelCleanupReconciler(t)

	err := r.PruneStaleKernelSlots(t.Context(), "pvc-1",
		[]string{"worker-1", "worker-2"},
		[]int32{0},
		map[int32]string{0: "/dev/zvol/pool/pvc-1"})
	if err != nil {
		t.Fatalf("PruneStaleKernelSlots: %v", err)
	}

	wantRetry(t, r, "pvc-1", defaultStuckSlotGrace)
}

func TestLateAddStallDwellAsksForARetry(t *testing.T) {
	r := NewReconciler(ReconcilerConfig{})

	if r.lateAddStallDwellElapsed("pvc-1") {
		t.Fatal("the first sighting of a stall ended its dwell")
	}

	wantRetry(t, r, "pvc-1", lateAddStallDwell)

	if r.lateAddStallDwellElapsed("pvc-1") {
		t.Fatal("the dwell ended at once")
	}

	wantRetry(t, r, "pvc-1", lateAddStallDwell)
}

func TestThrottledRecoveryPromoteAsksForARetry(t *testing.T) {
	r := NewReconciler(ReconcilerConfig{})

	if !r.recoveryPromoteDue("pvc-1") {
		t.Fatal("the first recovery-promote was throttled")
	}

	if _, ok := r.TakeRetry("pvc-1"); ok {
		t.Error("a recovery-promote that fired asked for a retry")
	}

	if r.recoveryPromoteDue("pvc-1") {
		t.Fatal("a second recovery-promote inside the throttle went through")
	}

	wantRetry(t, r, "pvc-1", recoveryPromoteThrottle)
}

func TestEarliestRetryWins(t *testing.T) {
	r := NewReconciler(ReconcilerConfig{})

	r.retryLater("pvc-1", time.Minute)
	r.retryLater("pvc-1", time.Second)
	r.retryLater("pvc-1", time.Hour)

	wantRetry(t, r, "pvc-1", time.Second)

	if _, ok := r.TakeRetry("pvc-2"); ok {
		t.Error("a retry for a resource nothing asked for")
	}
}

// TestFailedDeviceNodeAsksForARetry: on a node without udev the
// satellite creates /dev/drbd<minor> itself after an adjust. A failed
// mknod used to wait for the periodic re-check; now the pass asks for
// another, and one that created the node asks for none.
func TestFailedDeviceNodeAsksForARetry(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mknodErr  error
		wantRetry bool
	}{
		{name: "mknod failed", mknodErr: errors.New("mknod /dev/drbd1000: operation not permitted"), wantRetry: true},
		{name: "node in place", wantRetry: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(storage.NewFakeExec()), NodeName: "n1"})

			var minors []int

			rec.ensureDeviceNode = func(minor int) error {
				minors = append(minors, minor)

				return tc.mknodErr
			}

			dr := &intent.DesiredResource{
				Name:        "pvc-1",
				NodeName:    "n1",
				Volumes:     []*intent.DesiredVolume{{VolumeNumber: 0, SizeKib: 1024}},
				DrbdOptions: map[string]string{"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000"},
			}

			err := rec.adjustResource(t.Context(), dr, false)
			if err != nil {
				t.Fatalf("adjustResource: %v", err)
			}

			if !slices.Equal(minors, []int{1000}) {
				t.Fatalf("device nodes ensured for minors %v, want [1000]", minors)
			}

			if tc.wantRetry {
				wantRetry(t, rec, "pvc-1", bestEffortRetry)

				return
			}

			if after, ok := rec.TakeRetry("pvc-1"); ok {
				t.Errorf("a retry after %v with the device node in place", after)
			}
		})
	}
}

// TestFailedUnexpectedPeerTeardownAsksForARetry: the prune's del-peer of
// a kernel peer K8s no longer expects can fail, and the Resource
// controller logs that and goes on with the apply. Without an ask the
// live connection to that peer stays up until another event reaches the
// replica.
func TestFailedUnexpectedPeerTeardownAsksForARetry(t *testing.T) {
	r, fx := newKernelCleanupReconciler(t)
	fx.Expect("drbdadm del-peer worker-2:pvc-1", storage.FakeResponse{Err: storage.FakeExitError{Code: 10}})

	err := r.PruneStaleKernelSlots(t.Context(), "pvc-1", []string{"worker-1"}, []int32{0}, nil)
	if err == nil {
		t.Fatal("PruneStaleKernelSlots hid the failed del-peer")
	}

	wantRetry(t, r, "pvc-1", bestEffortRetry)
}
