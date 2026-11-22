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
	"testing"
	"time"
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
