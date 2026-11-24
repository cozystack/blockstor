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
	"k8s.io/client-go/tools/events"

	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// An apply that keeps failing is retried with a back-off that doubles
// up to applyFailureMaxRequeue, not every applyFailureRequeue for as long
// as it fails, and an apply that goes through starts the back-off over.
func TestAFailingApplyIsRetriedWithBackOff(t *testing.T) {
	t.Parallel()

	rd := groupMemberRD("STORAGE")
	res := memberReplica()
	r := newPassphraseTestReconciler(t, luksGroup(), rd, res)

	provider := &flakyCreateProvider{failsRemaining: 8}
	r.Config.Exec = storage.NewFakeExec()
	r.Config.Apply = satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"pool-a": provider},
		NodeName:  "node-a",
	})

	now := time.Unix(0, 0)
	r.retries.now = func() time.Time { return now }

	// Each run is the retry that came due.
	run := func() time.Duration {
		t.Helper()

		result, err := r.runApply(t.Context(), res, logr.Discard())
		if err != nil {
			t.Fatalf("runApply: %v", err)
		}

		now = now.Add(result.RequeueAfter)

		return result.RequeueAfter
	}

	want := []time.Duration{
		5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second,
		80 * time.Second, 160 * time.Second, 5 * time.Minute, 5 * time.Minute,
	}
	for i, w := range want {
		if got := run(); got != w {
			t.Fatalf("retry %d after %v, want %v", i+1, got, w)
		}
	}

	if got := run(); got != 0 {
		t.Fatalf("a storage-only apply that went through requeued after %v, want no requeue", got)
	}

	provider.failsRemaining = 1

	if got := run(); got != applyFailureRequeue {
		t.Fatalf("the first failure after a success retried after %v, want %v", got, applyFailureRequeue)
	}
}

// Reconciles that a watch event triggers before the retry is due, the
// peers' Status updates while a volume is created say, fail too, but
// they are not retries: the back-off counts only the retry that came
// due, as LINSTOR's RetryResourcesTask does, so a cause that clears up
// without an event is retried in seconds and not minutes.
func TestApplyBackOffCountsOnlyTheRetriesThatCameDue(t *testing.T) {
	t.Parallel()

	start := time.Unix(0, 0)
	now := start
	b := applyBackoff{now: func() time.Time { return now }}

	if got := b.next("r"); got != applyFailureRequeue {
		t.Fatalf("first failure retried after %v, want %v", got, applyFailureRequeue)
	}

	for range 6 {
		now = now.Add(500 * time.Millisecond)

		if got := b.next("r"); got > applyFailureRequeue {
			t.Fatalf("a failure before the retry was due pushed it out to %v", got)
		}
	}

	now = start.Add(applyFailureRequeue)

	if got := b.next("r"); got != 2*applyFailureRequeue {
		t.Fatalf("the first retry that came due and failed retried after %v, want %v", got, 2*applyFailureRequeue)
	}
}

// An apply that goes through after failures is reported as a Normal
// Event on the Resource, once: the Warning Events of the failures stay
// until they expire and would otherwise read as a replica still down,
// as LINSTOR logs a failed resource leaving its retry task.
func TestAnApplyThatRecoversIsReportedOnce(t *testing.T) {
	t.Parallel()

	rd := groupMemberRD("STORAGE")
	res := memberReplica()
	r := newPassphraseTestReconciler(t, luksGroup(), rd, res)

	recorder := events.NewFakeRecorder(16)
	r.Config.Recorder = recorder

	provider := &flakyCreateProvider{failsRemaining: 2}
	r.Config.Exec = storage.NewFakeExec()
	r.Config.Apply = satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"pool-a": provider},
		NodeName:  "node-a",
	})

	drain := func() []string {
		var got []string

		for {
			select {
			case ev := <-recorder.Events:
				got = append(got, ev)
			default:
				return got
			}
		}
	}

	for range 2 {
		if _, err := r.runApply(t.Context(), res, logr.Discard()); err != nil {
			t.Fatalf("runApply: %v", err)
		}
	}

	failed := drain()
	if len(failed) != 2 {
		t.Fatalf("events after two failed applies = %q, want two %s Warnings", failed, applyFailedReason)
	}

	for _, ev := range failed {
		if !strings.HasPrefix(ev, "Warning "+applyFailedReason) {
			t.Fatalf("events after two failed applies = %q, want two %s Warnings", failed, applyFailedReason)
		}
	}

	if _, err := r.runApply(t.Context(), res, logr.Discard()); err != nil {
		t.Fatalf("runApply: %v", err)
	}

	evs := drain()
	if len(evs) != 1 || !strings.HasPrefix(evs[0], "Normal "+applySucceededReason) || !strings.Contains(evs[0], "after 2 failed attempts") {
		t.Fatalf("events after the apply went through = %q, want one Normal %s after 2 failed attempts", evs, applySucceededReason)
	}

	if _, err := r.runApply(t.Context(), res, logr.Discard()); err != nil {
		t.Fatalf("runApply: %v", err)
	}

	if evs := drain(); len(evs) != 0 {
		t.Fatalf("an apply with no failure before it reported %q", evs)
	}
}

// A grow the apply left unfinished is retried on the short timer even
// when the same apply failed often enough to be backed off: the resize
// waiting on the device must not wait out the back-off of an unrelated
// failure.
func TestAnUnfinishedGrowIsNotBackedOff(t *testing.T) {
	t.Parallel()

	now := time.Unix(0, 0)

	var r ResourceReconciler

	r.retries.now = func() time.Time { return now }

	failed := []*intent.ResourceApplyResult{{Name: "pvc-g", Ok: false, Message: "adjust failed"}}
	for range 5 {
		now = now.Add(r.retryAfterApply("pvc-g", failed, true, false))
	}

	growing := []*intent.ResourceApplyResult{{Name: "pvc-g", Ok: false, Message: "adjust failed", GrowPending: true}}
	if got := r.retryAfterApply("pvc-g", growing, true, false); got != applyFailureRequeue {
		t.Errorf("a failed apply with a grow pending retried after %v, want %v", got, applyFailureRequeue)
	}

	grew := []*intent.ResourceApplyResult{{Name: "pvc-h", Ok: true, GrowPending: true}}
	if got := r.retryAfterApply("pvc-h", grew, false, false); got != applyFailureRequeue {
		t.Errorf("an apply with a grow pending retried after %v, want %v", got, applyFailureRequeue)
	}
}
