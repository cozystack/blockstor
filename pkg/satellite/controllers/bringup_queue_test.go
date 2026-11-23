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
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/priorityqueue"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// recordingQueue keeps the options of every add it is handed.
type recordingQueue struct {
	priorityqueue.PriorityQueue[reconcile.Request]

	adds map[string]priorityqueue.AddOpts
}

func (q *recordingQueue) AddWithOpts(opts priorityqueue.AddOpts, items ...reconcile.Request) {
	for _, item := range items {
		q.adds[item.Name] = opts
	}
}

func bringUpRequest(name string) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Name: name}}
}

func TestBringUpQueueRanksPendingReplicasFirst(t *testing.T) {
	tracker := newBringUpTracker()
	tracker.started("up")

	inner := &recordingQueue{adds: map[string]priorityqueue.AddOpts{}}
	queue := &bringUpQueue{PriorityQueue: inner, tracker: tracker}

	queue.AddWithOpts(priorityqueue.AddOpts{Priority: priorityPtr(handler.LowPriority)}, bringUpRequest("waiting"), bringUpRequest("up"))

	if got := priorityOf(inner.adds["waiting"]); got != bringUpPriority {
		t.Errorf("replica from the initial list not brought up yet: priority %d, want %d", got, bringUpPriority)
	}

	if got := priorityOf(inner.adds["up"]); got != handler.LowPriority {
		t.Errorf("replica already up: priority %d, want the %d it was queued with", got, handler.LowPriority)
	}

	queue.Add(bringUpRequest("up"))

	if got := priorityOf(inner.adds["up"]); got != 0 {
		t.Errorf("event for a replica already up: priority %d, want normal", got)
	}

	queue.AddAfter(bringUpRequest("new"), time.Second)

	if got := inner.adds["new"]; priorityOf(got) != bringUpPriority || got.After != time.Second {
		t.Errorf("retry of a replica not up yet: %+v, want priority %d after 1s", got, bringUpPriority)
	}

	queue.AddRateLimited(bringUpRequest("new"))

	if got := inner.adds["new"]; priorityOf(got) != bringUpPriority || !got.RateLimited {
		t.Errorf("rate-limited retry of a replica not up yet: %+v, want priority %d", got, bringUpPriority)
	}
}

// A first pass was dequeued at bringUpPriority, and controller-runtime
// queues its requeue at that priority. The requeue must not keep the
// rank: it goes back to the priority the replica would have run at
// without it, low for a replica of the initial list, normal for one an
// event queued.
func TestBringUpQueueDropsTheRankAfterTheFirstPass(t *testing.T) {
	tracker := newBringUpTracker()
	inner := &recordingQueue{adds: map[string]priorityqueue.AddOpts{}}
	queue := &bringUpQueue{PriorityQueue: inner, tracker: tracker}

	queue.AddWithOpts(priorityqueue.AddOpts{Priority: priorityPtr(handler.LowPriority)}, bringUpRequest("listed"))
	queue.Add(bringUpRequest("created"))

	for _, name := range []string{"listed", "created"} {
		if got := priorityOf(inner.adds[name]); got != bringUpPriority {
			t.Fatalf("first pass of %s: priority %d, want %d", name, got, bringUpPriority)
		}

		tracker.started(name)
		queue.AddWithOpts(priorityqueue.AddOpts{After: applyFailureRequeue, Priority: priorityPtr(bringUpPriority)}, bringUpRequest(name))
	}

	if got := inner.adds["listed"]; priorityOf(got) != handler.LowPriority || got.After != applyFailureRequeue {
		t.Errorf("requeue of a replica from the initial list: %+v, want priority %d after %v", got, handler.LowPriority, applyFailureRequeue)
	}

	if got := inner.adds["created"]; priorityOf(got) != 0 || got.After != applyFailureRequeue {
		t.Errorf("requeue of a replica created while the satellite runs: %+v, want normal priority after %v", got, applyFailureRequeue)
	}

	// A failed first pass is retried like any other pass.
	queue.AddWithOpts(priorityqueue.AddOpts{RateLimited: true, Priority: priorityPtr(bringUpPriority)}, bringUpRequest("created"))

	if got := inner.adds["created"]; priorityOf(got) != 0 || !got.RateLimited {
		t.Errorf("retry of a failed first pass: %+v, want normal priority", got)
	}

	tracker.forget("listed")
	queue.Add(bringUpRequest("listed"))

	if got := priorityOf(inner.adds["listed"]); got != bringUpPriority {
		t.Errorf("replica created again under a deleted one's name: priority %d, want %d", got, bringUpPriority)
	}
}

// The rank a requeue carries back says nothing about the priority the
// replica would have had; only what its adds asked for counts.
func TestBringUpTrackerIgnoresTheRankItself(t *testing.T) {
	tracker := newBringUpTracker()

	tracker.raised("r1", bringUpPriority)

	if got := tracker.unranked("r1"); got != handler.LowPriority {
		t.Errorf("only the rank was asked for: unranked %d, want %d", got, handler.LowPriority)
	}

	tracker.raised("r1", handler.LowPriority)
	tracker.raised("r1", 0)
	tracker.raised("r1", handler.LowPriority)

	if got := tracker.unranked("r1"); got != 0 {
		t.Errorf("low then normal asked for: unranked %d, want the highest, 0", got)
	}
}
