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
	tracker.start("up")

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

// A first pass was dequeued at bringUpPriority. Its requeue must not keep
// the rank: Reconcile names normal priority on every result, and an add
// that still carries the rank, a pass that ended in a recovered panic,
// goes to normal priority too, whether the replica came from the initial
// list or from an event.
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

		tracker.start(name)
	}

	queue.AddWithOpts(priorityqueue.AddOpts{After: applyFailureRequeue, Priority: normalPriority()}, bringUpRequest("listed"))

	if got := inner.adds["listed"]; priorityOf(got) != 0 || got.After != applyFailureRequeue {
		t.Errorf("requeue Reconcile asked for: %+v, want normal priority after %v", got, applyFailureRequeue)
	}

	queue.AddWithOpts(priorityqueue.AddOpts{RateLimited: true, Priority: priorityPtr(bringUpPriority)}, bringUpRequest("listed"), bringUpRequest("created"))

	for _, name := range []string{"listed", "created"} {
		if got := inner.adds[name]; priorityOf(got) != 0 || !got.RateLimited {
			t.Errorf("retry of %s carrying the rank back: %+v, want normal priority", name, got)
		}
	}

	tracker.forget("listed")
	queue.Add(bringUpRequest("listed"))

	if got := priorityOf(inner.adds["listed"]); got != bringUpPriority {
		t.Errorf("replica created again under a deleted one's name: priority %d, want %d", got, bringUpPriority)
	}
}
