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
	tracker.broughtUp("up")

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

// The pass that brings a replica up was dequeued at bringUpPriority, and
// controller-runtime queues its periodic re-check at that priority. The
// re-check must not keep it.
func TestBringUpQueueDropsTheRankOnceUp(t *testing.T) {
	tracker := newBringUpTracker()
	inner := &recordingQueue{adds: map[string]priorityqueue.AddOpts{}}
	queue := &bringUpQueue{PriorityQueue: inner, tracker: tracker}

	tracker.broughtUp("r1")
	queue.AddWithOpts(priorityqueue.AddOpts{After: activeDRBDRequeue, Priority: priorityPtr(bringUpPriority)}, bringUpRequest("r1"))

	if got := inner.adds["r1"]; priorityOf(got) != handler.LowPriority || got.After != activeDRBDRequeue {
		t.Errorf("re-check after the bring-up: %+v, want priority %d after %v", got, handler.LowPriority, activeDRBDRequeue)
	}

	tracker.forget("r1")
	queue.Add(bringUpRequest("r1"))

	if got := priorityOf(inner.adds["r1"]); got != bringUpPriority {
		t.Errorf("replica created again under a deleted one's name: priority %d, want %d", got, bringUpPriority)
	}
}
