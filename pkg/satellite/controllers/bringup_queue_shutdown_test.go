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

	"github.com/go-logr/logr"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/controller/priorityqueue"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// TestBringUpQueueDoneReturnsAfterShutdown replays the shutdown of a
// controller whose workers are busy while events keep coming: two
// passes in progress, repeat events for one of them, and a third worker
// parked in Get. The queue shuts down, the parked worker leaves, and the
// two passes finish. Each worker's Done has to return, or the controller
// never sees its workers end and the manager does not stop.
//
// Whether the queue of controller-runtime v0.23 hangs here depends on
// where its dispatcher is at the moment of shutdown, so the sequence
// runs many times; the queue under test must never hang.
func TestBringUpQueueDoneReturnsAfterShutdown(t *testing.T) {
	for trial := range 200 {
		if !doneReturnsAfterShutdown(t) {
			t.Fatalf("trial %d: Done after shutdown did not return", trial)
		}
	}
}

func doneReturnsAfterShutdown(t *testing.T) bool {
	t.Helper()

	newQueue := newBringUpQueueFunc(newBringUpTracker(), logr.Discard())
	queue := newQueue("shutdown", workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())

	pq, ok := queue.(priorityqueue.PriorityQueue[reconcile.Request])
	if !ok {
		t.Fatal("the bring-up queue is not a priority queue")
	}

	first, second := bringUpRequest("first"), bringUpRequest("second")

	pq.Add(first)
	pq.Add(second)

	for range 2 {
		_, _, shutdown := pq.GetWithPriority()
		if shutdown {
			t.Fatal("queue shut down before the test did")
		}
	}

	// Events for a replica whose pass is still running: ready, but not
	// handed out until that pass is done, and each one wakes the queue's
	// dispatcher.
	churn := make(chan struct{})
	churned := make(chan struct{})

	go func() {
		defer close(churned)

		for {
			select {
			case <-churn:
				return
			default:
				pq.Add(first)
			}
		}
	}()

	parked := make(chan struct{})

	go func() {
		defer close(parked)

		_, _, _ = pq.GetWithPriority()
	}()

	time.Sleep(time.Millisecond)

	pq.ShutDown()
	close(churn)
	<-churned

	select {
	case <-parked:
	case <-time.After(5 * time.Second):
		t.Fatal("a worker parked in Get did not return on shutdown")
	}

	for _, item := range []reconcile.Request{first, second} {
		done := make(chan struct{})

		go func() {
			defer close(done)

			pq.Done(item)
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			return false
		}

		// Let the dispatcher act on what Done released before the next
		// worker finishes.
		time.Sleep(time.Millisecond)
	}

	return true
}
