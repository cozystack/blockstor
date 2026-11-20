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
	"sync"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/controller/priorityqueue"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// bringUpPriority is the queue priority of a local Resource whose first
// pass has not started yet. It ranks above the normal priority of an
// event, so a replica waiting for its first pass goes before the repeat
// passes of replicas that already had one.
const bringUpPriority = 100

// bringUpTracker records which local Resources had their first pass in
// this process. A Resource it has not seen yet is pending, which covers
// both the replicas of a node that just started and a replica created
// while the satellite runs.
//
// A replica leaves the rank when its first pass starts, whatever that
// pass ends with. One that keeps failing, its storage pool not imported
// after a reboot say, is then retried like any other pass instead of
// holding every worker for as long as it fails. LINSTOR's device manager
// does the same: it dispatches every resource of the first full sync
// once and retries a failure as an ordinary update.
type bringUpTracker struct {
	mu      sync.Mutex
	entries map[string]bringUpEntry
}

// bringUpEntry is what the tracker knows about one Resource: whether its
// first pass started, and the priority its adds asked for while it was
// raised, which is the priority it would have run at without the rank.
type bringUpEntry struct {
	started  bool
	asked    int
	hasAsked bool
}

func newBringUpTracker() *bringUpTracker {
	return &bringUpTracker{entries: map[string]bringUpEntry{}}
}

// pending reports whether the Resource still waits for its first pass.
// A nil tracker has nothing pending.
func (t *bringUpTracker) pending(name string) bool {
	if t == nil {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	return !t.entries[name].started
}

// started marks the first pass of the Resource begun.
func (t *bringUpTracker) started(name string) {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	entry := t.entries[name]
	entry.started = true
	t.entries[name] = entry
}

// raised records the priority an add of a pending Resource asked for
// before the rank replaced it. Priorities at or above the rank are the
// rank itself coming back from a requeue and say nothing.
func (t *bringUpTracker) raised(name string, priority int) {
	if t == nil || priority >= bringUpPriority {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	entry := t.entries[name]
	if !entry.hasAsked || priority > entry.asked {
		entry.asked = priority
		entry.hasAsked = true
	}

	t.entries[name] = entry
}

// unranked is the priority a requeue of the Resource returns to when it
// comes back at the rank: the highest one its adds asked for while it
// was pending, or handler.LowPriority, where a Resource of the initial
// list runs, when none did.
func (t *bringUpTracker) unranked(name string) int {
	if t == nil {
		return handler.LowPriority
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	entry := t.entries[name]
	if !entry.hasAsked {
		return handler.LowPriority
	}

	return entry.asked
}

// forget drops a Resource that is gone, so one created again under the
// same name is brought up first like any new replica.
func (t *bringUpTracker) forget(name string) {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.entries, name)
}

// bringUpQueue raises every add of a pending Resource to bringUpPriority.
//
// controller-runtime ranks an event above the initial list: a Resource
// that was already in the API when the satellite started is queued at
// handler.LowPriority, and every later event at normal priority. After a
// reboot that is backwards. Each replica that comes up sets off kernel
// and peer events, which queue that replica again at normal priority, so
// once enough of them are up their repeat passes take every worker, and
// the replicas still waiting for their first pass sit behind them with
// nothing in progress and nothing logged. LINSTOR's satellite has no such
// order: its device manager applies every resource of the first full sync
// before it turns to updates.
//
// Priority is decided here, where every add passes, rather than in each
// event handler: the For watch, the map functions, the observer channel
// and the reconciler's own retries all reach the queue through these four
// methods.
type bringUpQueue struct {
	priorityqueue.PriorityQueue[reconcile.Request]

	tracker *bringUpTracker
}

// newBringUpQueueFunc returns the controller's NewQueue: the priority
// queue controller-runtime builds by default, wrapped in bringUpQueue.
func newBringUpQueueFunc(
	tracker *bringUpTracker, logger logr.Logger,
) func(string, workqueue.TypedRateLimiter[reconcile.Request]) workqueue.TypedRateLimitingInterface[reconcile.Request] {
	return func(name string, limiter workqueue.TypedRateLimiter[reconcile.Request]) workqueue.TypedRateLimitingInterface[reconcile.Request] {
		inner := priorityqueue.New(name, func(o *priorityqueue.Opts[reconcile.Request]) {
			o.Log = logger.WithValues("controller", name)
			o.RateLimiter = limiter
		})

		return &bringUpQueue{PriorityQueue: inner, tracker: tracker}
	}
}

// Add queues item at normal priority, or at bringUpPriority when pending.
func (q *bringUpQueue) Add(item reconcile.Request) {
	q.AddWithOpts(priorityqueue.AddOpts{}, item)
}

// AddAfter queues item after the delay, ranked as Add does.
func (q *bringUpQueue) AddAfter(item reconcile.Request, after time.Duration) {
	q.AddWithOpts(priorityqueue.AddOpts{After: after}, item)
}

// AddRateLimited queues item after its back-off, ranked as Add does.
func (q *bringUpQueue) AddRateLimited(item reconcile.Request) {
	q.AddWithOpts(priorityqueue.AddOpts{RateLimited: true}, item)
}

// AddWithOpts ranks each item by whether it is pending.
//
// A first pass is dequeued at bringUpPriority, and controller-runtime
// queues the pass's own requeue (a failure retry, the periodic re-check)
// at the priority it dequeued, so the replica would keep the rank for
// good. That requeue goes back to the priority the replica's adds asked
// for while it was pending: handler.LowPriority for a replica of the
// initial list, normal priority for one an event queued.
//
// An event that comes in after a first pass is dequeued and before the
// pass marks the replica started is still queued at the rank, and gives
// the replica one more pass ahead of the others. The window is the
// replica's read from the cache.
func (q *bringUpQueue) AddWithOpts(opts priorityqueue.AddOpts, items ...reconcile.Request) {
	var raised, rest []reconcile.Request

	for _, item := range items {
		if q.tracker.pending(item.Name) {
			q.tracker.raised(item.Name, priorityOf(opts))
			raised = append(raised, item)
		} else {
			rest = append(rest, item)
		}
	}

	if len(raised) > 0 {
		up := opts
		up.Priority = priorityPtr(max(priorityOf(opts), bringUpPriority))
		q.PriorityQueue.AddWithOpts(up, raised...)
	}

	for _, item := range rest {
		down := opts
		if priorityOf(opts) >= bringUpPriority {
			down.Priority = priorityPtr(q.tracker.unranked(item.Name))
		}

		q.PriorityQueue.AddWithOpts(down, item)
	}
}

// priorityOf is the priority opts asks for; unset is normal priority.
func priorityOf(opts priorityqueue.AddOpts) int {
	if opts.Priority == nil {
		return 0
	}

	return *opts.Priority
}

func priorityPtr(priority int) *int {
	return &priority
}
