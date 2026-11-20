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
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// recheckDeferral is how long a periodic re-check waits for its
	// turn when it cannot run yet. It costs one map lookup per try, and
	// no worker is held while it waits.
	recheckDeferral = time.Second

	// recheckCooldown is how many times its own duration the re-check
	// slot stays closed after a re-check. One re-check stream ran back
	// to back is enough to fill the satellite's CPU limit on a node
	// with a hundred replicas; with the cool-down the re-checks take
	// at most half of the time.
	recheckCooldown = 1

	// recheckMaxWait bounds how long a re-check gives way to passes an
	// event asked for, so a node that never goes quiet still gets its
	// safety net.
	recheckMaxWait = time.Minute
)

// recheckGate runs the periodic re-checks of converged replicas one at
// a time, so the resource workers stay free for passes that have
// something to do.
//
// Every DRBD replica asks for a re-check activeDRBDRequeue after it
// converged, and a re-check is a full pass: drbdadm parses every .res
// on the node and adjusts the resource. On a node with a hundred
// replicas that is ten passes a second of work that finds nothing, and
// with several workers they run side by side until they take all the
// CPU the satellite has. A new replica's pass then runs next to seven
// re-checks and its device appears tens of seconds late, past the
// minute linstor-csi gives CreateVolume. LINSTOR's satellite runs no
// such re-check at all: DeviceManagerImpl dispatches only resources
// with a pending change. One re-check at a time is what the satellite
// did with a single worker, and every pass an event asked for, a new
// replica, a changed spec, a peer, a kernel state change or the
// replicas of a rebooted node, still gets the full parallelism.
//
// One re-check at a time is still a full pass after a full pass, and
// on a node with a hundred replicas that alone can take all the CPU
// the satellite is given, which a new replica's pass then shares. So a
// re-check also waits while any pass an event asked for is running,
// and the slot cools down after each re-check for as long as it took.
//
// A pass is a re-check when the replica's previous pass scheduled one
// and no event for the replica came in since. The events are counted
// where they enter the queue (marker and the map functions), which is
// the only place that tells them apart from a timer.
type recheckGate struct {
	now func() time.Time

	mu    sync.Mutex
	state map[string]recheckState

	// running is set while a re-check holds the slot, and freeAt is
	// when the slot opens again after the last one.
	running bool
	freeAt  time.Time

	// active counts the passes in flight that an event asked for.
	active int
}

// recheckState is what the gate knows about one replica: how many
// events came in for it, and whether its next pass is the re-check the
// previous one scheduled.
type recheckState struct {
	events uint64
	due    bool

	// waiting is when the re-check first asked for the slot and did
	// not get it; zero while it is not waiting.
	waiting time.Time
}

func newRecheckGate() *recheckGate {
	return &recheckGate{
		now:   time.Now,
		state: map[string]recheckState{},
	}
}

// changed records an event for name: its next pass has a reason other
// than the timer.
func (g *recheckGate) changed(name string) {
	if g == nil {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	s := g.state[name]
	s.events++
	s.due = false
	s.waiting = time.Time{}
	g.state[name] = s
}

// begin starts a pass of name. It returns the event count the pass
// started at, for finish, and whether the pass is a re-check.
func (g *recheckGate) begin(name string) (uint64, bool) {
	if g == nil {
		return 0, false
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	s := g.state[name]
	recheck := s.due
	s.due = false
	g.state[name] = s

	return s.events, recheck
}

// finish ends a pass of name that began at token. When the pass
// scheduled a re-check and no event came in while it ran, the next
// pass is that re-check. Otherwise the next pass has a reason of its
// own, and when nothing is scheduled at all the replica is forgotten.
func (g *recheckGate) finish(name string, token uint64, recheckScheduled bool) {
	if g == nil {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	s := g.state[name]
	if s.events != token {
		return
	}

	if !recheckScheduled {
		delete(g.state, name)

		return
	}

	s.due = true
	g.state[name] = s
}

// tryAcquire takes the re-check slot for name when it is free, cooled
// down, and no pass an event asked for is running, or, past
// recheckMaxWait of waiting, regardless of those passes. The caller
// releases it with release, passing the time it got it.
func (g *recheckGate) tryAcquire(name string) (time.Time, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.now()

	s := g.state[name]
	if s.waiting.IsZero() {
		s.waiting = now
	}

	free := !g.running && !now.Before(g.freeAt)
	yield := g.active > 0 && now.Sub(s.waiting) < recheckMaxWait

	if !free || yield {
		g.state[name] = s

		return time.Time{}, false
	}

	s.waiting = time.Time{}
	g.state[name] = s
	g.running = true

	return now, true
}

// release frees the re-check slot taken at started, closing it for
// recheckCooldown times as long as the re-check held it.
func (g *recheckGate) release(started time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.now()
	g.running = false
	g.freeAt = now.Add(recheckCooldown * now.Sub(started))
}

// eventPass counts a pass an event asked for as running, until the
// function it returns is called.
func (g *recheckGate) eventPass() func() {
	g.mu.Lock()
	g.active++
	g.mu.Unlock()

	return func() {
		g.mu.Lock()
		g.active--
		g.mu.Unlock()
	}
}

// gate wraps inner so that a periodic re-check waits for the re-check
// slot. While it cannot have the slot the pass is deferred by
// recheckDeferral, without holding the worker it was handed to.
func (g *recheckGate) gate(inner reconcile.Reconciler) reconcile.Reconciler {
	return reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
		token, recheck := g.begin(req.Name)

		if recheck {
			started, ok := g.tryAcquire(req.Name)
			if !ok {
				g.finish(req.Name, token, true)

				return reconcile.Result{RequeueAfter: recheckDeferral}, nil
			}

			defer g.release(started)
		} else {
			defer g.eventPass()()
		}

		scheduled := false

		result, err := inner.Reconcile(context.WithValue(ctx, recheckMarkKey{}, &scheduled), req)

		g.finish(req.Name, token, err == nil && scheduled)

		return result, err //nolint:wrapcheck // the inner reconciler's own error
	})
}

// recheckMarkKey carries, in the context of a gated pass, the flag
// schedulePeriodicRecheck sets.
type recheckMarkKey struct{}

// schedulePeriodicRecheck is the result of a pass that leaves a
// converged DRBD replica to its periodic re-check, and tells the gate
// so. The pass says it rather than the gate guessing it from the
// requeue delay, which another path may share some day.
func schedulePeriodicRecheck(ctx context.Context) reconcile.Result {
	if mark, ok := ctx.Value(recheckMarkKey{}).(*bool); ok {
		*mark = true
	}

	return reconcile.Result{RequeueAfter: activeDRBDRequeue}
}

// marker is a predicate that lets every event through and records it.
// It goes last in a chain, so only events that reach the queue count.
func (g *recheckGate) marker() predicate.Predicate {
	record := func(obj client.Object) bool {
		g.changed(obj.GetName())

		return true
	}

	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return record(e.Object) },
		UpdateFunc:  func(e event.UpdateEvent) bool { return record(e.ObjectNew) },
		DeleteFunc:  func(e event.DeleteEvent) bool { return record(e.Object) },
		GenericFunc: func(e event.GenericEvent) bool { return record(e.Object) },
	}
}

// mapFunc records an event for every replica fn enqueues.
func (g *recheckGate) mapFunc(fn handler.MapFunc) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		reqs := fn(ctx, obj)
		for _, req := range reqs {
			g.changed(req.Name)
		}

		return reqs
	}
}
