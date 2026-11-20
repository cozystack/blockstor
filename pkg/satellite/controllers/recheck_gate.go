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

// recheckDeferral is how long a periodic re-check waits for its turn
// when another one is running. It costs one map lookup per try, and no
// worker is held while it waits.
const recheckDeferral = time.Second

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
// A pass is a re-check when the replica's previous pass scheduled one
// and no event for the replica came in since. The events are counted
// where they enter the queue (marker and the map functions), which is
// the only place that tells them apart from a timer.
type recheckGate struct {
	slot chan struct{}

	mu    sync.Mutex
	state map[string]recheckState
}

// recheckState is what the gate knows about one replica: how many
// events came in for it, and whether its next pass is the re-check the
// previous one scheduled.
type recheckState struct {
	events uint64
	due    bool
}

func newRecheckGate() *recheckGate {
	return &recheckGate{
		slot:  make(chan struct{}, 1),
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

// tryAcquire takes the re-check slot when it is free. The caller
// releases it with release.
func (g *recheckGate) tryAcquire() bool {
	select {
	case g.slot <- struct{}{}:
		return true
	default:
		return false
	}
}

func (g *recheckGate) release() {
	<-g.slot
}

// gate wraps inner so that a periodic re-check waits for the re-check
// slot. While another re-check holds it the pass is deferred by
// recheckDeferral, without holding the worker it was handed to.
func (g *recheckGate) gate(inner reconcile.Reconciler) reconcile.Reconciler {
	return reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
		token, recheck := g.begin(req.Name)

		if recheck {
			if !g.tryAcquire() {
				g.finish(req.Name, token, true)

				return reconcile.Result{RequeueAfter: recheckDeferral}, nil
			}

			defer g.release()
		}

		result, err := inner.Reconcile(ctx, req)

		g.finish(req.Name, token, err == nil && isPeriodicRecheck(result))

		return result, err //nolint:wrapcheck // the inner reconciler's own error
	})
}

// isPeriodicRecheck reports whether a pass ended by scheduling the
// periodic re-check of a converged DRBD replica: no other path
// requeues after activeDRBDRequeue.
func isPeriodicRecheck(result reconcile.Result) bool {
	return result.RequeueAfter == activeDRBDRequeue
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
