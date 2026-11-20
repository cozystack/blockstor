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
	"slices"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// recheckPoll is how soon the re-check first in line asks for the
	// slot again while it cannot have it. A try costs a few map
	// lookups, and no worker is held while it waits.
	recheckPoll = 100 * time.Millisecond

	// recheckMaxDeferral is the longest a re-check further back in
	// line waits between tries. It waits about as long as the
	// re-checks ahead of it should take, so a long line asks rarely.
	recheckMaxDeferral = 10 * time.Second

	// recheckStale is how long a re-check may go without asking before
	// it loses its place, so a replica whose deferred pass never comes
	// back cannot hold the line.
	recheckStale = 3 * recheckMaxDeferral

	// recheckTurnWeight is how many turns of the slot its average
	// remembers: each new turn moves it by 1/recheckTurnWeight.
	recheckTurnWeight = 8

	// recheckCooldown is how many times its own duration the re-check
	// slot stays closed after a re-check. One re-check stream ran back
	// to back is enough to fill the satellite's CPU limit on a node
	// with a hundred replicas; with the cool-down the re-checks take
	// at most half of the time.
	recheckCooldown = 1

	// passReportInterval is how often the satellite logs how many
	// resource passes it ran, by reason.
	passReportInterval = time.Minute

	// recheckMaxWait bounds how long a re-check gives way to passes an
	// event asked for, so a node that never goes quiet still gets its
	// safety net.
	recheckMaxWait = time.Minute
)

// The reasons a resource pass runs for, as the gate counts them.
const (
	passResource   = "resource"   // the replica's own Resource changed
	passPeer       = "peer"       // another node's replica of the same definition changed
	passDefinition = "definition" // the ResourceDefinition changed
	passKernel     = "kernel"     // the observer saw a kernel state change
	passRetry      = "retry"      // the previous pass asked to be run again
	passRecheck    = "recheck"    // the periodic re-check
	passDeferred   = "deferred"   // a re-check that waited for its turn instead
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
// and no event for the replica came in since. Otherwise its reason is
// the source of the last event that came in for it, or a retry the
// previous pass asked for. The events are counted
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

	// line holds the re-checks waiting for the slot, first come first
	// served, and cycle is about how long one turn of the slot takes,
	// the re-check and its cool-down.
	line  []string
	cycle time.Duration

	// passes counts the passes since the last report, by reason.
	passes map[string]uint64
}

// recheckState is what the gate knows about one replica: how many
// events came in for it, and whether its next pass is the re-check the
// previous one scheduled.
type recheckState struct {
	events uint64
	due    bool

	// source is the reason of the last event that came in.
	source string

	// waiting is when the re-check joined the line, and asked when it
	// last asked for the slot; both are zero while it is not in line.
	waiting time.Time
	asked   time.Time
}

func newRecheckGate() *recheckGate {
	return &recheckGate{
		now:    time.Now,
		state:  map[string]recheckState{},
		passes: map[string]uint64{},
	}
}

// changed records an event from source for name: its next pass has a
// reason other than the timer.
func (g *recheckGate) changed(name, source string) {
	if g == nil {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	s := g.state[name]
	s.events++
	s.due = false
	s.source = source
	g.leaveLine(name, &s)
	g.state[name] = s
}

// begin starts a pass of name. It returns the event count the pass
// started at, for finish, and the reason of the pass.
func (g *recheckGate) begin(name string) (uint64, string) {
	if g == nil {
		return 0, passRetry
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	s := g.state[name]

	reason := passRetry

	switch {
	case s.due:
		reason = passRecheck
	case s.source != "":
		reason = s.source
	}

	s.due = false
	s.source = ""
	g.state[name] = s

	return s.events, reason
}

// count records a pass for reason.
func (g *recheckGate) count(reason string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.passes[reason]++
}

// report returns the pass counts since the last report as key/value
// pairs for the log, with the re-checks waiting in line and how long a
// turn of the slot takes, and starts the counts over. It returns
// nothing when no pass ran.
func (g *recheckGate) report() []any {
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(g.passes) == 0 {
		return nil
	}

	fields := []any{"waiting", len(g.line), "turn", g.cycle.String()}

	for _, reason := range []string{passResource, passPeer, passDefinition, passKernel, passRetry, passRecheck, passDeferred} {
		fields = append(fields, reason, g.passes[reason])
	}

	clear(g.passes)

	return fields
}

// reportEvery logs the pass counts every interval until ctx ends, so
// the share of re-checks, and of the passes each kind of event causes,
// can be read off the satellite log.
func (g *recheckGate) reportEvery(interval time.Duration, logger logr.Logger) func(context.Context) error {
	return func(ctx context.Context) error {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				if kv := g.report(); kv != nil {
					logger.Info("resource passes", kv...)
				}
			}
		}
	}
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
		g.leaveLine(name, &s)
		delete(g.state, name)

		return
	}

	s.due = true
	g.state[name] = s
}

// tryAcquire takes the re-check slot for name when name is first in
// line, the slot is free and cooled down, and no pass an event asked
// for is running, or, past recheckMaxWait in line, regardless of those
// passes. The caller releases it with release, passing the time it got
// it. Otherwise name keeps its place in line and the result says how
// long to wait before asking again.
func (g *recheckGate) tryAcquire(name string) (time.Time, time.Duration, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.now()

	// The caller is asking now: refresh its place before the stale
	// ones go, so a late deferred pass does not lose its own place.
	if s := g.state[name]; !s.waiting.IsZero() {
		s.asked = now
		g.state[name] = s
	}

	g.dropStale(now)

	s := g.state[name]
	if s.waiting.IsZero() {
		g.line = append(g.line, name)
		s.waiting = now
	}

	s.asked = now
	g.state[name] = s

	pos := slices.Index(g.line, name)

	free := !g.running && !now.Before(g.freeAt)
	yield := g.active > 0 && now.Sub(s.waiting) < recheckMaxWait

	if pos != 0 || !free || yield {
		return time.Time{}, g.deferral(pos, now), false
	}

	g.leaveLine(name, &s)
	g.state[name] = s
	g.running = true

	return now, 0, true
}

// deferral is how long the re-check at pos in line waits before it
// asks again. The first in line asks when the cool-down ends, or soon
// when something else holds it; the others wait about as long as the
// turns ahead of them take.
func (g *recheckGate) deferral(pos int, now time.Time) time.Duration {
	if pos == 0 {
		switch {
		case g.active > 0:
			// Passes an event asked for run for seconds; no need to
			// ask ten times a second.
			return time.Second
		case !g.running && now.Before(g.freeAt):
			return max(g.freeAt.Sub(now), recheckPoll)
		default:
			return recheckPoll
		}
	}

	cycle := g.cycle
	if cycle <= 0 {
		cycle = time.Second
	}

	return min(max(time.Duration(pos)*cycle, recheckPoll), recheckMaxDeferral)
}

// release frees the re-check slot taken at started, closing it for
// recheckCooldown times as long as the re-check held it.
func (g *recheckGate) release(started time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.now()
	took := now.Sub(started)
	turn := (1 + recheckCooldown) * took

	g.running = false
	g.freeAt = now.Add(recheckCooldown * took)

	if g.cycle <= 0 {
		g.cycle = turn
	} else {
		g.cycle = ((recheckTurnWeight-1)*g.cycle + turn) / recheckTurnWeight
	}
}

// leaveLine takes name, whose state is s, out of the line.
func (g *recheckGate) leaveLine(name string, s *recheckState) {
	if s.waiting.IsZero() {
		return
	}

	g.line = slices.DeleteFunc(g.line, func(n string) bool { return n == name })
	s.waiting = time.Time{}
	s.asked = time.Time{}
}

// dropStale takes out of the line every re-check that has not asked
// for the slot in recheckStale.
func (g *recheckGate) dropStale(now time.Time) {
	g.line = slices.DeleteFunc(g.line, func(name string) bool {
		s := g.state[name]
		if now.Sub(s.asked) < recheckStale {
			return false
		}

		s.waiting = time.Time{}
		s.asked = time.Time{}
		g.state[name] = s

		return true
	})
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
// slot. While it cannot have the slot the pass is deferred, without
// holding the worker it was handed to.
func (g *recheckGate) gate(inner reconcile.Reconciler) reconcile.Reconciler {
	return reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
		token, reason := g.begin(req.Name)

		if reason == passRecheck {
			started, wait, ok := g.tryAcquire(req.Name)
			if !ok {
				g.finish(req.Name, token, true)
				g.count(passDeferred)

				return reconcile.Result{RequeueAfter: wait}, nil
			}

			defer g.release(started)
		} else {
			defer g.eventPass()()
		}

		g.count(reason)

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

// marker is a predicate that lets every event through and records it
// as coming from source. It goes last in a chain, so only events that
// reach the queue count.
func (g *recheckGate) marker(source string) predicate.Predicate {
	record := func(obj client.Object) bool {
		g.changed(obj.GetName(), source)

		return true
	}

	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return record(e.Object) },
		UpdateFunc:  func(e event.UpdateEvent) bool { return record(e.ObjectNew) },
		DeleteFunc:  func(e event.DeleteEvent) bool { return record(e.Object) },
		GenericFunc: func(e event.GenericEvent) bool { return record(e.Object) },
	}
}

// mapFunc records an event from source for every replica fn enqueues.
func (g *recheckGate) mapFunc(source string, fn handler.MapFunc) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		reqs := fn(ctx, obj)
		for _, req := range reqs {
			g.changed(req.Name, source)
		}

		return reqs
	}
}
