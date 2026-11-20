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
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestRecheckGateTellsRechecksFromEvents(t *testing.T) {
	gate := newRecheckGate()

	gate.changed("r")

	token, recheck := gate.begin("r")
	if recheck {
		t.Fatalf("a pass an event asked for counted as a re-check")
	}

	gate.finish("r", token, true)

	token, recheck = gate.begin("r")
	if !recheck {
		t.Fatalf("the pass the previous one scheduled is not a re-check")
	}

	// An event while the re-check runs: the pass it causes has a reason
	// of its own even though the re-check schedules the next one.
	gate.changed("r")
	gate.finish("r", token, true)

	if _, recheck = gate.begin("r"); recheck {
		t.Fatalf("a pass for an event that came in during a re-check counted as a re-check")
	}

	// An event between the scheduling and the pass.
	gate.finish("r", gate.events("r"), true)
	gate.changed("r")

	if _, recheck = gate.begin("r"); recheck {
		t.Fatalf("a re-check with an event since its scheduling counted as a re-check")
	}

	// A pass that schedules nothing forgets the replica.
	gate.finish("r", gate.events("r"), false)

	if _, known := gate.state["r"]; known {
		t.Fatalf("a replica with nothing scheduled is still tracked")
	}
}

func TestRecheckGateRunsOneAtATime(t *testing.T) {
	gate, _ := newClockedRecheckGate()

	started, _, ok := gate.tryAcquire("a")
	if !ok {
		t.Fatalf("free slot refused")
	}

	if _, _, ok := gate.tryAcquire("b"); ok {
		t.Fatalf("a second re-check got the slot while the first one runs")
	}

	gate.release(started)

	if _, _, ok := gate.tryAcquire("b"); !ok {
		t.Fatalf("slot not free after a re-check that took no time")
	}
}

// TestRecheckGivesWayToEvents: a re-check does not start while a pass
// an event asked for runs, so that pass does not share the CPU with a
// stream of re-checks; past recheckMaxWait it goes anyway.
func TestRecheckGivesWayToEvents(t *testing.T) {
	gate, clock := newClockedRecheckGate()

	done := gate.eventPass()

	if _, _, ok := gate.tryAcquire("r"); ok {
		t.Fatalf("a re-check started while an event pass runs")
	}

	*clock = clock.Add(recheckMaxWait)

	started, _, ok := gate.tryAcquire("r")
	if !ok {
		t.Fatalf("a re-check waited past recheckMaxWait for event passes")
	}

	gate.release(started)
	done()

	if _, _, ok := gate.tryAcquire("other"); !ok {
		t.Fatalf("slot not free once the event passes ended")
	}
}

// TestGatedRecheckWaitsForEventPass drives the wrapped reconciler: a
// replica due for its re-check is deferred, not run, while another
// replica's event pass is in flight.
func TestGatedRecheckWaitsForEventPass(t *testing.T) {
	gate, _ := newClockedRecheckGate()

	entered := make(chan struct{})
	release := make(chan struct{})

	var idlePasses atomic.Int32

	reconciler := gate.gate(reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
		switch req.Name {
		case "new":
			close(entered)
			<-release
		case "idle":
			idlePasses.Add(1)
		}

		return schedulePeriodicRecheck(ctx), nil
	}))

	gate.finish("idle", gate.events("idle"), true)

	done := make(chan struct{})

	go func() {
		defer close(done)

		_, _ = reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: "new"}})
	}()

	<-entered

	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: "idle"}})
	if err != nil {
		t.Fatal(err)
	}

	close(release)
	<-done

	if idlePasses.Load() != 0 || result.RequeueAfter == 0 {
		t.Errorf("a re-check ran (%d passes, result %v) while an event pass was in flight", idlePasses.Load(), result)
	}
}

// TestRecheckSlotCoolsDown: after a re-check the slot stays closed for
// as long as the re-check took, so back-to-back re-checks cannot take
// all the satellite's CPU.
func TestRecheckSlotCoolsDown(t *testing.T) {
	gate, clock := newClockedRecheckGate()

	started, _, ok := gate.tryAcquire("a")
	if !ok {
		t.Fatalf("free slot refused")
	}

	const took = 300 * time.Millisecond

	*clock = clock.Add(took)
	gate.release(started)

	*clock = clock.Add(recheckCooldown*took - time.Millisecond)

	if _, _, ok := gate.tryAcquire("b"); ok {
		t.Fatalf("the next re-check started before the slot cooled down")
	}

	*clock = clock.Add(time.Millisecond)

	if _, _, ok := gate.tryAcquire("b"); !ok {
		t.Fatalf("slot still closed after the cool-down")
	}
}

// TestRechecksTakeTurnsInOrder: the re-checks waiting for the slot get
// it in the order they first asked, whichever of them asks first once
// it is free, and the ones further back are told to wait longer.
func TestRechecksTakeTurnsInOrder(t *testing.T) {
	gate, clock := newClockedRecheckGate()

	started, _, ok := gate.tryAcquire("running")
	if !ok {
		t.Fatalf("free slot refused")
	}

	waits := make([]time.Duration, 0, 3)

	for _, name := range []string{"a", "b", "c"} {
		_, wait, ok := gate.tryAcquire(name)
		if ok {
			t.Fatalf("%s got the slot while a re-check runs", name)
		}

		waits = append(waits, wait)
	}

	*clock = clock.Add(time.Second)
	gate.release(started)
	*clock = clock.Add(recheckCooldown * time.Second)

	if waits[2] <= waits[1] || waits[1] <= waits[0] {
		t.Errorf("waits %v do not grow with the place in line", waits)
	}

	for _, name := range []string{"c", "b"} {
		if _, _, ok := gate.tryAcquire(name); ok {
			t.Fatalf("%s got the slot ahead of the re-check that asked before it", name)
		}
	}

	for _, name := range []string{"a", "b", "c"} {
		started, _, ok := gate.tryAcquire(name)
		if !ok {
			t.Fatalf("%s did not get the slot on its turn", name)
		}

		gate.release(started)
	}
}

// TestStaleRecheckLosesItsPlace: a re-check that stopped asking, its
// deferred pass lost, does not hold the line.
func TestStaleRecheckLosesItsPlace(t *testing.T) {
	gate, clock := newClockedRecheckGate()

	started, _, _ := gate.tryAcquire("running")

	if _, _, ok := gate.tryAcquire("gone"); ok {
		t.Fatalf("a re-check got the slot while another runs")
	}

	gate.release(started)

	*clock = clock.Add(recheckStale)

	if _, _, ok := gate.tryAcquire("next"); !ok {
		t.Fatalf("a re-check that stopped asking still holds the line")
	}
}

// TestEventTakesRecheckOutOfLine: an event for a waiting re-check makes
// its next pass an event pass, which leaves the line.
func TestEventTakesRecheckOutOfLine(t *testing.T) {
	gate, _ := newClockedRecheckGate()

	started, _, _ := gate.tryAcquire("running")

	for _, name := range []string{"changed", "next"} {
		if _, _, ok := gate.tryAcquire(name); ok {
			t.Fatalf("%s got the slot while a re-check runs", name)
		}
	}

	gate.changed("changed")
	gate.release(started)

	if _, _, ok := gate.tryAcquire("next"); !ok {
		t.Fatalf("a replica an event took out of the line still holds it")
	}
}

// newClockedRecheckGate is a gate whose time only moves when the test
// moves it.
func newClockedRecheckGate() (*recheckGate, *time.Time) {
	clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	gate := newRecheckGate()
	gate.now = func() time.Time { return clock }

	return gate, &clock
}

func TestNilRecheckGateGatesNothing(t *testing.T) {
	var gate *recheckGate

	gate.changed("r")
	gate.finish("r", 0, true)

	if _, recheck := gate.begin("r"); recheck {
		t.Fatalf("a reconciler without SetupWithManager gated a pass")
	}
}

// TestRecheckIsWhatThePassSays: a pass is followed by a re-check only
// when it said so, not when its requeue delay happens to equal the
// re-check period.
func TestRecheckIsWhatThePassSays(t *testing.T) {
	gate := newRecheckGate()

	same := reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
		return reconcile.Result{RequeueAfter: activeDRBDRequeue}, nil
	})
	marked := reconcile.Func(func(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
		return schedulePeriodicRecheck(ctx), nil
	})

	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "r"}}

	_, err := gate.gate(same).Reconcile(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}

	if _, recheck := gate.begin("r"); recheck {
		t.Errorf("a pass that only shares the re-check period counted as scheduling a re-check")
	}

	_, err = gate.gate(marked).Reconcile(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}

	if _, recheck := gate.begin("r"); !recheck {
		t.Errorf("a pass that scheduled the re-check was not followed by one")
	}
}

// events is the event count of name, as a pass starting now would get it.
func (g *recheckGate) events(name string) uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.state[name].events
}
