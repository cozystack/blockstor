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
	"testing"

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
	gate := newRecheckGate()

	if !gate.tryAcquire() {
		t.Fatalf("free slot refused")
	}

	if gate.tryAcquire() {
		t.Fatalf("a second re-check got the slot while the first one runs")
	}

	gate.release()

	if !gate.tryAcquire() {
		t.Fatalf("slot not free after release")
	}
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
