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

// TestPassesAreCountedByReason: every pass is counted by why it ran, so
// the share of each kind of event can be read off the satellite log, and
// the counts start over on every report.
func TestPassesAreCountedByReason(t *testing.T) {
	counter := newPassCounter()

	reconciler := counter.counted(reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
		return reconcile.Result{}, nil
	}))

	pass := func(name string) {
		t.Helper()

		_, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})
		if err != nil {
			t.Fatal(err)
		}
	}

	if kv := counter.report(); kv != nil {
		t.Fatalf("a report before any pass: %v", kv)
	}

	counter.changed("a", passResource)
	counter.changed("a", passPeer) // the last event before the pass counts
	pass("a")
	pass("a") // nothing came in since: a retry

	counter.changed("b", passKernel)
	pass("b")

	counter.changed("c", passDrift)
	pass("c")

	want := map[string]uint64{passPeer: 1, passRetry: 1, passKernel: 1, passDrift: 1}

	got := map[string]uint64{}

	kv := counter.report()
	for i := 0; i+1 < len(kv); i += 2 {
		if n, ok := kv[i+1].(uint64); ok {
			got[kv[i].(string)] = n
		}
	}

	for reason, n := range want {
		if got[reason] != n {
			t.Errorf("%s passes = %d, want %d (report %v)", reason, got[reason], n, kv)
		}
	}

	if got[passResource] != 0 || got[passDefinition] != 0 {
		t.Errorf("passes counted for events that did not reach a pass: %v", kv)
	}

	if kv := counter.report(); kv != nil {
		t.Errorf("the counts did not start over after a report: %v", kv)
	}
}

func TestNilPassCounterCountsNothing(t *testing.T) {
	var counter *passCounter

	counter.changed("r", passResource)

	if reason := counter.begin("r"); reason != passRetry {
		t.Fatalf("a reconciler without SetupWithManager gave a pass reason %q", reason)
	}
}

// passCount is how many passes ran for reason since the last report.
func (c *passCounter) passCount(reason string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.passes[reason]
}
