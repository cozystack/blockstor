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

package storetest

import (
	"sync"
	"testing"
)

// RunControllerPropsStore exercises the singleton controller-scope
// props bag. Added with the BUG-022 fix: the k8s store used to back
// this with a process-local map populated once at construction, so
// the CRD-backed and in-memory implementations drifted — reconcilers
// reading through the k8s store never saw `linstor controller
// set-property` writes. The shared suite pins both implementations
// to identical Get/Set semantics.
func RunControllerPropsStore(t *testing.T, newStore Factory) {
	t.Helper()
	t.Run("GetEmpty", func(t *testing.T) { testCtrlPropsGetEmpty(t, newStore) })
	t.Run("SetThenGet", func(t *testing.T) { testCtrlPropsSetThenGet(t, newStore) })
	t.Run("SetReplaces", func(t *testing.T) { testCtrlPropsSetReplaces(t, newStore) })
	t.Run("SetNilClears", func(t *testing.T) { testCtrlPropsSetNilClears(t, newStore) })
	t.Run("GetDefensiveCopy", func(t *testing.T) { testCtrlPropsGetDefensiveCopy(t, newStore) })
	// Reconcilers and the placer call Get concurrently while the REST
	// shim writes — pinned under -race so an implementation that hands
	// out its backing map (or mutates without locking) fails loudly.
	t.Run("ConcurrentGetSet", func(t *testing.T) { testCtrlPropsConcurrentGetSet(t, newStore) })
}

// testCtrlPropsGetEmpty pins the "no value written yet" contract: an
// empty, non-nil map so callers can do `props[key]` without nil-checks.
func testCtrlPropsGetEmpty(t *testing.T, newStore Factory) {
	t.Helper()

	props, err := newStore(t).ControllerProps().Get(t.Context())
	if err != nil {
		t.Fatalf("Get on fresh store: %v", err)
	}

	if props == nil {
		t.Fatal("Get on fresh store: want empty non-nil map, got nil")
	}

	if len(props) != 0 {
		t.Errorf("Get on fresh store: want empty map, got %v", props)
	}
}

func testCtrlPropsSetThenGet(t *testing.T, newStore Factory) {
	t.Helper()

	st := newStore(t)
	ctx := t.Context()

	want := map[string]string{
		"BalanceResourcesInterval":        "1",
		"Autoplacer/Weights/MaxFreeSpace": "2.0",
	}

	if err := st.ControllerProps().Set(ctx, want); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := st.ControllerProps().Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("Get after Set: got %v, want %v", got, want)
	}

	for k, v := range want {
		if got[k] != v {
			t.Errorf("Get after Set: key %q = %q, want %q", k, got[k], v)
		}
	}
}

// testCtrlPropsSetReplaces pins the replace-not-merge contract from
// store.ControllerPropsStore: a second Set drops keys absent from the
// new map.
func testCtrlPropsSetReplaces(t *testing.T, newStore Factory) {
	t.Helper()

	st := newStore(t)
	ctx := t.Context()

	if err := st.ControllerProps().Set(ctx, map[string]string{
		"BalanceResourcesInterval": "1",
		"BalanceResourcesEnabled":  "false",
	}); err != nil {
		t.Fatalf("first Set: %v", err)
	}

	if err := st.ControllerProps().Set(ctx, map[string]string{
		"BalanceResourcesInterval": "7",
	}); err != nil {
		t.Fatalf("second Set: %v", err)
	}

	got, err := st.ControllerProps().Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got["BalanceResourcesInterval"] != "7" {
		t.Errorf("BalanceResourcesInterval: got %q, want %q", got["BalanceResourcesInterval"], "7")
	}

	if _, stale := got["BalanceResourcesEnabled"]; stale {
		t.Errorf("BalanceResourcesEnabled survived a replacing Set: %v", got)
	}
}

func testCtrlPropsSetNilClears(t *testing.T, newStore Factory) {
	t.Helper()

	st := newStore(t)
	ctx := t.Context()

	if err := st.ControllerProps().Set(ctx, map[string]string{"A": "1"}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if err := st.ControllerProps().Set(ctx, nil); err != nil {
		t.Fatalf("Set(nil): %v", err)
	}

	got, err := st.ControllerProps().Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if len(got) != 0 {
		t.Errorf("Set(nil) must clear the bag; got %v", got)
	}
}

// testCtrlPropsGetDefensiveCopy pins that mutating the returned map
// never leaks back into the store.
func testCtrlPropsGetDefensiveCopy(t *testing.T, newStore Factory) {
	t.Helper()

	st := newStore(t)
	ctx := t.Context()

	if err := st.ControllerProps().Set(ctx, map[string]string{"A": "1"}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	first, err := st.ControllerProps().Get(ctx)
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}

	first["A"] = "tampered"
	first["B"] = "injected"

	second, err := st.ControllerProps().Get(ctx)
	if err != nil {
		t.Fatalf("second Get: %v", err)
	}

	if second["A"] != "1" {
		t.Errorf("mutation of a returned map leaked into the store: A=%q", second["A"])
	}

	if _, leaked := second["B"]; leaked {
		t.Errorf("insertion into a returned map leaked into the store: %v", second)
	}
}

func testCtrlPropsConcurrentGetSet(t *testing.T, newStore Factory) {
	t.Helper()

	st := newStore(t)
	ctx := t.Context()

	const iterations = 20

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		for i := range iterations {
			_ = st.ControllerProps().Set(ctx, map[string]string{
				"BalanceResourcesInterval": string(rune('1' + i%9)),
			})
		}
	}()

	go func() {
		defer wg.Done()

		for range iterations {
			props, err := st.ControllerProps().Get(ctx)
			if err != nil {
				continue
			}

			_ = props["BalanceResourcesInterval"]
		}
	}()

	wg.Wait()
}
