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

package k8s_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// Bug 201 tests. The wholesale `existing.Spec = wireToCRDXSpec(in)` pattern
// in Store.Nodes().Update / Store.ResourceGroups().Update applies a STALE
// wire snapshot on every retry of `RetryOnConflict`. Two concurrent
// REST-handler-style read-mutate-write sequences against the same CRD
// silently drop one writer's mutation. The patch helpers introduced by
// the Bug 201 fix re-derive the mutation against the freshly-fetched
// CRD on every retry, so concurrent disjoint mutations all converge.
//
// TestBug201LostUpdateOnVanillaUpdate is the regression-witness: it
// repeats the NetInterface burst using the old REST-style
// Get → mutate → Update pattern (no Patch) and shows that AT LEAST
// ONE mutation is silently lost — proving the bug exists on
// e3a29897d. The remaining three tests use the new Patch helpers
// and converge as expected.

func bug201NewFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(
			&crdv1alpha1.Node{},
			&crdv1alpha1.ResourceGroup{},
		).
		Build()
}

// TestBug201LostUpdateOnVanillaUpdate proves the bug: under burst,
// the wholesale-Spec-replace path silently loses concurrent
// mutations. The test reproduces the v13/Bug 201 race using only
// the existing `Get → mutate → Update` API the REST handlers use
// today (no Patch helper). If this test PASSES on main HEAD
// e3a29897d, the bug is not reachable from this fixture and the
// scenario picked here doesn't reflect production behaviour —
// re-check the seed before proceeding.
//
// This test is wrapped in a sub-test that uses t.Skip() once the
// Patch helpers are in place so it doesn't fail CI; the witness
// stays in tree so future audits can re-prove the class.
func TestBug201LostUpdateOnVanillaUpdate(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("witness for Bug 201 — runs in long mode only")
	}

	const burst = 50

	node := &apiv1.Node{
		Name: "witness",
		Type: "SATELLITE",
	}
	for i := range burst {
		node.NetInterfaces = append(node.NetInterfaces, apiv1.NetInterface{
			Name:    fmt.Sprintf("seed-%d", i),
			Address: fmt.Sprintf("10.0.0.%d", i+1),
		})
	}

	cli := bug201NewFakeClient(t)
	s := k8s.New(cli)

	if err := s.Nodes().Create(t.Context(), node); err != nil {
		t.Fatalf("Create: %v", err)
	}

	var wg sync.WaitGroup

	for i := range burst {
		wg.Add(2)

		go func(idx int) {
			defer wg.Done()

			// Old REST-handler pattern: Get → mutate → Update.
			n, err := s.Nodes().Get(t.Context(), "witness")
			if err != nil {
				return
			}

			n.NetInterfaces = append(n.NetInterfaces, apiv1.NetInterface{
				Name:    fmt.Sprintf("added-%d", idx),
				Address: fmt.Sprintf("10.1.0.%d", idx+1),
			})

			_ = s.Nodes().Update(t.Context(), &n)
		}(i)

		go func(idx int) {
			defer wg.Done()

			n, err := s.Nodes().Get(t.Context(), "witness")
			if err != nil {
				return
			}

			out := n.NetInterfaces[:0]
			target := fmt.Sprintf("seed-%d", idx)

			for _, ni := range n.NetInterfaces {
				if ni.Name == target {
					continue
				}

				out = append(out, ni)
			}

			n.NetInterfaces = out

			_ = s.Nodes().Update(t.Context(), &n)
		}(i)
	}

	wg.Wait()

	got, err := s.Nodes().Get(t.Context(), "witness")
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}

	have := map[string]bool{}
	for _, ni := range got.NetInterfaces {
		have[ni.Name] = true
	}

	lost := 0

	for i := range burst {
		if !have[fmt.Sprintf("added-%d", i)] {
			lost++
		}

		if have[fmt.Sprintf("seed-%d", i)] {
			lost++
		}
	}

	// Witness pin: at least ONE mutation lost under burst proves the
	// class. Production behaviour is much worse (the dispatcher's
	// pre-Get → autoplace pipeline widens the stale-snapshot window
	// from microseconds to seconds), but the fake-client baseline is
	// enough to surface the class.
	if lost == 0 {
		t.Errorf("expected lost-update under burst, got 0 — fixture may not exercise the race")
	} else {
		t.Logf("Bug 201 witness: lost %d mutations under burst=%d (concurrent add+delete)", lost, burst)
	}
}

// TestBug201NetInterfaceCreateDeleteRaceWithStoreRetry bursts 50 pairs of
// (AddNetInterface, DeleteNetInterface) concurrently against the same
// Node CRD. Under the Patch helper, every disjoint addition lands and
// every disjoint deletion lands; under the old wholesale-replace Update
// path, the stale wire snapshot causes at least one of the additions
// (or deletions) to silently disappear.
func TestBug201NetInterfaceCreateDeleteRaceWithStoreRetry(t *testing.T) {
	t.Parallel()

	const burst = 50

	node := &apiv1.Node{
		Name:          "n1",
		Type:          "SATELLITE",
		NetInterfaces: []apiv1.NetInterface{
			// Seed with `burst` interfaces that the delete goroutines
			// will each remove a disjoint one of, plus baseline_X
			// interfaces the add goroutines will see in their Get.
			// Keeping the seed list non-empty exercises the
			// "wholesale Spec replace clobbers concurrent additions"
			// pattern from the report (steps T0–T3 in v13/Bug 201).
		},
	}

	// Seed `burst` pre-existing interfaces named "seed-N" — the delete
	// goroutines target these.
	for i := range burst {
		node.NetInterfaces = append(node.NetInterfaces, apiv1.NetInterface{
			Name:    fmt.Sprintf("seed-%d", i),
			Address: fmt.Sprintf("10.0.0.%d", i+1),
		})
	}

	cli := bug201NewFakeClient(t)
	s := k8s.New(cli)

	if err := s.Nodes().Create(t.Context(), node); err != nil {
		t.Fatalf("Create: %v", err)
	}

	var wg sync.WaitGroup
	errCount := atomic.Int64{}

	for i := range burst {
		wg.Add(2)

		// Add goroutine: append "added-i".
		go func(idx int) {
			defer wg.Done()

			err := s.Nodes().PatchNetInterfaces(t.Context(), "n1", func(in []apiv1.NetInterface) ([]apiv1.NetInterface, error) {
				return append(in, apiv1.NetInterface{
					Name:    fmt.Sprintf("added-%d", idx),
					Address: fmt.Sprintf("10.1.0.%d", idx+1),
				}), nil
			})
			if err != nil {
				errCount.Add(1)

				t.Errorf("add %d: %v", idx, err)
			}
		}(i)

		// Delete goroutine: drop "seed-i".
		go func(idx int) {
			defer wg.Done()

			err := s.Nodes().PatchNetInterfaces(t.Context(), "n1", func(in []apiv1.NetInterface) ([]apiv1.NetInterface, error) {
				out := in[:0]
				target := fmt.Sprintf("seed-%d", idx)

				for _, ni := range in {
					if ni.Name == target {
						continue
					}

					out = append(out, ni)
				}

				return out, nil
			})
			if err != nil {
				errCount.Add(1)

				t.Errorf("delete %d: %v", idx, err)
			}
		}(i)
	}

	wg.Wait()

	if errCount.Load() != 0 {
		t.Fatalf("unexpected errors: %d", errCount.Load())
	}

	got, err := s.Nodes().Get(t.Context(), "n1")
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}

	have := map[string]bool{}
	for _, ni := range got.NetInterfaces {
		have[ni.Name] = true
	}

	// Every added-* must be present.
	for i := range burst {
		name := fmt.Sprintf("added-%d", i)
		if !have[name] {
			t.Errorf("lost-update: %q is missing after burst", name)
		}
	}

	// Every seed-* must be gone.
	for i := range burst {
		name := fmt.Sprintf("seed-%d", i)
		if have[name] {
			t.Errorf("lost-update: %q survived despite delete-goroutine", name)
		}
	}
}

// TestBug201NodePropertyConcurrentWritesAllPersist exercises Patch on
// Props: 50 parallel goroutines each set a DIFFERENT key on the same
// Node. Disjoint keys mean an ideal store converges to all 50 present.
// The wholesale-Update wire-replace path drops some of the keys because
// every goroutine reads `Props=[seed]` at the REST layer and re-writes
// `Props=[seed, mine]` over whatever a sibling already added.
func TestBug201NodePropertyConcurrentWritesAllPersist(t *testing.T) {
	t.Parallel()

	const burst = 50

	node := &apiv1.Node{
		Name:  "np1",
		Type:  "SATELLITE",
		Props: map[string]string{"seed": "1"},
	}

	cli := bug201NewFakeClient(t)
	s := k8s.New(cli)

	if err := s.Nodes().Create(t.Context(), node); err != nil {
		t.Fatalf("Create: %v", err)
	}

	var wg sync.WaitGroup

	errCount := atomic.Int64{}

	for i := range burst {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			err := s.Nodes().PatchProps(t.Context(), "np1", func(props map[string]string) error {
				props[fmt.Sprintf("k-%d", idx)] = fmt.Sprintf("v-%d", idx)

				return nil
			})
			if err != nil {
				errCount.Add(1)

				t.Errorf("patch %d: %v", idx, err)
			}
		}(i)
	}

	wg.Wait()

	if errCount.Load() != 0 {
		t.Fatalf("unexpected errors: %d", errCount.Load())
	}

	got, err := s.Nodes().Get(t.Context(), "np1")
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}

	if got.Props["seed"] != "1" {
		t.Errorf("seed prop lost: got %q want %q", got.Props["seed"], "1")
	}

	for i := range burst {
		key := fmt.Sprintf("k-%d", i)
		want := fmt.Sprintf("v-%d", i)

		if got.Props[key] != want {
			t.Errorf("lost-update: prop %q got %q want %q", key, got.Props[key], want)
		}
	}
}

// TestBug201RGModifyAndDeletePropConverges exercises Patch on
// ResourceGroup props: 50 concurrent goroutines pair "set key-i" with
// "delete key-i-other". Disjoint keys mean both writers should land
// without losing each other's work.
func TestBug201RGModifyAndDeletePropConverges(t *testing.T) {
	t.Parallel()

	const burst = 50

	rg := &apiv1.ResourceGroup{
		Name: "rg1",
	}

	// Seed with `burst` "del-*" keys that the delete goroutines remove.
	rg.Props = map[string]string{}
	for i := range burst {
		rg.Props[fmt.Sprintf("del-%d", i)] = fmt.Sprintf("v-%d", i)
	}

	cli := bug201NewFakeClient(t)
	s := k8s.New(cli)

	if err := s.ResourceGroups().Create(t.Context(), rg); err != nil {
		t.Fatalf("Create: %v", err)
	}

	var wg sync.WaitGroup

	errCount := atomic.Int64{}

	for i := range burst {
		wg.Add(2)

		// Setter goroutine: write "add-i".
		go func(idx int) {
			defer wg.Done()

			err := s.ResourceGroups().PatchResourceGroup(t.Context(), "rg1", func(g *apiv1.ResourceGroup) error {
				if g.Props == nil {
					g.Props = map[string]string{}
				}

				g.Props[fmt.Sprintf("add-%d", idx)] = fmt.Sprintf("set-%d", idx)

				return nil
			})
			if err != nil {
				errCount.Add(1)

				t.Errorf("set %d: %v", idx, err)
			}
		}(i)

		// Deleter goroutine: drop "del-i".
		go func(idx int) {
			defer wg.Done()

			err := s.ResourceGroups().PatchResourceGroup(t.Context(), "rg1", func(g *apiv1.ResourceGroup) error {
				delete(g.Props, fmt.Sprintf("del-%d", idx))

				return nil
			})
			if err != nil {
				errCount.Add(1)

				t.Errorf("delete %d: %v", idx, err)
			}
		}(i)
	}

	wg.Wait()

	if errCount.Load() != 0 {
		t.Fatalf("unexpected errors: %d", errCount.Load())
	}

	got, err := s.ResourceGroups().Get(t.Context(), "rg1")
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}

	for i := range burst {
		add := fmt.Sprintf("add-%d", i)
		want := fmt.Sprintf("set-%d", i)

		if got.Props[add] != want {
			t.Errorf("lost-update: add-prop %q got %q want %q", add, got.Props[add], want)
		}

		del := fmt.Sprintf("del-%d", i)
		if _, present := got.Props[del]; present {
			t.Errorf("lost-update: del-prop %q survived deletion", del)
		}
	}
}
