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
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// Bug 204a tests. The singleton `ControllerConfig` CRD is mutated
// from at least two REST call paths that race against each other:
//
//   - pkg/rest/controller_props.go:264, :203 — wholesale `Update(...)`
//     of `Spec.ExtraProps`.
//   - pkg/rest/node_connections.go:432, :467 — wholesale `Update(...)`
//     of `Spec.NodeConnections`.
//
// Two writers (e.g. `linstor c sp KeyA=v` and `linstor node-conn sp
// nodeA nodeB KeyB=v`) read the same CRD, mutate disjoint fields,
// and `Update(...)` in any order — the second writer's stale Spec
// wholesale-overwrites the first writer's mutation.
//
// `PatchControllerExtraProps` / `PatchControllerNodeConnections`
// re-derive their mutation against the freshly-fetched CRD on every
// `RetryOnConflict` cycle, so disjoint concurrent mutations all
// converge. Witness tests below burst 100 disjoint mutations against
// each helper and assert every write lands.

// bug204aNewFakeClient constructs a fresh fake client wired with the
// ControllerConfig CRD and Status subresource so optimistic-locking
// Patch reaches the same conflict-on-stale-RV behaviour the real
// apiserver enforces. Mirrors bug201NewFakeClient.
func bug204aNewFakeClient(t *testing.T, objs ...ctrlclient.Object) ctrlclient.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(
			&crdv1alpha1.ControllerConfig{},
		).
		Build()
}

// seedControllerConfig creates the singleton `default` ControllerConfig
// CRD with empty maps so subsequent Patch helpers exercise the
// "object exists, mutate field, Patch" branch — the racy path the
// REST handlers hit in production. The auto-create-on-NotFound branch
// is exercised by the inline `applyControllerProps` / `applyNodeConnectionProps`
// callers; the lost-update class targeted by Bug 204a manifests after
// the CRD already exists.
func seedControllerConfig(t *testing.T, c ctrlclient.Client) {
	t.Helper()

	cc := &crdv1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: crdv1alpha1.ControllerConfigName},
		Spec: crdv1alpha1.ControllerConfigSpec{
			ExtraProps:      map[string]string{},
			NodeConnections: map[string]map[string]string{},
		},
	}

	if err := c.Create(context.Background(), cc); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("seed ControllerConfig: %v", err)
	}
}

// TestBug204aExtraPropsConcurrentWritesAllPersist bursts 100 goroutines,
// each setting a unique key on `Spec.ExtraProps` via the Patch helper.
// Disjoint keys mean an ideal store converges to all 100 present; the
// wholesale-Update wire-replace path drops some of the keys because
// every goroutine reads the same baseline and overwrites whatever a
// sibling already wrote.
func TestBug204aExtraPropsConcurrentWritesAllPersist(t *testing.T) {
	t.Parallel()

	const burst = 100

	cli := bug204aNewFakeClient(t)
	seedControllerConfig(t, cli)

	var wg sync.WaitGroup

	errCount := atomic.Int64{}

	for i := range burst {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			err := k8s.PatchControllerExtraProps(t.Context(), cli, func(props map[string]string) error {
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

	var got crdv1alpha1.ControllerConfig
	if err := cli.Get(t.Context(),
		ctrlclient.ObjectKey{Name: crdv1alpha1.ControllerConfigName},
		&got,
	); err != nil {
		t.Fatalf("final Get: %v", err)
	}

	for i := range burst {
		key := fmt.Sprintf("k-%d", i)
		want := fmt.Sprintf("v-%d", i)

		if got.Spec.ExtraProps[key] != want {
			t.Errorf("lost-update: ExtraProps[%q] got %q want %q", key, got.Spec.ExtraProps[key], want)
		}
	}
}

// TestBug204aExtraPropsAutoCreatesSingleton exercises the
// "ControllerConfig CRD doesn't exist yet" branch — the helper must
// create the singleton on first write so a fresh cluster doesn't need
// an explicit `kubectl apply` of the ControllerConfig manifest before
// `linstor controller set-property` works. Matches the upstream
// `applyControllerProps` auto-create branch.
func TestBug204aExtraPropsAutoCreatesSingleton(t *testing.T) {
	t.Parallel()

	cli := bug204aNewFakeClient(t)

	err := k8s.PatchControllerExtraProps(t.Context(), cli, func(props map[string]string) error {
		props["first-key"] = "first-value"

		return nil
	})
	if err != nil {
		t.Fatalf("PatchControllerExtraProps: %v", err)
	}

	var got crdv1alpha1.ControllerConfig
	if err := cli.Get(t.Context(),
		ctrlclient.ObjectKey{Name: crdv1alpha1.ControllerConfigName},
		&got,
	); err != nil {
		t.Fatalf("Get after auto-create: %v", err)
	}

	if got.Spec.ExtraProps["first-key"] != "first-value" {
		t.Errorf("auto-create lost value: got %q want %q", got.Spec.ExtraProps["first-key"], "first-value")
	}
}

// TestBug204aLostUpdateOnVanillaUpdate is the regression-witness:
// it bursts the same 100 disjoint ExtraProps writes using the OLD
// REST-handler-style `Get -> mutate -> Update` pattern (no Patch
// helper) and shows that AT LEAST ONE mutation is silently lost.
// Mirrors TestBug201LostUpdateOnVanillaUpdate. If this PASSES then
// the fixture doesn't exercise the race — re-check the seed before
// proceeding.
func TestBug204aLostUpdateOnVanillaUpdate(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("witness for Bug 204a — runs in long mode only")
	}

	const burst = 50

	cli := bug204aNewFakeClient(t)
	seedControllerConfig(t, cli)

	var wg sync.WaitGroup

	for i := range burst {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			// Old REST-handler pattern: Get -> mutate -> Update.
			var cc crdv1alpha1.ControllerConfig
			if err := cli.Get(t.Context(),
				ctrlclient.ObjectKey{Name: crdv1alpha1.ControllerConfigName},
				&cc,
			); err != nil {
				return
			}

			if cc.Spec.ExtraProps == nil {
				cc.Spec.ExtraProps = map[string]string{}
			}

			cc.Spec.ExtraProps[fmt.Sprintf("k-%d", idx)] = fmt.Sprintf("v-%d", idx)

			_ = cli.Update(t.Context(), &cc)
		}(i)
	}

	wg.Wait()

	var got crdv1alpha1.ControllerConfig
	if err := cli.Get(t.Context(),
		ctrlclient.ObjectKey{Name: crdv1alpha1.ControllerConfigName},
		&got,
	); err != nil {
		t.Fatalf("final Get: %v", err)
	}

	lost := 0
	for i := range burst {
		if _, present := got.Spec.ExtraProps[fmt.Sprintf("k-%d", i)]; !present {
			lost++
		}
	}

	if lost == 0 {
		t.Errorf("expected lost-update under burst, got 0 — fixture may not exercise the race")
	} else {
		t.Logf("Bug 204a witness: lost %d/%d ExtraProps writes under wholesale Update", lost, burst)
	}
}

// TestBug204aExtraPropsModifyAndDeleteConverges pairs disjoint
// concurrent "set add-i" and "delete del-i" mutations on the same
// singleton — exercises both directions of the helper at once.
func TestBug204aExtraPropsModifyAndDeleteConverges(t *testing.T) {
	t.Parallel()

	const burst = 50

	cli := bug204aNewFakeClient(t)

	// Seed with `burst` "del-*" entries that the delete goroutines remove.
	cc := &crdv1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: crdv1alpha1.ControllerConfigName},
		Spec: crdv1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{},
		},
	}
	for i := range burst {
		cc.Spec.ExtraProps[fmt.Sprintf("del-%d", i)] = fmt.Sprintf("v-%d", i)
	}

	if err := cli.Create(t.Context(), cc); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var wg sync.WaitGroup

	errCount := atomic.Int64{}

	for i := range burst {
		wg.Add(2)

		// Setter goroutine: write "add-i".
		go func(idx int) {
			defer wg.Done()

			err := k8s.PatchControllerExtraProps(t.Context(), cli, func(props map[string]string) error {
				props[fmt.Sprintf("add-%d", idx)] = fmt.Sprintf("set-%d", idx)

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

			err := k8s.PatchControllerExtraProps(t.Context(), cli, func(props map[string]string) error {
				delete(props, fmt.Sprintf("del-%d", idx))

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

	var got crdv1alpha1.ControllerConfig
	if err := cli.Get(t.Context(),
		ctrlclient.ObjectKey{Name: crdv1alpha1.ControllerConfigName},
		&got,
	); err != nil {
		t.Fatalf("final Get: %v", err)
	}

	for i := range burst {
		add := fmt.Sprintf("add-%d", i)
		want := fmt.Sprintf("set-%d", i)

		if got.Spec.ExtraProps[add] != want {
			t.Errorf("lost-update: add-prop %q got %q want %q", add, got.Spec.ExtraProps[add], want)
		}

		del := fmt.Sprintf("del-%d", i)
		if _, present := got.Spec.ExtraProps[del]; present {
			t.Errorf("lost-update: del-prop %q survived deletion", del)
		}
	}
}
