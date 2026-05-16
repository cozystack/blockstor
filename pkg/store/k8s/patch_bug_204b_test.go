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

// Bug 204b tests. v14 audit Bug 204 caught four wholesale-`Update(...)`
// sites on the tenant CRDs (ResourceDefinition, Resource, StoragePool,
// VolumeDefinition) that exhibit the same lost-update class as Bug 201.
// Two of the four (RD, VD) wrap the wholesale Spec replace in
// `RetryOnConflict`, but the retry replays the STALE wire snapshot the
// caller built once before the loop — so a 409 just re-applies the
// already-stale write. The other two (Resource, StoragePool) have no
// retry at all and silently drop the loser of any concurrent burst.
//
// The witnesses below burst 100 concurrent disjoint mutations against
// the same CRD. With the wholesale `Update`+stale-snapshot path each
// witness loses ~95+/100 writes. With the new `PatchXxxSpec` helpers
// (re-fetch + re-apply mutate + MergeFromWithOptimisticLock under
// `RetryOnConflict` with the Bug 201 backoff) every disjoint mutation
// converges.

func bug204bNewFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := crdv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(
			&crdv1alpha1.ResourceDefinition{},
			&crdv1alpha1.Resource{},
			&crdv1alpha1.StoragePool{},
		).
		Build()
}

// TestBug204bLostUpdateOnVanillaRDUpdate is the witness for the RD
// wholesale-Update path: each goroutine does `Get → mutate Props →
// Update`, exactly the shape the REST handlers use today. Under burst
// the stale-wire-snapshot retry on `resourceDefinitions.Update` causes
// most disjoint additions to silently disappear. Lives in tree so
// future audits can re-prove the class.
func TestBug204bLostUpdateOnVanillaRDUpdate(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("witness for Bug 204b — runs in long mode only")
	}

	const burst = 100

	rd := &apiv1.ResourceDefinition{
		Name:              "witness-rd",
		ResourceGroupName: "default",
		Props:             map[string]string{"seed": "1"},
	}

	cli := bug204bNewFakeClient(t)
	s := k8s.New(cli)

	if err := s.ResourceDefinitions().Create(t.Context(), rd); err != nil {
		t.Fatalf("Create: %v", err)
	}

	var wg sync.WaitGroup

	for i := range burst {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			cur, err := s.ResourceDefinitions().Get(t.Context(), "witness-rd")
			if err != nil {
				return
			}

			if cur.Props == nil {
				cur.Props = map[string]string{}
			}

			cur.Props[fmt.Sprintf("k-%d", idx)] = fmt.Sprintf("v-%d", idx)

			_ = s.ResourceDefinitions().Update(t.Context(), &cur)
		}(i)
	}

	wg.Wait()

	got, err := s.ResourceDefinitions().Get(t.Context(), "witness-rd")
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}

	lost := 0

	for i := range burst {
		if got.Props[fmt.Sprintf("k-%d", i)] == "" {
			lost++
		}
	}

	if lost == 0 {
		t.Errorf("expected lost-update under burst, got 0 — fixture may not exercise the race")
	} else {
		t.Logf("Bug 204b RD witness: lost %d/%d disjoint prop writes", lost, burst)
	}
}

// TestBug204bLostUpdateOnVanillaResourceUpdate is the witness for the
// Resource wholesale-Update path (no retry at all). Same shape as the
// RD witness; even more lossy because there's no RetryOnConflict.
func TestBug204bLostUpdateOnVanillaResourceUpdate(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("witness for Bug 204b — runs in long mode only")
	}

	const burst = 100

	res := &apiv1.Resource{
		Name:     "witness-rd",
		NodeName: "n1",
		Props:    map[string]string{"seed": "1"},
	}

	cli := bug204bNewFakeClient(t)
	s := k8s.New(cli)

	if err := s.Resources().Create(t.Context(), res); err != nil {
		t.Fatalf("Create: %v", err)
	}

	var wg sync.WaitGroup

	for i := range burst {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			cur, err := s.Resources().Get(t.Context(), "witness-rd", "n1")
			if err != nil {
				return
			}

			if cur.Props == nil {
				cur.Props = map[string]string{}
			}

			cur.Props[fmt.Sprintf("k-%d", idx)] = fmt.Sprintf("v-%d", idx)

			_ = s.Resources().Update(t.Context(), &cur)
		}(i)
	}

	wg.Wait()

	got, err := s.Resources().Get(t.Context(), "witness-rd", "n1")
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}

	lost := 0

	for i := range burst {
		if got.Props[fmt.Sprintf("k-%d", i)] == "" {
			lost++
		}
	}

	if lost == 0 {
		t.Errorf("expected lost-update under burst, got 0 — fixture may not exercise the race")
	} else {
		t.Logf("Bug 204b Resource witness: lost %d/%d disjoint prop writes", lost, burst)
	}
}

// TestPatchResourceDefinitionSpecConcurrent bursts 100 goroutines, each
// setting a unique prop key on the same ResourceDefinition. Under the
// Patch helper every disjoint key lands; under the wholesale `Update`
// (with retry replaying the stale wire snapshot) most writes vanish.
func TestPatchResourceDefinitionSpecConcurrent(t *testing.T) {
	t.Parallel()

	const burst = 100

	rd := &apiv1.ResourceDefinition{
		Name:              "rd1",
		ResourceGroupName: "default",
		Props:             map[string]string{"seed": "1"},
	}

	cli := bug204bNewFakeClient(t)
	s := k8s.New(cli)

	if err := s.ResourceDefinitions().Create(t.Context(), rd); err != nil {
		t.Fatalf("Create: %v", err)
	}

	var wg sync.WaitGroup

	errCount := atomic.Int64{}

	for i := range burst {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			err := s.ResourceDefinitions().PatchResourceDefinitionSpec(t.Context(), "rd1", func(r *apiv1.ResourceDefinition) error {
				if r.Props == nil {
					r.Props = map[string]string{}
				}

				r.Props[fmt.Sprintf("k-%d", idx)] = fmt.Sprintf("v-%d", idx)

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

	got, err := s.ResourceDefinitions().Get(t.Context(), "rd1")
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
			t.Errorf("lost-update: RD prop %q got %q want %q", key, got.Props[key], want)
		}
	}
}

// TestPatchResourceSpecConcurrent bursts 100 goroutines, each setting a
// unique prop key on the same (rd, node) Resource. Under the Patch
// helper every disjoint key lands; under the wholesale `Update` (no
// retry at all on Resources) writes silently disappear under contention.
func TestPatchResourceSpecConcurrent(t *testing.T) {
	t.Parallel()

	const burst = 100

	res := &apiv1.Resource{
		Name:     "rd1",
		NodeName: "n1",
		Props:    map[string]string{"seed": "1"},
	}

	cli := bug204bNewFakeClient(t)
	s := k8s.New(cli)

	if err := s.Resources().Create(t.Context(), res); err != nil {
		t.Fatalf("Create: %v", err)
	}

	var wg sync.WaitGroup

	errCount := atomic.Int64{}

	for i := range burst {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			err := s.Resources().PatchResourceSpec(t.Context(), "rd1", "n1", func(r *apiv1.Resource) error {
				if r.Props == nil {
					r.Props = map[string]string{}
				}

				r.Props[fmt.Sprintf("k-%d", idx)] = fmt.Sprintf("v-%d", idx)

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

	got, err := s.Resources().Get(t.Context(), "rd1", "n1")
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
			t.Errorf("lost-update: Resource prop %q got %q want %q", key, got.Props[key], want)
		}
	}
}

// TestPatchStoragePoolSpecConcurrent bursts 100 goroutines, each setting
// a unique prop key on the same (node, pool) StoragePool. Same shape as
// the Resource witness — StoragePool's wholesale `Update` is also un-
// retried so the lost-update rate is ~100% under burst.
func TestPatchStoragePoolSpecConcurrent(t *testing.T) {
	t.Parallel()

	const burst = 100

	sp := &apiv1.StoragePool{
		StoragePoolName: "pool1",
		NodeName:        "n1",
		ProviderKind:    "LVM",
		Props:           map[string]string{"seed": "1"},
	}

	cli := bug204bNewFakeClient(t)
	s := k8s.New(cli)

	if err := s.StoragePools().Create(t.Context(), sp); err != nil {
		t.Fatalf("Create: %v", err)
	}

	var wg sync.WaitGroup

	errCount := atomic.Int64{}

	for i := range burst {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			err := s.StoragePools().PatchStoragePoolSpec(t.Context(), "n1", "pool1", func(p *apiv1.StoragePool) error {
				if p.Props == nil {
					p.Props = map[string]string{}
				}

				p.Props[fmt.Sprintf("k-%d", idx)] = fmt.Sprintf("v-%d", idx)

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

	got, err := s.StoragePools().Get(t.Context(), "n1", "pool1")
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
			t.Errorf("lost-update: StoragePool prop %q got %q want %q", key, got.Props[key], want)
		}
	}
}

// TestPatchVolumeDefinitionSpecConcurrent bursts 100 goroutines, each
// setting a unique prop key on the SAME VolumeDefinition (vol_num=0).
// Witnesses the v14 RD-VD-shared-spec hazard: every patch writes
// rd.Spec.VolumeDefinitions[0].Props[k-idx]=..., the wholesale path
// replays the stale wire snapshot on retry, so concurrent disjoint
// keys clobber each other.
func TestPatchVolumeDefinitionSpecConcurrent(t *testing.T) {
	t.Parallel()

	const burst = 100

	rd := &apiv1.ResourceDefinition{
		Name:              "rd-vd",
		ResourceGroupName: "default",
	}

	cli := bug204bNewFakeClient(t)
	s := k8s.New(cli)

	if err := s.ResourceDefinitions().Create(t.Context(), rd); err != nil {
		t.Fatalf("Create RD: %v", err)
	}

	vd := &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      4096,
		Props:        map[string]string{"seed": "1"},
	}

	if err := s.VolumeDefinitions().Create(t.Context(), "rd-vd", vd); err != nil {
		t.Fatalf("Create VD: %v", err)
	}

	var wg sync.WaitGroup

	errCount := atomic.Int64{}

	for i := range burst {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			err := s.VolumeDefinitions().PatchVolumeDefinitionSpec(t.Context(), "rd-vd", 0, func(v *apiv1.VolumeDefinition) error {
				if v.Props == nil {
					v.Props = map[string]string{}
				}

				v.Props[fmt.Sprintf("k-%d", idx)] = fmt.Sprintf("v-%d", idx)

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

	got, err := s.VolumeDefinitions().Get(t.Context(), "rd-vd", 0)
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
			t.Errorf("lost-update: VD prop %q got %q want %q", key, got.Props[key], want)
		}
	}
}
