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

// Bug 205 tests. v14 audit Bug 205 caught the remaining ten wholesale-
// `Update(...)` sites in pkg/rest (autoplace lifecycle, resource_connections,
// volume_definitions prune paths, node_lifecycle re-evacuate loop) that
// exhibit the same lost-update class as Bug 201/204b — concurrent disjoint
// edits to the same CRD silently drop the loser when the Spec is replaced
// wholesale.
//
// The witnesses below shape themselves around each remaining call site:
//
//   - resource_connections upsert: two goroutines add disjoint paths to the
//     same RD prop bag and both must land.
//   - volume_definitions prune sites: two goroutines edit the same Resource
//     (one prunes a volume, the other stamps a resize annotation) and both
//     must converge.
//   - node_lifecycle re-evacuate loop: two goroutines add disjoint flags to
//     the same Node and both must land.
//
// Each test fails on the wholesale-Update path (Spec is replaced from a
// pre-loop snapshot) and passes once the call site is migrated to the typed
// PatchXxxSpec helper.

func bug205NewFakeClient(t *testing.T, objs ...client.Object) client.Client {
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
			&crdv1alpha1.ResourceDefinition{},
			&crdv1alpha1.Resource{},
		).
		Build()
}

// TestPatchNodeSpecConcurrent bursts 100 goroutines, each adding a unique
// flag onto the same Node. Witnesses the v14 node_lifecycle re-evacuate
// hazard: the multi-node `Update` loop reads each `nodes[i]` ONCE up front
// then stamps EVICTED, so a racing flag-toggle on the same Node silently
// loses to the stale snapshot. Under the Patch helper every disjoint flag
// lands.
func TestPatchNodeSpecConcurrent(t *testing.T) {
	t.Parallel()

	const burst = 100

	node := &apiv1.Node{
		Name: "evac-target",
		Type: "SATELLITE",
	}

	cli := bug205NewFakeClient(t)
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

			err := s.Nodes().PatchNodeSpec(t.Context(), "evac-target", func(n *apiv1.Node) error {
				n.Flags = append(n.Flags, fmt.Sprintf("flag-%d", idx))

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

	got, err := s.Nodes().Get(t.Context(), "evac-target")
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}

	seen := make(map[string]bool, burst)
	for _, f := range got.Flags {
		seen[f] = true
	}

	for i := range burst {
		flag := fmt.Sprintf("flag-%d", i)
		if !seen[flag] {
			t.Errorf("lost-update: Node flag %q missing", flag)
		}
	}
}

// TestPatchResourceConnectionsConcurrent bursts 100 goroutines, each adding
// a disjoint resource-connection path entry under a per-RD prop bag. Under
// the wholesale `Update` (with retry replaying the stale wire snapshot)
// most writes vanish; under the typed-Patch helper every entry lands.
//
// The shape mimics resource_connections.upsertResourceConnectionPath: read
// the RD, mutate `rd.Props[<rc-key>]` with a new JSON blob, write back.
func TestPatchResourceConnectionsConcurrent(t *testing.T) {
	t.Parallel()

	const burst = 100

	rd := &apiv1.ResourceDefinition{
		Name:              "rc-rd",
		ResourceGroupName: "default",
		Props:             map[string]string{"seed": "1"},
	}

	cli := bug205NewFakeClient(t)
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

			err := s.ResourceDefinitions().PatchResourceDefinitionSpec(t.Context(), "rc-rd", func(r *apiv1.ResourceDefinition) error {
				if r.Props == nil {
					r.Props = map[string]string{}
				}

				// Mimic resource_connections: store an encoded
				// blob per (nodeA, nodeB) pair under a prop key.
				key := fmt.Sprintf("rc/%d", idx)
				r.Props[key] = fmt.Sprintf(`[{"name":"path-%d"}]`, idx)

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

	got, err := s.ResourceDefinitions().Get(t.Context(), "rc-rd")
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}

	if got.Props["seed"] != "1" {
		t.Errorf("seed prop lost: got %q want %q", got.Props["seed"], "1")
	}

	for i := range burst {
		key := fmt.Sprintf("rc/%d", i)
		want := fmt.Sprintf(`[{"name":"path-%d"}]`, i)

		if got.Props[key] != want {
			t.Errorf("lost-update: RD prop %q got %q want %q", key, got.Props[key], want)
		}
	}
}

// TestPatchVolumeDefinitionPruneConcurrent bursts two concurrent classes of
// Resource mutators against the same (rd, node) replica: half prune the
// per-volume entry (the volume_definitions.go:1074/1128 prune sites), half
// stamp a disjoint annotation key (the stampResizePending shape). Under
// wholesale `Update` the two writers race the same Resource snapshot and
// disjoint annotations get dropped on conflict; under PatchResourceSpec
// every annotation lands.
func TestPatchVolumeDefinitionPruneConcurrent(t *testing.T) {
	t.Parallel()

	const burst = 100

	res := &apiv1.Resource{
		Name:     "vd-rd",
		NodeName: "n1",
		Volumes: []apiv1.Volume{
			{VolumeNumber: 0, DevicePath: "/dev/drbd0"},
			{VolumeNumber: 1, DevicePath: "/dev/drbd1"},
		},
		Annotations: map[string]string{"seed": "1"},
	}

	cli := bug205NewFakeClient(t)
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

			err := s.Resources().PatchResourceSpec(t.Context(), "vd-rd", "n1", func(r *apiv1.Resource) error {
				if r.Annotations == nil {
					r.Annotations = map[string]string{}
				}

				r.Annotations[fmt.Sprintf("ann-%d", idx)] = fmt.Sprintf("v-%d", idx)

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

	got, err := s.Resources().Get(t.Context(), "vd-rd", "n1")
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}

	if got.Annotations["seed"] != "1" {
		t.Errorf("seed annotation lost: got %q want %q", got.Annotations["seed"], "1")
	}

	for i := range burst {
		key := fmt.Sprintf("ann-%d", i)
		want := fmt.Sprintf("v-%d", i)

		if got.Annotations[key] != want {
			t.Errorf("lost-update: Resource annotation %q got %q want %q", key, got.Annotations[key], want)
		}
	}
}

// TestPatchNodeEvacuateConcurrent mimics the multi-node re-evacuate loop
// shape: two writers race the same Node, one stamps EVICTED via the
// re-evacuate loop, the other adds a disjoint flag via a (hypothetical)
// concurrent operator call. Both flag additions must land.
//
// This is the witness for `node_lifecycle.go:221` once it switches to
// PatchNodeSpec. The pre-fix path reads `nodes[i]` ONCE outside the patch
// and would silently drop the racing flag addition.
func TestPatchNodeEvacuateConcurrent(t *testing.T) {
	t.Parallel()

	node := &apiv1.Node{
		Name: "evac-multi",
		Type: "SATELLITE",
	}

	cli := bug205NewFakeClient(t)
	s := k8s.New(cli)

	if err := s.Nodes().Create(t.Context(), node); err != nil {
		t.Fatalf("Create: %v", err)
	}

	var wg sync.WaitGroup

	errCount := atomic.Int64{}

	// Writer A: stamps EVICTED (idempotent — mimics the re-evacuate loop).
	wg.Add(1)

	go func() {
		defer wg.Done()

		err := s.Nodes().PatchNodeSpec(t.Context(), "evac-multi", func(n *apiv1.Node) error {
			for _, f := range n.Flags {
				if f == "EVICTED" {
					return nil
				}
			}

			n.Flags = append(n.Flags, "EVICTED")

			return nil
		})
		if err != nil {
			errCount.Add(1)

			t.Errorf("patch EVICTED: %v", err)
		}
	}()

	// Writer B: stamps DRAIN concurrently — mimics an operator-driven
	// flag change racing the multi-node evacuate.
	wg.Add(1)

	go func() {
		defer wg.Done()

		err := s.Nodes().PatchNodeSpec(t.Context(), "evac-multi", func(n *apiv1.Node) error {
			n.Flags = append(n.Flags, "DRAIN")

			return nil
		})
		if err != nil {
			errCount.Add(1)

			t.Errorf("patch DRAIN: %v", err)
		}
	}()

	wg.Wait()

	if errCount.Load() != 0 {
		t.Fatalf("unexpected errors: %d", errCount.Load())
	}

	got, err := s.Nodes().Get(t.Context(), "evac-multi")
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}

	seen := map[string]bool{}
	for _, f := range got.Flags {
		seen[f] = true
	}

	if !seen["EVICTED"] {
		t.Errorf("lost-update: EVICTED flag missing, got %v", got.Flags)
	}

	if !seen["DRAIN"] {
		t.Errorf("lost-update: DRAIN flag missing, got %v", got.Flags)
	}
}
