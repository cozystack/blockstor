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

package controller_test

import (
	"context"
	"testing"

	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestPickTiebreakerNodeDeterministic pins the witness-node
// selector's lowest-name-first tiebreak. Two reconcile races on the
// same RD must converge on the same answer rather than both creating
// witnesses on different nodes — otherwise auto-quorum would end up
// with a 2-witness configuration on a 2-replica RD, defeating the
// majority-of-3 invariant.
func TestPickTiebreakerNodeDeterministic(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := context.Background()

	for _, name := range []string{"n3", "n1", "n2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: name, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}

	rec := &controllerpkg.ResourceDefinitionReconciler{Store: st}

	got1, err := rec.PickTiebreakerNode(ctx, map[string]bool{"n2": true})
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}

	got2, err := rec.PickTiebreakerNode(ctx, map[string]bool{"n2": true})
	if err != nil {
		t.Fatalf("Pick (second call): %v", err)
	}

	if got1 != got2 {
		t.Errorf("non-deterministic tiebreak: first=%q second=%q", got1, got2)
	}

	// n1 < n3 lexically → n1 wins (lowest healthy non-replica).
	if got1 != "n1" {
		t.Errorf("got %q, want n1 (lowest non-replica name)", got1)
	}
}

// TestPickTiebreakerNodeNoCandidates pins the "no spare healthy
// node" surface: when every healthy satellite is already hosting a
// replica, the picker returns "" without erroring. createWitness
// uses this signal to fall back to "off" quorum rather than reject
// the RD outright.
func TestPickTiebreakerNodeNoCandidates(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := context.Background()

	for _, name := range []string{"n1", "n2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: name, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	rec := &controllerpkg.ResourceDefinitionReconciler{Store: st}

	got, err := rec.PickTiebreakerNode(ctx, map[string]bool{"n1": true, "n2": true})
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}

	if got != "" {
		t.Errorf("got %q, want empty (no spare nodes)", got)
	}
}

// TestPickTiebreakerNodeSkipsEvicted pins that the witness-picker
// honours the EVICTED/LOST drain signals. Pinning a witness on a
// dying node would defeat the auto-quorum invariant the operator
// is draining the cluster *toward*.
func TestPickTiebreakerNodeSkipsEvicted(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := context.Background()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1", Type: apiv1.NodeTypeSatellite,
		Flags: []string{apiv1.NodeFlagEvicted},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n2-healthy", Type: apiv1.NodeTypeSatellite,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	rec := &controllerpkg.ResourceDefinitionReconciler{Store: st}

	got, err := rec.PickTiebreakerNode(ctx, map[string]bool{})
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}

	if got != "n2-healthy" {
		t.Errorf("got %q, want n2-healthy (n1 is EVICTED)", got)
	}
}

// TestPickTiebreakerNodeSkipsControllerOnly pins that
// controller-only nodes (Type=CONTROLLER) are never picked as
// witnesses. They don't run a satellite agent so a DISKLESS
// resource pinned there would have no DRBD process to drive.
func TestPickTiebreakerNodeSkipsControllerOnly(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := context.Background()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "ctrl", Type: "CONTROLLER",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1", Type: apiv1.NodeTypeSatellite,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	rec := &controllerpkg.ResourceDefinitionReconciler{Store: st}

	got, err := rec.PickTiebreakerNode(ctx, map[string]bool{})
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}

	if got != "n1" {
		t.Errorf("got %q, want n1 (CONTROLLER-only must be skipped)", got)
	}
}

// TestIsDisabledNode pins the EVICTED/LOST drain-signal flag set.
// A regression that dropped one of the two flags would silently
// stop honouring an operator's drain command.
func TestIsDisabledNode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		node apiv1.Node
		want bool
	}{
		{"healthy", apiv1.Node{Name: "n", Flags: nil}, false},
		{"unrelated flag", apiv1.Node{Name: "n", Flags: []string{"OTHER"}}, false},
		{"evicted", apiv1.Node{Name: "n", Flags: []string{apiv1.NodeFlagEvicted}}, true},
		{"lost", apiv1.Node{Name: "n", Flags: []string{apiv1.NodeFlagLost}}, true},
		{"both", apiv1.Node{Name: "n", Flags: []string{apiv1.NodeFlagEvicted, apiv1.NodeFlagLost}}, true},
	}

	for _, c := range cases {
		got := controllerpkg.IsDisabledNode(&c.node)
		if got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
