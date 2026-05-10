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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestEnsureTiebreakerCreatesWitnessOn2Replicas pins the
// auto-add-witness branch of ensureTiebreaker (was 81.8%): a 2-
// replica RD with auto-tiebreaker enabled (default) and no
// existing witness must:
//
//  1. Create a TIE_BREAKER replica on a healthy non-replica node.
//  2. Set the RD's quorum prop to "majority".
//
// Pinned so a regression that flipped either step would silently
// drop the auto-quorum invariant: a 2-replica partition without
// witness can't make progress under split-brain.
func TestEnsureTiebreakerCreatesWitnessOn2Replicas(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-quorum", NodeName: n,
		}); err != nil {
			t.Fatalf("seed replica %s: %v", n, err)
		}
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-quorum"},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	if err := rec.EnsureTiebreaker(ctx, rd); err != nil {
		t.Fatalf("EnsureTiebreaker: %v", err)
	}

	// Witness landed on n3 (lowest non-replica name).
	got, err := st.Resources().Get(ctx, "pvc-quorum", "n3")
	if err != nil {
		t.Fatalf("witness not created on n3: %v", err)
	}

	hasTB := false

	for _, f := range got.Flags {
		if f == apiv1.ResourceFlagTieBreaker {
			hasTB = true

			break
		}
	}

	if !hasTB {
		t.Errorf("witness must carry TIE_BREAKER flag; got %v", got.Flags)
	}

	// quorum prop must be "majority" — 2 diskful + 1 witness → majority feasible.
	final := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-quorum"}, final); err != nil {
		t.Fatalf("Get RD: %v", err)
	}

	if final.Spec.Props["DrbdOptions/Resource/quorum"] != "majority" {
		t.Errorf("quorum prop: got %q, want majority",
			final.Spec.Props["DrbdOptions/Resource/quorum"])
	}
}

// TestEnsureTiebreakerOffOnSingleReplica pins the quorum-off
// surface for a 1-replica RD: no auto-witness, quorum prop set to
// "off". A single-replica resource fundamentally can't have
// majority, so the controller writes "off" so the satellite's
// drbd config matches reality (avoids drbd-9 panicking on
// "quorum:majority" with insufficient peers).
func TestEnsureTiebreakerOffOnSingleReplica(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1", Type: apiv1.NodeTypeSatellite,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-solo", NodeName: "n1",
	}); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-solo"},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	if err := rec.EnsureTiebreaker(ctx, rd); err != nil {
		t.Fatalf("EnsureTiebreaker: %v", err)
	}

	final := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-solo"}, final); err != nil {
		t.Fatalf("Get RD: %v", err)
	}

	if final.Spec.Props["DrbdOptions/Resource/quorum"] != "off" {
		t.Errorf("quorum prop: got %q, want off (1-replica RD)",
			final.Spec.Props["DrbdOptions/Resource/quorum"])
	}

	// No witness should have been auto-added on a 1-replica RD.
	for _, n := range []string{"n2", "n3"} {
		if _, err := st.Resources().Get(ctx, "pvc-solo", n); err == nil {
			t.Errorf("unexpected witness on %s for 1-replica RD", n)
		}
	}
}
