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
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestNoTieBreakerSpawnedForStorageOnlyRD pins Bug 334: a RD whose
// effective LayerStack carries no DRBD layer must NOT spawn a
// TIE_BREAKER witness regardless of replica count or quorum policy.
//
// Stand reproduction (with Bug 335 fix in place):
//
//	$ linstor r c test3 --auto-place=1 -l STORAGE -s stand
//	→ 1 local volume + 1 spurious TIE_BREAKER on a third node
//
// TIE_BREAKER is a DRBD-9 quorum primitive: it acts as a 1-diskless
// arbiter peer for `quorum: majority` decisions in 2-replica setups.
// Without DRBD in the LayerStack there is no quorum machinery at all
// — the witness is meaningless and the operator gets surprised by an
// unexpected third Resource CRD.
func TestNoTieBreakerSpawnedForStorageOnlyRD(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	// 3-node stand (mirrors the user-reported reproduction).
	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	// 2 diskful replicas (would normally trigger witness creation on
	// the default DRBD stack — exact same shape the Bug 334 stand
	// reproduction lands in once Bug 335's place_count gate fires
	// somewhere upstream).
	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "test3", NodeName: n,
		}); err != nil {
			t.Fatalf("seed replica %s: %v", n, err)
		}
	}

	// LayerStack=[STORAGE] — the no-DRBD shape. This is the load-
	// bearing input for Bug 334: without DRBD the witness must NOT
	// be spawned.
	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "test3"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack: []string{apiv1.LayerKindStorage},
		},
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

	got, err := st.Resources().ListByDefinition(ctx, "test3")
	if err != nil {
		t.Fatalf("list resources: %v", err)
	}

	// No witness must exist. Exactly the 2 original diskful replicas.
	if len(got) != 2 {
		t.Fatalf("Bug 334 regression: replica count drifted from 2 to %d (spurious witness?); got %+v",
			len(got), got)
	}

	for i := range got {
		if slices.Contains(got[i].Flags, apiv1.ResourceFlagTieBreaker) {
			t.Errorf("Bug 334 regression: TIE_BREAKER witness on %s for a STORAGE-only RD; flags=%v",
				got[i].NodeName, got[i].Flags)
		}

		if slices.Contains(got[i].Flags, apiv1.ResourceFlagDiskless) {
			t.Errorf("Bug 334 regression: DISKLESS replica on %s for a STORAGE-only RD; flags=%v",
				got[i].NodeName, got[i].Flags)
		}
	}
}

// TestTieBreakerStillSpawnedForDRBDStackRD is the inverse pin: with
// DRBD in the LayerStack the witness-creation invariant must still
// fire (otherwise the Bug 334 gate over-broadens and breaks the
// canonical 2-replica + 1-witness quorum shape).
func TestTieBreakerStillSpawnedForDRBDStackRD(t *testing.T) {
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
			Name: "rd-drbd", NodeName: n,
		}); err != nil {
			t.Fatalf("seed replica %s: %v", n, err)
		}
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "rd-drbd"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack: []string{apiv1.LayerKindDRBD, apiv1.LayerKindStorage},
		},
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

	got, err := st.Resources().ListByDefinition(ctx, "rd-drbd")
	if err != nil {
		t.Fatalf("list resources: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("DRBD-stack RD must still gain a witness; got %d replicas, want 3", len(got))
	}

	hasWitness := false

	for i := range got {
		if slices.Contains(got[i].Flags, apiv1.ResourceFlagTieBreaker) {
			hasWitness = true

			break
		}
	}

	if !hasWitness {
		t.Errorf("DRBD-stack RD must gain a TIE_BREAKER witness; got %+v", got)
	}
}

// TestNoTieBreakerSpawnedForEmptyLayerStackInheritsRG mirrors the
// production code path where the RD itself carries no LayerStack and
// the effective stack is inherited from a parent RG. When the parent
// RG specifies a STORAGE-only stack the witness must still be skipped.
func TestNoTieBreakerSpawnedForEmptyLayerStackInheritsRG(t *testing.T) {
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

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-storage",
		SelectFilter: apiv1.AutoSelectFilter{
			LayerStack: []string{apiv1.LayerKindStorage},
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "rd-inherit", NodeName: n,
		}); err != nil {
			t.Fatalf("seed replica %s: %v", n, err)
		}
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "rd-inherit"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: "rg-storage",
		},
	}

	rgCRD := &blockstoriov1alpha1.ResourceGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "rg-storage"},
		Spec: blockstoriov1alpha1.ResourceGroupSpec{
			SelectFilter: blockstoriov1alpha1.ResourceGroupSelectFilter{
				LayerStack: []string{apiv1.LayerKindStorage},
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd, rgCRD).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	if err := rec.EnsureTiebreaker(ctx, rd); err != nil {
		t.Fatalf("EnsureTiebreaker: %v", err)
	}

	got, err := st.Resources().ListByDefinition(ctx, "rd-inherit")
	if err != nil {
		t.Fatalf("list resources: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("Bug 334 regression on RG-inherited STORAGE-only stack: "+
			"replica count drifted from 2 to %d; got %+v", len(got), got)
	}

	for i := range got {
		if slices.Contains(got[i].Flags, apiv1.ResourceFlagTieBreaker) {
			t.Errorf("Bug 334 regression: TIE_BREAKER on RG-inherited STORAGE-only RD; got %+v", got[i])
		}
	}
}
