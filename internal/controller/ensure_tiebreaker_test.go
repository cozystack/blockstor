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
	"time"

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

// TestEnsureTiebreakerHonoursSuppressionAnnotation pins Bug 4:
// when the RD carries a fresh
// `blockstor.io/auto-tiebreaker-suppressed-until` annotation, the
// reconciler must NOT auto-stamp a witness. Models the operator
// workflow `linstor r d <tiebreaker-node> <rd>`: the REST handler
// stamps the annotation BEFORE deleting the replica; the next
// reconcile reads it and skips the auto-witness branch.
//
// Without this gate, the witness comes back within milliseconds of
// the operator's delete and the cluster ignores explicit intent.
func TestEnsureTiebreakerHonoursSuppressionAnnotation(t *testing.T) {
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
			Name: "pvc-suppressed", NodeName: n,
		}); err != nil {
			t.Fatalf("seed replica %s: %v", n, err)
		}
	}

	// Fresh suppression: deadline 5 minutes in the future.
	deadline := time.Now().Add(5 * time.Minute).UTC().Format(time.RFC3339)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvc-suppressed",
			Annotations: map[string]string{
				controllerpkg.AutoTiebreakerSuppressedUntilAnnotation: deadline,
			},
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

	// No witness must have landed on n3.
	if _, err := st.Resources().Get(ctx, "pvc-suppressed", "n3"); err == nil {
		t.Errorf("witness was created on n3 despite suppression annotation")
	}

	// The suppression-aware helper must agree.
	if !controllerpkg.IsTiebreakerSuppressed(rd) {
		t.Errorf("IsTiebreakerSuppressed returned false for a fresh annotation")
	}
}

// TestEnsureTiebreakerExpiredSuppressionResumesAutoWitness: once
// the suppression deadline passes, normal auto-witness behaviour
// resumes without any manual cleanup. A bad / hand-typed annotation
// must also not freeze the invariant forever — the helper treats
// unparseable values as "not suppressed".
func TestEnsureTiebreakerExpiredSuppressionResumesAutoWitness(t *testing.T) {
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
			Name: "pvc-expired", NodeName: n,
		}); err != nil {
			t.Fatalf("seed replica %s: %v", n, err)
		}
	}

	// Expired: deadline 5 minutes in the past.
	expired := time.Now().Add(-5 * time.Minute).UTC().Format(time.RFC3339)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvc-expired",
			Annotations: map[string]string{
				controllerpkg.AutoTiebreakerSuppressedUntilAnnotation: expired,
			},
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

	// Witness must have been auto-created on n3 — the expired
	// annotation does not block the normal path.
	got, err := st.Resources().Get(ctx, "pvc-expired", "n3")
	if err != nil {
		t.Fatalf("witness not created on n3 despite expired suppression: %v", err)
	}

	hasTB := false

	for _, f := range got.Flags {
		if f == apiv1.ResourceFlagTieBreaker {
			hasTB = true

			break
		}
	}

	if !hasTB {
		t.Errorf("witness on n3 lacks TIE_BREAKER flag; got %v", got.Flags)
	}

	if controllerpkg.IsTiebreakerSuppressed(rd) {
		t.Errorf("IsTiebreakerSuppressed returned true for an expired annotation")
	}

	// Hand-typed garbage must also not freeze the invariant.
	rdGarbage := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvc-junk",
			Annotations: map[string]string{
				controllerpkg.AutoTiebreakerSuppressedUntilAnnotation: "definitely not a timestamp",
			},
		},
	}

	// Use Get on final RD spec to confirm.
	final := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-expired"}, final); err != nil {
		t.Fatalf("Get RD: %v", err)
	}

	if final.Spec.Props["DrbdOptions/Resource/quorum"] != "majority" {
		t.Errorf("quorum prop: got %q, want majority (witness was created)",
			final.Spec.Props["DrbdOptions/Resource/quorum"])
	}

	if controllerpkg.IsTiebreakerSuppressed(rdGarbage) {
		t.Errorf("IsTiebreakerSuppressed returned true for unparseable annotation")
	}
}
