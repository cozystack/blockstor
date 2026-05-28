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

// TestEnsureTiebreakerHonoursAutoQuorumDisabled: scenario 7.W01
// (wave2-07-quorum-observability.md §7.W01, UG9 lines 4233-4279).
//
// When `DrbdOptions/AutoQuorum=disabled` is stamped on the RD (the
// REST POST handler folds cluster / RG-scope props onto the RD at
// create time, so this single check covers all three scopes), the
// auto-quorum reconciler must NOT overwrite the operator's manual
// `DrbdOptions/Resource/quorum`. Without this gate, every reconcile
// would revert the operator's policy to the auto-computed value the
// moment they tried to opt out.
//
// The witness invariant is independent — auto-tiebreaker still runs
// because it's gated on a separate prop (DrbdOptions/AutoAddQuorumTiebreaker).
// This test pins the quorum-only opt-out: witness creation is allowed
// (default), but quorum prop stays at the operator's chosen value.
func TestEnsureTiebreakerHonoursAutoQuorumDisabled(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	for _, n := range []string{"n1", "n2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}

		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-manual-quorum", NodeName: n,
		}); err != nil {
			t.Fatalf("seed replica %s: %v", n, err)
		}
	}

	// Operator opted out of auto-quorum and set the per-RD policy
	// explicitly. `quorum=off` + `on-no-quorum=io-error` is the
	// "scale-out fast, fail-loud" combo from UG9.
	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-manual-quorum"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Props: map[string]string{
				"DrbdOptions/AutoQuorum":            "disabled",
				"DrbdOptions/Resource/quorum":       "off",
				"DrbdOptions/Resource/on-no-quorum": "io-error",
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	// Sanity: the gate must agree with the prop.
	if !controllerpkg.IsAutoQuorumDisabled(rd) {
		t.Fatalf("IsAutoQuorumDisabled returned false for AutoQuorum=disabled RD")
	}

	if err := rec.EnsureTiebreaker(ctx, rd); err != nil {
		t.Fatalf("EnsureTiebreaker: %v", err)
	}

	final := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-manual-quorum"}, final); err != nil {
		t.Fatalf("Get RD: %v", err)
	}

	// Operator's manual value must survive: the auto reconciler
	// would otherwise have computed `majority` (2 diskful + witness)
	// and clobbered the `off`.
	if got := final.Spec.Props["DrbdOptions/Resource/quorum"]; got != "off" {
		t.Errorf("quorum prop: got %q, want %q (auto-quorum=disabled must leave manual value)",
			got, "off")
	}

	if got := final.Spec.Props["DrbdOptions/Resource/on-no-quorum"]; got != "io-error" {
		t.Errorf("on-no-quorum prop: got %q, want %q (auto-quorum=disabled must leave manual value)",
			got, "io-error")
	}

	// Auto-quorum opt-out marker must survive the round-trip
	// unchanged — a stamp-and-strip refactor would be a regression.
	if got := final.Spec.Props["DrbdOptions/AutoQuorum"]; got != "disabled" {
		t.Errorf("AutoQuorum prop: got %q, want %q (must round-trip verbatim)",
			got, "disabled")
	}
}

// TestIsAutoQuorumDisabled pins the helper across the input shapes
// the production code can encounter: nil RD, nil Props, missing key,
// explicit `disabled` (canonical and mixed case), and the two other
// accepted values (`suspend-io` / `io-error`) which are NOT disable
// signals — those tell auto-quorum which on-no-quorum to set, not
// to stop reconciling.
func TestIsAutoQuorumDisabled(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		rd   *blockstoriov1alpha1.ResourceDefinition
		want bool
	}{
		{"nil RD", nil, false},
		{"nil props", &blockstoriov1alpha1.ResourceDefinition{}, false},
		{
			name: "no AutoQuorum key",
			rd: &blockstoriov1alpha1.ResourceDefinition{
				Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
					Props: map[string]string{"other": "value"},
				},
			},
			want: false,
		},
		{
			name: "disabled (canonical)",
			rd: &blockstoriov1alpha1.ResourceDefinition{
				Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
					Props: map[string]string{"DrbdOptions/AutoQuorum": "disabled"},
				},
			},
			want: true,
		},
		{
			name: "Disabled (mixed case from manual paste)",
			rd: &blockstoriov1alpha1.ResourceDefinition{
				Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
					Props: map[string]string{"DrbdOptions/AutoQuorum": "Disabled"},
				},
			},
			want: true,
		},
		{
			name: "suspend-io (auto-set instruction, not disable)",
			rd: &blockstoriov1alpha1.ResourceDefinition{
				Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
					Props: map[string]string{"DrbdOptions/AutoQuorum": "suspend-io"},
				},
			},
			want: false,
		},
		{
			name: "io-error (auto-set instruction, not disable)",
			rd: &blockstoriov1alpha1.ResourceDefinition{
				Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
					Props: map[string]string{"DrbdOptions/AutoQuorum": "io-error"},
				},
			},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := controllerpkg.IsAutoQuorumDisabled(tc.rd); got != tc.want {
				t.Errorf("IsAutoQuorumDisabled = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestEnsureTiebreakerPreservedAfterToggleDiskful2Diskless pins Bug
// 104. Starting from the steady state the auto-witness path creates
// (2 diskful + 1 TIE_BREAKER), an operator toggles one diskful to
// DISKLESS via `linstor r td --diskless`. The pre-Bug-104 invariant
// recomputed wantWitness from scratch and saw "1 diskful, 1
// non-witness diskless" — flipping the decision to "no witness
// needed" and DELETING the TIE_BREAKER. That collapses the cluster
// to 1 diskful + 1 diskless with no third voter, so the next
// network partition freezes the volume read-only (UG9 §"Quorum"
// failure-mode 2). The fix keeps the witness whenever it already
// exists and diskful is in [1, 3): the cluster needs the witness
// MORE in that window, not less.
func TestEnsureTiebreakerPreservedAfterToggleDiskful2Diskless(t *testing.T) {
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

	// Steady state after auto-witness placement: n1 + n2 diskful,
	// n3 carries the auto-stamped TIE_BREAKER witness.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-bug104", NodeName: "n1",
	}); err != nil {
		t.Fatalf("seed n1: %v", err)
	}

	// Operator toggled the diskful on n1 to diskless (the
	// observable effect of `linstor r td --diskless n1 pvc-bug104`,
	// which is the only path the REST layer wires today — see
	// handleResourceToggleDiskToDiskless in
	// pkg/rest/resource_toggle_disk.go).
	if err := st.Resources().Update(ctx, &apiv1.Resource{
		Name: "pvc-bug104", NodeName: "n1",
		Flags: []string{apiv1.ResourceFlagDiskless},
	}); err != nil {
		t.Fatalf("toggle n1 to diskless: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-bug104", NodeName: "n2",
	}); err != nil {
		t.Fatalf("seed n2: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-bug104", NodeName: "n3",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed n3 witness: %v", err)
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-bug104"},
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

	// Bug 104 invariant: all three Resources MUST still exist.
	// Pre-fix, n3 (TIE_BREAKER) got removed by applyWitnessDecision.
	all, err := st.Resources().ListByDefinition(ctx, "pvc-bug104")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(all) != 3 {
		t.Fatalf("replica count: got %d, want 3 (1 diskful + 1 diskless + 1 TIE_BREAKER); entries=%v",
			len(all), all)
	}

	witnessCount := 0
	disklessCount := 0
	diskfulCount := 0

	for i := range all {
		hasDiskless := false
		hasTB := false

		for _, f := range all[i].Flags {
			if f == apiv1.ResourceFlagDiskless {
				hasDiskless = true
			}

			if f == apiv1.ResourceFlagTieBreaker {
				hasTB = true
			}
		}

		switch {
		case hasTB:
			witnessCount++
		case hasDiskless:
			disklessCount++
		default:
			diskfulCount++
		}
	}

	if diskfulCount != 1 || disklessCount != 1 || witnessCount != 1 {
		t.Errorf("post-toggle composition: diskful=%d diskless=%d witness=%d, want 1/1/1; entries=%v",
			diskfulCount, disklessCount, witnessCount, all)
	}

	// Quorum prop must remain "majority": diskful=1 + diskless=2
	// (1 user-diskless + 1 witness) still satisfies the
	// `(diskful == 2 AND diskless ≥ 1) OR diskful ≥ 3` upstream
	// rule? No — diskful=1 + diskless=2 falls into the "off" branch
	// of quorumPolicy. The witness preservation is about not making
	// the situation WORSE: with the witness gone we'd have
	// diskful=1+diskless=1=2, still "off", but the operator can
	// recover by re-toggling. Without the witness, re-toggling
	// gives 2 diskful + 1 user-diskless and quorumPolicy returns
	// "majority" — but during the partition-vulnerable window the
	// witness was still useful as a connection-mesh participant.
	// We do not pin a specific quorum prop value here because the
	// 1-diskful state is intentionally a transient operator
	// workflow, not steady state.
}

// TestBug108EnsureTiebreakerFullSequenceAfterToggle reproduces the
// EXACT production sequence reported in bug-hunt v2 for Bug 108:
//
//  1. `rd ap --place-count 2` lands 2 diskful replicas; the RD
//     reconciler runs `EnsureTiebreaker` and AUTO-CREATES the
//     TIE_BREAKER witness on the third node (so we don't pre-seed
//     n3 — the reconciler picks it).
//  2. `r td --diskless dev-kvaps-worker-1 <rd>` updates the n1
//     replica spec to add the DISKLESS flag (the only thing
//     handleResourceToggleDiskToDiskless does — see
//     pkg/rest/resource_toggle_disk.go).
//  3. The Resource Update event fires the RD-reconciler watch.
//     `EnsureTiebreaker` runs a SECOND time and must NOT drop the
//     auto-stamped witness.
//
// Unlike TestEnsureTiebreakerPreservedAfterToggleDiskful2Diskless
// (which pre-seeds the witness with the TIE_BREAKER flag), this
// test verifies the witness survives across the
// create-then-evaluate cycle the auto-place flow actually
// exercises in production. This is the no-race path: the witness
// IS stamped before the toggle fires.
func TestBug108EnsureTiebreakerFullSequenceAfterToggle(t *testing.T) {
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

	// Step 1a: auto-place lands 2 diskful (n1, n2).
	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "poke108e", NodeName: n,
		}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "poke108e"},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	// Step 1b: first reconcile creates the witness on the unused node.
	if err := rec.EnsureTiebreaker(ctx, rd); err != nil {
		t.Fatalf("EnsureTiebreaker (step 1): %v", err)
	}

	pre, err := st.Resources().ListByDefinition(ctx, "poke108e")
	if err != nil {
		t.Fatalf("list pre-toggle: %v", err)
	}

	if len(pre) != 3 {
		t.Fatalf("pre-toggle: got %d replicas, want 3 (2 diskful + 1 TB); entries=%v",
			len(pre), pre)
	}

	// Identify the witness node so we can verify it survives.
	witnessNode := ""

	for i := range pre {
		hasTB := false

		for _, f := range pre[i].Flags {
			if f == apiv1.ResourceFlagTieBreaker {
				hasTB = true
			}
		}

		if hasTB {
			witnessNode = pre[i].NodeName
		}
	}

	if witnessNode == "" {
		t.Fatalf("pre-toggle: no TIE_BREAKER found in %v", pre)
	}

	// Step 2: toggle n1 to diskless — what handleResourceToggleDiskToDiskless does.
	if err := st.Resources().Update(ctx, &apiv1.Resource{
		Name: "poke108e", NodeName: "n1",
		Flags: []string{apiv1.ResourceFlagDiskless},
	}); err != nil {
		t.Fatalf("toggle n1 to diskless: %v", err)
	}

	// Refresh the RD spec from the fake client — setQuorum may have
	// mutated it during step 1, and production runs hit a fresh Get
	// at the top of every Reconcile.
	if err := cli.Get(ctx, types.NamespacedName{Name: "poke108e"}, rd); err != nil {
		t.Fatalf("refresh rd: %v", err)
	}

	// Step 3: Resource Update event triggers a second reconcile.
	if err := rec.EnsureTiebreaker(ctx, rd); err != nil {
		t.Fatalf("EnsureTiebreaker (step 3): %v", err)
	}

	post, err := st.Resources().ListByDefinition(ctx, "poke108e")
	if err != nil {
		t.Fatalf("list post-toggle: %v", err)
	}

	// Bug 108 invariant: TIE_BREAKER on witnessNode MUST survive.
	if len(post) != 3 {
		t.Fatalf("post-toggle: got %d replicas, want 3; entries=%v", len(post), post)
	}

	witnessSurvived := false

	for i := range post {
		if post[i].NodeName != witnessNode {
			continue
		}

		for _, f := range post[i].Flags {
			if f == apiv1.ResourceFlagTieBreaker {
				witnessSurvived = true
			}
		}
	}

	if !witnessSurvived {
		t.Fatalf("Bug 108: TIE_BREAKER on %s reaped after toggle; post=%v",
			witnessNode, post)
	}
}

// TestBug108EnsureTiebreakerToggleBeforeWitnessLands pins the EXACT
// regression the bug-hunt v2 agent reported (3/3 repros):
//
//  1. `rd c <rd>; vd c <rd> 32M; rd ap --place-count 2` posts the
//     two diskful replicas. The RD reconciler is enqueued but the
//     witness-creation step hasn't run yet (or just started).
//  2. `r td --diskless worker-1 <rd>` lands BEFORE the witness
//     Resource hits the apiserver. Toggle handler updates n1 →
//     Resource Update event fires the RD watch.
//  3. The (now-final) reconcile sees 1 diskful + 1 user-diskless +
//     0 witness. Bug 104's keep-branch only preserves an EXISTING
//     witness; with none present, both branches gate to false and
//     `wantWitness=false`. Final state: 2 replicas, no witness.
//
// Bug 108's invariant is "TIE_BREAKER survives the toggle, full
// stop" — that has to extend to "a witness is created when the
// post-toggle state needs one, even if the steady-state precursor
// reconcile never landed it". Without this, an unlucky timing
// permanently kills the witness; subsequent reconciles see "1
// diskful + 1 diskless" and stay in the no-witness branch forever.
// Mirrors the v2 report's curl observation: `len(resources) == 2`.
func TestBug108EnsureTiebreakerToggleBeforeWitnessLands(t *testing.T) {
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

	// Steady state just after `rd ap --place-count 2`: 2 diskful
	// replicas land. The witness reconcile hasn't run yet — this
	// IS the race the bug-hunt v2 agent hit. No TIE_BREAKER on n3.
	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "poke108e", NodeName: n,
		}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	// Operator fires `r td --diskless n1 poke108e` BEFORE the RD
	// reconciler runs (cache-trail / queue-drain race). Toggle
	// handler only flips the DISKLESS flag — see
	// handleResourceToggleDiskToDiskless in resource_toggle_disk.go.
	if err := st.Resources().Update(ctx, &apiv1.Resource{
		Name: "poke108e", NodeName: "n1",
		Flags: []string{apiv1.ResourceFlagDiskless},
	}); err != nil {
		t.Fatalf("toggle n1 to diskless: %v", err)
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "poke108e"},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	// RD-reconciler drains its work queue and runs (post-toggle
	// view). Bug 104's keep-branch can't help — no witness was
	// ever stamped. The fix must extend wantWitness to cover this
	// transient case.
	if err := rec.EnsureTiebreaker(ctx, rd); err != nil {
		t.Fatalf("EnsureTiebreaker: %v", err)
	}

	post, err := st.Resources().ListByDefinition(ctx, "poke108e")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	// Bug 108 invariant from v2 report: a TIE_BREAKER witness MUST
	// land on the unused node (n3). Pre-fix the controller settled
	// at 2 replicas with no witness — len==2 in the v2 curl trace.
	if len(post) != 3 {
		t.Fatalf("Bug 108: post-toggle replica count = %d, want 3 "+
			"(1 diskful + 1 user-diskless + 1 auto-witness); entries=%v",
			len(post), post)
	}

	witnessCount := 0

	for i := range post {
		for _, f := range post[i].Flags {
			if f == apiv1.ResourceFlagTieBreaker {
				witnessCount++
			}
		}
	}

	if witnessCount != 1 {
		t.Fatalf("Bug 108: TIE_BREAKER count = %d, want 1; entries=%v",
			witnessCount, post)
	}
}

// TestBug338TiebreakerCollapsesWhenDiskfulDropsToOne pins Bug 338:
// stand-observed, user-reported. Starting from the steady state the
// auto-witness path creates (2 diskful + 1 TIE_BREAKER), an operator
// runs `linstor r d <one-of-diskful-nodes> <rd>` — a real delete,
// not a toggle. The diskful Resource on that node is removed; the
// reconciler MUST tear down the now-pointless TIE_BREAKER too.
//
// Pre-fix, Bug 104's keep-branch fires unconditionally for the
// (diskful=1, witness present) shape and PRESERVES the witness.
// That leaves the cluster at 1 diskful + 1 tiebreaker — a 2-voter
// quorum with no real majority. The tiebreaker is now meaningless
// (no peer to arbitrate between) and just noise on `linstor r l`.
//
// Per upstream LINSTOR CtrlAutoQuorumTask: when diskful count drops
// below 2 AND there is no user-added diskless that could need a
// witness to break a tie, tear down the tiebreaker. The single
// remaining diskful runs with quorum=off (no peer to lose).
//
// Distinct from Bug 104 (toggle, NOT delete): Bug 104's path leaves
// a non-witness diskless behind (1 diskful + 1 user-diskless +
// 1 witness). The witness still has work to do as a third voter
// there. Bug 338 has no user-diskless — only the lone diskful and
// the now-orphaned witness.
func TestBug338TiebreakerCollapsesWhenDiskfulDropsToOne(t *testing.T) {
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

	// Steady state: n1 + n2 diskful, n3 TIE_BREAKER. This is what
	// the production stand reported before the user ran `linstor r d`.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-bug338", NodeName: "n2",
	}); err != nil {
		t.Fatalf("seed n2 diskful: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-bug338", NodeName: "n3",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed n3 witness: %v", err)
	}

	// User just ran `linstor r d n1 pvc-bug338`: the REST handler
	// (or the cascade) already removed the n1 Resource from the
	// store. The reconciler is now firing on the resulting state.
	// Snapshot: n2 diskful + n3 tiebreaker.
	//
	// This is a GENUINE Bug-338 standalone (operator deleted a diskful
	// and stopped), not the transient relocate-onto-the-tiebreaker
	// window. Pre-stamp the orphan-witness grace with a timestamp
	// safely older than orphanWitnessCollapseGrace so the collapse
	// fires this reconcile rather than being deferred. (A fresh
	// transient — no stamp — is exercised by
	// TestEnsureTiebreakerRelocateDefersOrphanCollapse.)
	seenAt := time.Now().Add(-2 * controllerpkg.OrphanWitnessCollapseGrace).UTC().Format(time.RFC3339)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "pvc-bug338",
			Annotations: map[string]string{controllerpkg.OrphanWitnessSeenAtAnnotation: seenAt},
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

	// Bug 338 invariant: the tiebreaker must be gone. Only the lone
	// diskful Resource on n2 remains.
	post, err := st.Resources().ListByDefinition(ctx, "pvc-bug338")
	if err != nil {
		t.Fatalf("list post-delete: %v", err)
	}

	if len(post) != 1 {
		t.Fatalf("Bug 338: replica count = %d, want 1 (single diskful, no orphaned witness); entries=%v",
			len(post), post)
	}

	if post[0].NodeName != "n2" {
		t.Errorf("Bug 338: surviving replica on %s, want n2; entries=%v",
			post[0].NodeName, post)
	}

	for _, f := range post[0].Flags {
		if f == apiv1.ResourceFlagTieBreaker {
			t.Errorf("Bug 338: surviving replica still carries TIE_BREAKER flag; flags=%v",
				post[0].Flags)
		}

		if f == apiv1.ResourceFlagDiskless {
			t.Errorf("Bug 338: surviving replica should be diskful, has DISKLESS; flags=%v",
				post[0].Flags)
		}
	}
}

// TestEnsureTiebreakerRelocateDefersOrphanCollapse pins the
// relocate-onto-the-tiebreaker stability gate. A FRESH 1-diskful +
// 1-orphan-witness snapshot (no grace stamp yet) is the transient
// mid-relocate shape: after `r d <one-of-two-diskful>` the RD sits at
// 1 diskful + 1 orphan witness for the sub-second window before
// `r c <tiebreaker-node>` promotes the witness into the relocate
// target. The reconciler MUST NOT collapse the witness on this first
// observation — it must defer (keep the witness, stamp the seen-at
// grace) so the in-flight promote isn't raced and the placement
// decision doesn't oscillate.
//
// Contrast TestBug338TiebreakerCollapsesWhenDiskfulDropsToOne, which
// pre-stamps a past-grace timestamp to exercise the genuine
// standalone (operator deleted a diskful and stopped) where the
// collapse SHOULD fire.
func TestEnsureTiebreakerRelocateDefersOrphanCollapse(t *testing.T) {
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

	const rdName = "pvc-relocate-defer"

	// Transient mid-relocate snapshot: lone diskful on n2 + orphan
	// witness on n3, NO grace stamp (this is the first observation).
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: rdName, NodeName: "n2",
	}); err != nil {
		t.Fatalf("seed n2 diskful: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: rdName, NodeName: "n3",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed n3 witness: %v", err)
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
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

	// Deferral invariant: the witness MUST still be present (collapse
	// deferred), and the grace stamp MUST now be set so the next
	// reconcile after the grace can act.
	if _, err := st.Resources().Get(ctx, rdName, "n3"); err != nil {
		t.Fatalf("relocate-defer: witness on n3 was collapsed on first observation: %v", err)
	}

	final := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(ctx, types.NamespacedName{Name: rdName}, final); err != nil {
		t.Fatalf("Get RD: %v", err)
	}

	if final.Annotations[controllerpkg.OrphanWitnessSeenAtAnnotation] == "" {
		t.Errorf("relocate-defer: orphan-witness grace stamp not set; annotations=%v", final.Annotations)
	}

	// Now simulate the relocate `r c n3` landing inside the grace:
	// promote n3's witness to diskful. The next reconcile sees 2
	// diskful + 0 witness, must CLEAR the grace stamp and want a fresh
	// witness (no orphan-collapse, no flip).
	if err := st.Resources().Update(ctx, &apiv1.Resource{
		Name: rdName, NodeName: "n3",
		Props: map[string]string{"StorPoolName": "stand"},
	}); err != nil {
		t.Fatalf("promote n3 witness to diskful: %v", err)
	}

	if err := rec.EnsureTiebreaker(ctx, final); err != nil {
		t.Fatalf("EnsureTiebreaker after promote: %v", err)
	}

	cleared := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(ctx, types.NamespacedName{Name: rdName}, cleared); err != nil {
		t.Fatalf("Get RD after promote: %v", err)
	}

	if cleared.Annotations[controllerpkg.OrphanWitnessSeenAtAnnotation] != "" {
		t.Errorf("relocate-defer: grace stamp not cleared after relocate landed; annotations=%v",
			cleared.Annotations)
	}

	all, err := st.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		t.Fatalf("list after promote: %v", err)
	}

	diskful, _, witness := classifyReplicas(all)
	if diskful != 2 {
		t.Errorf("after relocate: diskful=%d, want 2 (n2 + relocated n3); entries=%v", diskful, all)
	}

	if witness != 1 {
		t.Errorf("after relocate: witness=%d, want 1 (fresh on n1); entries=%v", witness, all)
	}
}

// classifyReplicas counts (diskful, plainDiskless, witness) over a
// replica slice using the same flag rules ensureTiebreaker applies.
func classifyReplicas(replicas []apiv1.Resource) (int, int, int) {
	var diskful, diskless, witness int

	for i := range replicas {
		hasDiskless := false
		hasTB := false

		for _, f := range replicas[i].Flags {
			switch f {
			case apiv1.ResourceFlagDiskless:
				hasDiskless = true
			case apiv1.ResourceFlagTieBreaker:
				hasTB = true
			}
		}

		switch {
		case hasTB:
			witness++
		case hasDiskless:
			diskless++
		default:
			diskful++
		}
	}

	return diskful, diskless, witness
}

// TestEnsureTiebreakerRelocateOntoTiebreakerConverges reproduces the
// `r-full-lifecycle.sh` Phase-3 relocate-onto-the-tiebreaker
// oscillation under physical-delete semantics.
//
// Stand sequence (3 workers): start 2 diskful (n1, n2) + 1 TIE_BREAKER
// (n3). `r d n2` physically removes n2's diskful, leaving 1 diskful +
// 1 orphan witness — the Bug-338 shape. Then `r c n3` (the only worker
// that is neither the survivor nor the just-freed node) lands on the
// tiebreaker's own node: promoteDisklessReplica strips TIE_BREAKER +
// DISKLESS from the n3 row in-place and stamps StorPoolName, turning
// the witness INTO the diskful relocate target on the same (rd, node)
// key.
//
// Pre-fix the Bug-338 orphan-collapse path (removeWitnesses) deleted
// the witness on n3 by node key from a snapshot taken before the
// promote. Racing the promote, that Delete wiped the freshly-promoted
// relocate target, the topology reset to 1 diskful, the next reconcile
// re-evaluated, and the diskful count flip-flopped 1↔2 (willRemove ↔
// willCreate) — the relocate target never stabilized.
//
// This test drives the destructive overlap directly: it promotes n3's
// witness in-place, then runs EnsureTiebreaker repeatedly with the
// PRE-promote witness still in the snapshot path. The invariants:
//
//  1. The promoted relocate target on n3 MUST survive — never reaped
//     as an orphan witness.
//  2. The reconcile converges to a single stable shape (2 diskful +
//     exactly 1 witness on the freed node) and stays there across
//     repeated reconciles — no create/remove flip.
func TestEnsureTiebreakerRelocateOntoTiebreakerConverges(t *testing.T) {
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

	const rdName = "pvc-relocate-tb"

	// Post-`r d n2` state: lone diskful on n1 + orphan witness on n3.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: rdName, NodeName: "n1",
	}); err != nil {
		t.Fatalf("seed n1 diskful: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: rdName, NodeName: "n3",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed n3 witness: %v", err)
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	// The destructive race: ensureTiebreaker snapshots the topology
	// while n3 is STILL the orphan witness (diskful=1 → Bug-338
	// collapse decision = willRemove, witness snapshot = [n3]). Model
	// that stale snapshot explicitly so removeWitnesses is exercised
	// against a row that gets promoted out from under it.
	staleWitnessSnapshot := []apiv1.Resource{
		{Name: rdName, NodeName: "n3", Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker}},
	}

	// `r c n3`: promote the witness on n3 to the diskful relocate
	// target in-place — TIE_BREAKER + DISKLESS stripped, StorPoolName
	// stamped on the SAME (rd, node) row (promoteDisklessReplica).
	if err := st.Resources().Update(ctx, &apiv1.Resource{
		Name: rdName, NodeName: "n3",
		Props: map[string]string{"StorPoolName": "stand"},
	}); err != nil {
		t.Fatalf("promote n3 witness to diskful: %v", err)
	}

	// Bug-338 collapse fires with the stale snapshot AFTER the promote
	// landed. removeWitnesses must NOT delete the now-diskful n3 row.
	if err := rec.RemoveWitnesses(ctx, rdName, staleWitnessSnapshot); err != nil {
		t.Fatalf("RemoveWitnesses with stale snapshot: %v", err)
	}

	if _, err := st.Resources().Get(ctx, rdName, "n3"); err != nil {
		t.Fatalf("destructive race: removeWitnesses reaped the promoted relocate target on n3: %v", err)
	}

	// Drive several reconciles. They must converge and stay idempotent.
	for pass := range 4 {
		if err := rec.EnsureTiebreaker(ctx, rd); err != nil {
			t.Fatalf("EnsureTiebreaker pass %d: %v", pass, err)
		}

		all, err := st.Resources().ListByDefinition(ctx, rdName)
		if err != nil {
			t.Fatalf("list pass %d: %v", pass, err)
		}

		// Invariant 1: the relocate target on n3 must survive as a
		// diskful replica, never reaped as an orphan witness.
		n3, getErr := st.Resources().Get(ctx, rdName, "n3")
		if getErr != nil {
			t.Fatalf("pass %d: relocate target on n3 was reaped: %v", pass, getErr)
		}

		for _, f := range n3.Flags {
			if f == apiv1.ResourceFlagTieBreaker {
				t.Fatalf("pass %d: relocate target on n3 still flagged TIE_BREAKER; flags=%v",
					pass, n3.Flags)
			}
		}

		diskful, diskless, witness := classifyReplicas(all)

		// Invariant 2: converged shape is 2 diskful + exactly 1
		// witness (on the freed n2) + no plain diskless. The diskful
		// count must be a stable 2 — not flipping to 1.
		if diskful != 2 {
			t.Errorf("pass %d: diskful=%d, want 2 (n1 + relocated n3); entries=%v",
				pass, diskful, all)
		}

		if witness != 1 {
			t.Errorf("pass %d: witness=%d, want exactly 1 (no create/remove flip); entries=%v",
				pass, witness, all)
		}

		if diskless != 0 {
			t.Errorf("pass %d: plain diskless=%d, want 0; entries=%v",
				pass, diskless, all)
		}
	}

	// The witness must have landed on the freed n2 (the only spare
	// node), not back on n3 (the diskful relocate target).
	if _, err := st.Resources().Get(ctx, rdName, "n2"); err != nil {
		t.Errorf("witness should land on freed n2; got error %v", err)
	}
}
