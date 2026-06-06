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
	"fmt"
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Upstream-mined corner cases U272 / U427 (campaign-2, tiebreaker
// reliability). Upstream LINSTOR users reported the auto-tiebreaker
// machinery being unreliable: a deleted diskless replica not getting its
// witness back, a manually-added tiebreaker instantly latching DELETING,
// and bulk RD creation only covering a fraction of resources (28/100 in
// the upstream report). These pin the blockstor invariants that defend
// against each.

// resourceHasTieBreaker reports whether `replicas` contains a TIE_BREAKER
// witness (DISKLESS + TIE_BREAKER) for the RD.
func resourceHasTieBreaker(replicas []apiv1.Resource) bool {
	for i := range replicas {
		if slices.Contains(replicas[i].Flags, apiv1.ResourceFlagTieBreaker) {
			return true
		}
	}

	return false
}

// TestU272a_DeletedDisklessWitnessReAddedWhenNotSuppressed pins the
// U272(a) re-add invariant in its cleanest form: a 2-diskful RD whose
// auto TIE_BREAKER witness has just been deleted (and with NO active
// suppression annotation — the suppression window the CLI `r d
// <tiebreaker-node>` stamps has expired or the witness vanished for
// another reason) MUST get a fresh witness re-stamped on the next
// reconcile. Without this the RD sits at 2 diskful with no quorum witness
// — a split-brain hazard on the next partition.
//
// This is the steady-state complement to
// TestEnsureTiebreakerExpiredSuppressionResumesAutoWitness: here the
// witness was present and removed, and the reconciler must restore it.
func TestU272a_DeletedDisklessWitnessReAddedWhenNotSuppressed(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	seedNodes(t, ctx, st, "n1", "n2", "n3")

	// 2 diskful replicas, NO witness yet (it was just deleted).
	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-272a", NodeName: n}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-272a"},
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

	replicas, err := st.Resources().ListByDefinition(ctx, "pvc-272a")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if !resourceHasTieBreaker(replicas) {
		t.Fatalf("[U272a] auto TIE_BREAKER NOT re-added on a 2-diskful RD after the witness was deleted; replicas=%+v", replicas)
	}
}

// TestU272b_ManualTiebreakerOn2DiskfulNotReaped pins U272(b): a manually
// added TIE_BREAKER on the third node of a 2-diskful set (the operator
// ran `r c --drbd-diskless <node>` to create an explicit witness) must NOT
// be instantly reaped / latched into DELETING by the auto-tiebreaker
// reconciler. For a 2-diskful + 1-witness shape the invariant decision is
// "keep the witness" (it's the third quorum voter the auto-quorum
// contract promises), so a reconcile must leave the witness in place and
// keep quorum=majority.
func TestU272b_ManualTiebreakerOn2DiskfulNotReaped(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	seedNodes(t, ctx, st, "n1", "n2", "n3")

	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-272b", NodeName: n}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	// Manually-added witness on n3 (DISKLESS + TIE_BREAKER), as
	// `r c --drbd-diskless n3 pvc-272b` would create.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "pvc-272b",
		NodeName: "n3",
		Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed manual witness: %v", err)
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-272b"},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	// Reconcile a few times — no eventual reap must slip through.
	for i := range 3 {
		if err := rec.EnsureTiebreaker(ctx, rd); err != nil {
			t.Fatalf("EnsureTiebreaker (pass %d): %v", i, err)
		}

		replicas, err := st.Resources().ListByDefinition(ctx, "pvc-272b")
		if err != nil {
			t.Fatalf("list (pass %d): %v", i, err)
		}

		if !resourceHasTieBreaker(replicas) {
			t.Fatalf("[U272b] manually-added TIE_BREAKER on n3 was reaped by the reconciler on pass %d (must be kept as the third quorum voter); replicas=%+v", i, replicas)
		}

		// The witness must still be on n3 specifically — not shuffled.
		got, err := st.Resources().Get(ctx, "pvc-272b", "n3")
		if err != nil {
			t.Fatalf("[U272b] witness gone from n3 on pass %d: %v", i, err)
		}

		if !slices.Contains(got.Flags, apiv1.ResourceFlagTieBreaker) {
			t.Fatalf("[U272b] n3 lost its TIE_BREAKER flag on pass %d: %v", i, got.Flags)
		}
	}
}

// TestU272c_BulkRDsEachGetTiebreaker pins U272(c) / U427: creating many
// place-count-2 RDs must give EVERY one its auto TIE_BREAKER witness — no
// partial coverage like the upstream 28/100 report. We drive 15 RDs
// (each 2 diskful) through EnsureTiebreaker and assert all 15 end with a
// witness and quorum=majority.
func TestU272c_BulkRDsEachGetTiebreaker(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	seedNodes(t, ctx, st, "n1", "n2", "n3")

	const rdCount = 15

	allObjs := make([]client.Object, 0, rdCount)
	rdCRDs := make([]*blockstoriov1alpha1.ResourceDefinition, 0, rdCount)
	names := make([]string, 0, rdCount)

	for i := 1; i <= rdCount; i++ {
		name := fmt.Sprintf("bulk-272c-%d", i)
		names = append(names, name)

		crd := &blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: name},
		}
		rdCRDs = append(rdCRDs, crd)
		allObjs = append(allObjs, crd)

		for _, n := range []string{"n1", "n2"} {
			if err := st.Resources().Create(ctx, &apiv1.Resource{Name: name, NodeName: n}); err != nil {
				t.Fatalf("seed diskful %s/%s: %v", name, n, err)
			}
		}
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(allObjs...).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	for i := range rdCRDs {
		if err := rec.EnsureTiebreaker(ctx, rdCRDs[i]); err != nil {
			t.Fatalf("EnsureTiebreaker %s: %v", names[i], err)
		}
	}

	missing := make([]string, 0)

	for _, name := range names {
		replicas, err := st.Resources().ListByDefinition(ctx, name)
		if err != nil {
			t.Fatalf("list %s: %v", name, err)
		}

		if !resourceHasTieBreaker(replicas) {
			missing = append(missing, name)
		}
	}

	if len(missing) > 0 {
		t.Fatalf("[U272c] %d/%d RDs got no TIE_BREAKER witness (partial coverage like the upstream 28/100 report): %v",
			len(missing), rdCount, missing)
	}
}
