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
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestTiebreakerCreated: an RD with 2 diskful replicas in a 3-node
// cluster auto-gains a 3rd DISKLESS replica on the remaining node.
// Without it, a network partition would freeze the surviving replica
// because DRBD-9 can't tell majority from minority on a 1-vs-1 split.
func TestTiebreakerCreated(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	st := store.NewInMemory()

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-tb"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-tb", NodeName: n}); err != nil {
			t.Fatalf("seed replica: %v", err)
		}
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-tb"},
		}).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme, Store: st}

	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-tb"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, err := st.Resources().ListByDefinition(ctx, "pvc-tb")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("replica count: got %d, want 3 (2 diskful + 1 tiebreaker); entries=%v", len(got), got)
	}

	var tb *apiv1.Resource

	for i := range got {
		if slices.Contains(got[i].Flags, "DISKLESS") {
			tb = &got[i]

			break
		}
	}

	if tb == nil {
		t.Fatalf("no DISKLESS replica created; got %v", got)
	}

	if tb.NodeName != "n3" {
		t.Errorf("tiebreaker node: got %q, want n3 (only remaining node)", tb.NodeName)
	}

	if !slices.Contains(tb.Flags, "TIE_BREAKER") {
		t.Errorf("tiebreaker should carry TIE_BREAKER flag for cleanup tracking; got %v", tb.Flags)
	}
}

// TestTiebreakerSkipsThreeReplicas: 3 diskful replicas already give
// quorum:majority on every split, no tiebreaker needed.
func TestTiebreakerSkipsThreeReplicas(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	st := store.NewInMemory()

	for _, n := range []string{"n1", "n2", "n3", "n4"} {
		_ = st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite})
	}

	_ = st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-3rep"})

	for _, n := range []string{"n1", "n2", "n3"} {
		_ = st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-3rep", NodeName: n})
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "pvc-3rep"}}).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme, Store: st}

	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-3rep"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-3rep")
	if len(got) != 3 {
		t.Errorf("replica count drifted: got %d, want 3 (no tiebreaker on 3-replica RD)", len(got))
	}
}

// TestTiebreakerSkipsTwoNodeCluster: a 2-node cluster has no third
// node available — leave the RD as-is rather than packing the
// tiebreaker on an existing replica's node.
func TestTiebreakerSkipsTwoNodeCluster(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	st := store.NewInMemory()

	for _, n := range []string{"n1", "n2"} {
		_ = st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite})
		_ = st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-2node", NodeName: n})
	}

	_ = st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-2node"})

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "pvc-2node"}}).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme, Store: st}

	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-2node"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-2node")
	if len(got) != 2 {
		t.Errorf("replica count: got %d, want 2 (no spare node for tiebreaker)", len(got))
	}
}

// TestTiebreakerSkipsEvictedNode: the tiebreaker must never land on
// a node the operator is draining. With 3 nodes total + 1 evicted +
// 2 hosting replicas, no candidate exists; the RD stays at 2.
func TestTiebreakerSkipsEvictedNode(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	st := store.NewInMemory()

	_ = st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite})
	_ = st.Nodes().Create(ctx, &apiv1.Node{Name: "n2", Type: apiv1.NodeTypeSatellite})
	_ = st.Nodes().Create(ctx, &apiv1.Node{
		Name:  "n3",
		Type:  apiv1.NodeTypeSatellite,
		Flags: []string{apiv1.NodeFlagEvicted},
	})

	for _, n := range []string{"n1", "n2"} {
		_ = st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-evict", NodeName: n})
	}

	_ = st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-evict"})

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "pvc-evict"}}).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme, Store: st}

	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-evict"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-evict")

	for i := range got {
		if got[i].NodeName == "n3" {
			t.Errorf("tiebreaker landed on EVICTED n3: %v", got)
		}
	}
}

// TestTiebreakerEvenWithDiskless: 2 diskful + 1 user-added DISKLESS
// (no TIE_BREAKER flag) = 3 non-witness replicas (odd). Don't add a
// witness — the user's diskless replica already breaks the tie. This
// is the upstream LINSTOR semantic the user specified: parity counts
// include user-added diskless replicas, only the TIE_BREAKER flag
// distinguishes a managed witness.
func TestTiebreakerEvenWithDiskless(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	st := store.NewInMemory()

	for _, n := range []string{"n1", "n2", "n3", "n4"} {
		_ = st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite})
	}

	_ = st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-3total"})
	_ = st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-3total", NodeName: "n1"})
	_ = st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-3total", NodeName: "n2"})
	_ = st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-3total", NodeName: "n3",
		Flags: []string{apiv1.ResourceFlagDiskless}, // user-added diskless, NO TIE_BREAKER
	})

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "pvc-3total"}}).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme, Store: st}

	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-3total"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-3total")
	if len(got) != 3 {
		t.Errorf("replica count: got %d, want 3 (3 non-witness replicas, parity is odd, no witness)", len(got))
	}

	for i := range got {
		if slices.Contains(got[i].Flags, apiv1.ResourceFlagTieBreaker) {
			t.Errorf("witness created despite odd parity (3 user replicas): %v", got[i])
		}
	}
}

// TestTiebreakerEvenAfterUserAdds4: 4 user-added replicas (even) →
// witness must land. Common when an operator scales `linstor r c` up
// from 3 to 4 explicitly; without the witness the partition tolerance
// drops on a 2-2 split.
func TestTiebreakerEvenAfterUserAdds4(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	st := store.NewInMemory()

	for _, n := range []string{"n1", "n2", "n3", "n4", "n5"} {
		_ = st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite})
	}

	_ = st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-4total"})

	for _, n := range []string{"n1", "n2", "n3", "n4"} {
		_ = st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-4total", NodeName: n})
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "pvc-4total"}}).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme, Store: st}

	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-4total"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-4total")
	if len(got) != 5 {
		t.Errorf("replica count: got %d, want 5 (4 user + 1 witness)", len(got))
	}

	witnessCount := 0

	for i := range got {
		if slices.Contains(got[i].Flags, apiv1.ResourceFlagTieBreaker) {
			witnessCount++

			if got[i].NodeName != "n5" {
				t.Errorf("witness on wrong node: %s, want n5", got[i].NodeName)
			}
		}
	}

	if witnessCount != 1 {
		t.Errorf("witness count: got %d, want 1", witnessCount)
	}
}

// TestTiebreakerRemovedWhenParityFlips: a witness exists from when
// the count was even, then the user adds another replica making it
// odd. The witness must be deleted automatically — leaving it would
// be wasted network presence and (on flip-flop scaling) confuse the
// quorum maths.
func TestTiebreakerRemovedWhenParityFlips(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	st := store.NewInMemory()

	for _, n := range []string{"n1", "n2", "n3", "n4"} {
		_ = st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite})
	}

	_ = st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-flip"})
	_ = st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-flip", NodeName: "n1"})
	_ = st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-flip", NodeName: "n2"})
	_ = st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-flip", NodeName: "n3"})
	// Stale witness from when count was 2.
	_ = st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-flip", NodeName: "n4",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	})

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "pvc-flip"}}).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme, Store: st}

	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-flip"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-flip")

	if len(got) != 3 {
		t.Errorf("replica count: got %d, want 3 (witness removed when parity flipped to odd)", len(got))
	}

	for i := range got {
		if slices.Contains(got[i].Flags, apiv1.ResourceFlagTieBreaker) {
			t.Errorf("witness still present after parity flip: %v", got[i])
		}
	}
}

// TestTiebreakerSinglesAreLeftAlone: a 1-replica RD must NOT gain a
// witness. There's no quorum to defend, and adding one would create
// a 1+1 = 2 setup that DOES have quorum-loss surface (a partition
// would freeze the lone replica because it can't outvote the witness).
func TestTiebreakerSinglesAreLeftAlone(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	st := store.NewInMemory()

	for _, n := range []string{"n1", "n2", "n3"} {
		_ = st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite})
	}

	_ = st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-single"})
	_ = st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-single", NodeName: "n1"})

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(&blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "pvc-single"}}).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme, Store: st}

	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-single"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-single")
	if len(got) != 1 {
		t.Errorf("replica count: got %d, want 1 (no witness on single-replica RD)", len(got))
	}
}
