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
	"errors"
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// withKernelPeers gives a replica the per-peer connection map the k8s
// store builds from its Status.Connections.
func withKernelPeers(res *apiv1.Resource, peers ...string) {
	conns := make(map[string]apiv1.DrbdConnection, len(peers))
	for _, p := range peers {
		conns[p] = apiv1.DrbdConnection{}
	}

	res.LayerObject = &apiv1.ResourceLayer{
		Type: apiv1.LayerKindDRBD,
		Drbd: &apiv1.DrbdResourceLayer{Connections: conns},
	}
}

// A diskful replica deleted out of three leaves two diskful and calls
// for a witness. The only spare is the node that just lost its replica,
// and the survivors' kernels still hold their connection to it. The
// witness waits until they have dropped it: placed earlier, it comes up
// with a new node-id and every survivor's new-peer fails with "name
// already exists" against the old connection.
func TestEnsureTiebreakerWaitsForDepartedPeerToLeaveTheKernel(t *testing.T) {
	t.Parallel()

	const rdName = "pvc-departed-peer"

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	seedNodes(t, ctx, st, "n1", "n2", "n3")

	for _, n := range []string{"n1", "n2"} {
		res := apiv1.Resource{Name: rdName, NodeName: n}
		withKernelPeers(&res, "n1", "n2", "n3")

		if err := st.Resources().Create(ctx, &res); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: rdName}}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()
	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme, Store: st}

	if err := rec.EnsureTiebreaker(ctx, rd); err != nil {
		t.Fatalf("EnsureTiebreaker: %v", err)
	}

	if _, err := st.Resources().Get(ctx, rdName, "n3"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("witness placed on n3 while survivors still hold its old connection (err=%v)", err)
	}

	// n2 has run del-peer; n1 has not yet.
	n2, err := st.Resources().Get(ctx, rdName, "n2")
	if err != nil {
		t.Fatalf("get n2: %v", err)
	}

	withKernelPeers(&n2, "n1")

	if err := st.Resources().Update(ctx, &n2); err != nil {
		t.Fatalf("update n2: %v", err)
	}

	if err := rec.EnsureTiebreaker(ctx, rd); err != nil {
		t.Fatalf("EnsureTiebreaker: %v", err)
	}

	if _, err := st.Resources().Get(ctx, rdName, "n3"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("witness placed on n3 while n1 still holds its old connection (err=%v)", err)
	}

	n1, err := st.Resources().Get(ctx, rdName, "n1")
	if err != nil {
		t.Fatalf("get n1: %v", err)
	}

	withKernelPeers(&n1, "n2")

	if err := st.Resources().Update(ctx, &n1); err != nil {
		t.Fatalf("update n1: %v", err)
	}

	if err := rec.EnsureTiebreaker(ctx, rd); err != nil {
		t.Fatalf("EnsureTiebreaker: %v", err)
	}

	got, err := st.Resources().Get(ctx, rdName, "n3")
	if err != nil {
		t.Fatalf("witness not placed on n3 once the old connection is gone: %v", err)
	}

	if !slices.Contains(got.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("n3 replica must carry TIE_BREAKER; got %v", got.Flags)
	}
}

// A definition flagged DELETE gets no witness: upstream never runs its
// auto-tiebreaker helper on a definition being deleted.
func TestEnsureTiebreakerSkipsDefinitionFlaggedDelete(t *testing.T) {
	t.Parallel()

	const rdName = "pvc-flagged-delete"

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	seedNodes(t, ctx, st, "n1", "n2", "n3")

	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{Name: rdName, NodeName: n}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Flags: []string{apiv1.ResourceFlagDelete},
		},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()
	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme, Store: st}

	if err := rec.EnsureTiebreaker(ctx, rd); err != nil {
		t.Fatalf("EnsureTiebreaker: %v", err)
	}

	if _, err := st.Resources().Get(ctx, rdName, "n3"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("witness placed for a definition flagged DELETE (err=%v)", err)
	}
}
