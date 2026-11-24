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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// The RD reconciler came back to every definition every 5 s, settled or
// not: an uncached read of the RD, its replicas, its group and the
// nodes each time. On a cluster with a few hundred definitions that
// kept the controller near a core while nothing changed. A settled RD
// now waits for its next event; only a deferred decision or a witness
// deadline brings it back on its own.

// tiebreakerFixture is a 2-diskful RD on n1 and n2 with n3 spare, whose
// node list can still serve a node that is gone.
func tiebreakerFixture(t *testing.T, annotations map[string]string) (*controllerpkg.ResourceDefinitionReconciler, store.Store, *bool) {
	t.Helper()

	ctx := context.Background()
	inner := store.NewInMemory()

	for _, n := range []string{"n1", "n2", "n3"} {
		err := inner.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite})
		if err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	for _, n := range []string{"n1", "n2"} {
		err := inner.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-settle", NodeName: n})
		if err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	stale := false
	st := &staleListStore{Store: inner, stale: &stale, ghost: apiv1.Node{Name: "a-lost", Type: apiv1.NodeTypeSatellite}}

	scheme := newScheme(t)
	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-settle", Annotations: annotations},
	}

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build(),
		Scheme: scheme,
		Store:  st,
	}

	return rec, inner, &stale
}

func reconcileSettle(t *testing.T, rec *controllerpkg.ResourceDefinitionReconciler) ctrl.Result {
	t.Helper()

	result, err := rec.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-settle"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	return result
}

func TestSettledRDAsksForNoRequeue(t *testing.T) {
	t.Parallel()

	rec, inner, _ := tiebreakerFixture(t, nil)

	reconcileSettle(t, rec)

	witness, err := inner.Resources().Get(context.Background(), "pvc-settle", "n3")
	if err != nil || !slices.Contains(witness.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Fatalf("no witness on n3 after the first pass: %v %v", witness, err)
	}

	if result := reconcileSettle(t, rec); result.RequeueAfter != 0 {
		t.Errorf("a settled RD asked to come back after %v", result.RequeueAfter)
	}
}

// A witness create deferred because its candidate vanished has no event
// to bring it back: the pass asks for the next one itself.
func TestDeferredWitnessComesBack(t *testing.T) {
	t.Parallel()

	rec, inner, stale := tiebreakerFixture(t, nil)
	*stale = true

	if result := reconcileSettle(t, rec); result.RequeueAfter != controllerpkg.RDReconcileRequeue {
		t.Fatalf("a deferred witness create came back after %v, want %v", result.RequeueAfter, controllerpkg.RDReconcileRequeue)
	}

	*stale = false

	if result := reconcileSettle(t, rec); result.RequeueAfter != 0 {
		t.Errorf("the pass that placed the witness asked to come back after %v", result.RequeueAfter)
	}

	_, err := inner.Resources().Get(context.Background(), "pvc-settle", "n3")
	if errors.Is(err, store.ErrNotFound) {
		t.Fatal("the witness never landed on n3")
	}
}

// The witness suppression after an operator deleted the witness ends at
// a deadline no event marks; the RD comes back when it passes.
func TestRDComesBackWhenAWitnessDeadlinePasses(t *testing.T) {
	t.Parallel()

	const left = 30 * time.Second

	for _, key := range []string{controllerpkg.AutoTiebreakerSuppressedUntilAnnotation, controllerpkg.KeepTiebreakerUntilAnnotation} {
		deadline := time.Now().Add(left).UTC().Format(time.RFC3339)
		rec, _, _ := tiebreakerFixture(t, map[string]string{key: deadline})

		result := reconcileSettle(t, rec)
		if result.RequeueAfter < left-time.Second || result.RequeueAfter > left+2*time.Second {
			t.Errorf("%s: came back after %v, want about %v", key, result.RequeueAfter, left)
		}
	}

	past := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	rec, _, _ := tiebreakerFixture(t, map[string]string{controllerpkg.AutoTiebreakerSuppressedUntilAnnotation: past})

	if result := reconcileSettle(t, rec); result.RequeueAfter != 0 {
		t.Errorf("a passed deadline brought the RD back after %v", result.RequeueAfter)
	}
}
