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

package controllers

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// TestObserverStampsKernelLoadedTrueOnExists pins Phase 11.3
// Stage 3's primary contract: an `exists resource <name>` events2
// frame (the initial sync at subscribe time and the verb
// drbdsetup emits for every loaded slot when the observer
// reconnects) MUST flip `Status.Conditions[type=KernelLoaded]` to
// True. The reconciler's `observeForFsm` reads this Condition to
// skip the `drbdsetup status` round-trip on the hot path.
func TestObserverStampsKernelLoadedTrueOnExists(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	existing := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-kl.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-kl",
			NodeName:               "n1",
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existing).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	fx := storage.NewFakeExec()
	o := &ObserverRunnable{Client: cli, Exec: fx, NodeName: "n1"}
	adm := drbd.NewAdm(fx)

	// Synthesise the events2 dispatch — the same shape
	// translateResourceEvent emits for `exists resource name:pvc-kl
	// role:Secondary suspended:no`.
	obs, ok := translateResourceEvent(drbd.Event{
		Kind:   eventKindResource,
		Action: eventActionExists,
		Fields: map[string]string{
			"name": "pvc-kl",
			"role": "Secondary",
		},
	})
	if !ok {
		t.Fatalf("translateResourceEvent rejected the exists frame")
	}

	if !obs.HasKernelLoaded {
		t.Fatalf("HasKernelLoaded = false, want true (resource-kind frames carry the slot signal)")
	}

	if !obs.KernelLoaded {
		t.Fatalf("KernelLoaded = false, want true (exists verb means slot is present)")
	}

	o.handleObservation(context.Background(), adm, &obs)

	var got blockstoriov1alpha1.Resource

	err := cli.Get(context.Background(), client.ObjectKey{Name: "pvc-kl.n1"}, &got)
	if err != nil {
		t.Fatalf("get Resource: %v", err)
	}

	if !meta.IsStatusConditionTrue(got.Status.Conditions, blockstoriov1alpha1.ConditionKernelLoaded) {
		t.Errorf("KernelLoaded Condition not True on Resource: %+v", got.Status.Conditions)
	}
}

// TestObserverStampsKernelLoadedFalseOnDestroy mirrors the True
// case for the slot-gone half. When events2 emits `destroy
// resource <name>` (kernel slot torn down via drbdadm down /
// resource removed), the observer MUST flip the cached
// KernelLoaded Condition to False so the reconciler's hot path
// falls back to the legacy probe rather than acting on a stale
// True value.
func TestObserverStampsKernelLoadedFalseOnDestroy(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	// Resource exists with no prior Condition — the destroy frame
	// must stamp KernelLoaded=False from scratch. Genuine "Condition
	// flips True → False" is exercised at runtime by the resync
	// ticker re-applying the same field-manager's payload; the
	// c-r fake client's SSA implementation can't reproduce that
	// merge contract faithfully, so the integration of the flip
	// itself lives in e2e (the apiserver does the merge for real).
	existing := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-kl-d.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-kl-d",
			NodeName:               "n1",
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existing).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	fx := storage.NewFakeExec()
	o := &ObserverRunnable{Client: cli, Exec: fx, NodeName: "n1"}
	adm := drbd.NewAdm(fx)

	obs, ok := translateResourceEvent(drbd.Event{
		Kind:   eventKindResource,
		Action: eventActionDestroy,
		Fields: map[string]string{
			"name": "pvc-kl-d",
		},
	})
	if !ok {
		t.Fatalf("translateResourceEvent rejected the destroy frame")
	}

	if !obs.HasKernelLoaded {
		t.Fatalf("HasKernelLoaded = false, want true (resource-kind frames carry the slot signal)")
	}

	if obs.KernelLoaded {
		t.Fatalf("KernelLoaded = true, want false (destroy verb means slot is gone)")
	}

	o.handleObservation(context.Background(), adm, &obs)

	var got blockstoriov1alpha1.Resource

	err := cli.Get(context.Background(), client.ObjectKey{Name: "pvc-kl-d.n1"}, &got)
	if err != nil {
		t.Fatalf("get Resource: %v", err)
	}

	cond := meta.FindStatusCondition(got.Status.Conditions, blockstoriov1alpha1.ConditionKernelLoaded)
	if cond == nil {
		t.Fatalf("KernelLoaded Condition absent after destroy; want present with Status=False: %+v", got.Status.Conditions)
	}

	if cond.Status != metav1.ConditionFalse {
		t.Errorf("KernelLoaded Status = %q, want %q", cond.Status, metav1.ConditionFalse)
	}
}

// TestNormalizeKernelLoadedConditionPredicate enumerates the
// events2 resource-kind verbs and pins the verb → KernelLoaded
// truth table. The predicate is the only point where drbd-9's
// verb vocabulary maps onto the Condition value — a future kernel
// adding a new verb surfaces here as a test failure rather than a
// silent stale-True regression.
func TestNormalizeKernelLoadedConditionPredicate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		action string
		want   bool
	}{
		// Slot is loaded → True.
		{action: eventActionExists, want: true},
		{action: eventActionChange, want: true},
		{action: eventActionCreate, want: true},
		// Slot is gone → False.
		{action: eventActionDestroy, want: false},
		// Unknown / empty verb → conservatively False (a transient
		// False forces the legacy probe path which is correct but
		// slow; a transient True hides a missing slot which is
		// silently wrong).
		{action: "", want: false},
		{action: "unknown-future-verb", want: false},
	}

	for _, tc := range cases {
		t.Run(tc.action, func(t *testing.T) {
			t.Parallel()

			got := normalizeKernelLoaded(tc.action)
			if got != tc.want {
				t.Errorf("normalizeKernelLoaded(%q) = %v, want %v", tc.action, got, tc.want)
			}
		})
	}
}
