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

package controller

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// Bug 399: `vd d` drops a VolumeDefinition from the RD, but the
// controller's RD.Spec.VolumeDefinitions → Resource.Spec.Volumes
// projection was ADD-ONLY (ensureSeedFromGI / setSeedFromGI only ever
// append). The stale Spec.Volumes entry for the removed volume then
// kept the controller "knowing" about it, so the phantom Status.Volumes
// entry was never garbage-collected and the Resource Status flapped
// forever (resourceVersion churned ~1/s).
//
// These tests pin the prune (remove side) added in this fix and prove
// it does NOT regress the still-present volume or the `vd c` late-add
// (Bug 384) path, and that it is strictly idempotent (no PATCH thrash
// once converged — the actual flap-stopper).

func bug399Scheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	return scheme
}

func volNumbers(res *blockstoriov1alpha1.Resource) []int32 {
	out := make([]int32, 0, len(res.Spec.Volumes))
	for i := range res.Spec.Volumes {
		out = append(out, res.Spec.Volumes[i].VolumeNumber)
	}

	return out
}

// TestBug399PrunesRemovedVolumeFromSpec is the core regression: an RD
// reduced to a single VolumeDefinition (vol-0) must drive the Resource's
// Spec.Volumes — still carrying the stale vol-1 left by the add-only
// projection — back to exactly [vol-0] on Reconcile, and the second
// Reconcile must be a no-op (no resourceVersion churn) so the flap
// stops.
func TestBug399PrunesRemovedVolumeFromSpec(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := bug399Scheme(t)

	const (
		rdName       = "flap-rd"
		nodeName     = "n1"
		resourceName = rdName + "." + nodeName
	)

	// RD already reduced to vol-0 only (post `vd d 1`).
	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024},
			},
		},
	}

	// Resource Spec.Volumes still carries the orphaned vol-1.
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               nodeName,
			Volumes: []blockstoriov1alpha1.ResourceVolumeSpec{
				{VolumeNumber: 0},
				{VolumeNumber: 1},
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&blockstoriov1alpha1.Resource{},
			&blockstoriov1alpha1.ResourceDefinition{},
		).
		WithObjects(rd, res).
		Build()

	rec := &ResourceReconciler{Client: cli, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: resourceName}}

	reconcileN := func(label string, n int) {
		for i := range n {
			if _, err := rec.Reconcile(ctx, req); err != nil {
				t.Fatalf("%s Reconcile %d: %v", label, i, err)
			}
		}
	}

	// Drain the requeue chain to convergence: the first passes stamp
	// DRBD-IDs / skip-sync decision (each requeues), then the prune
	// fires. A real controller drains its workqueue the same way.
	reconcileN("initial", 10)

	got := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: resourceName}, got); err != nil {
		t.Fatalf("get after prune: %v", err)
	}

	if nums := volNumbers(got); len(nums) != 1 || nums[0] != 0 {
		t.Fatalf("Spec.Volumes after prune = %v, want [0]", nums)
	}

	// The no-flap assertion: once converged, further Reconciles must NOT
	// write to the apiserver. We detect a write via resourceVersion
	// churn (the fake client bumps it on every Update). A non-idempotent
	// prune would re-remove/re-add the same entry every pass — exactly
	// the production flap (resourceVersion churning ~1/s).
	rvAfterPrune := got.ResourceVersion

	reconcileN("steady-state", 5)

	settled := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: resourceName}, settled); err != nil {
		t.Fatalf("get after steady-state: %v", err)
	}

	if settled.ResourceVersion != rvAfterPrune {
		t.Errorf("resourceVersion churned after prune converged (%s -> %s): prune is not idempotent, the flap would persist",
			rvAfterPrune, settled.ResourceVersion)
	}

	if nums := volNumbers(settled); len(nums) != 1 || nums[0] != 0 {
		t.Errorf("Spec.Volumes drifted after steady-state = %v, want [0]", nums)
	}
}

// TestBug399KeepsPresentAndMidAddVolumes pins the two non-regression
// cases for the prune:
//
//  1. A volume still declared by the RD is NEVER pruned.
//  2. The `vd c` late-add (Bug 384) shape — the RD already carries the
//     new VolumeDefinition (that is what triggers the Resource
//     reconcile) while the Resource's Spec.Volumes has not yet caught
//     up — must be preserved: prune keys off ABSENCE from the RD, so a
//     mid-add volume (present in the RD) is in the desired set and is
//     never removed.
func TestBug399KeepsPresentAndMidAddVolumes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := bug399Scheme(t)

	const (
		rdName       = "twovol-rd"
		nodeName     = "n2"
		resourceName = rdName + "." + nodeName
	)

	// RD carries vol-0 AND vol-1 (e.g. mid `vd c 1` — the new VD is
	// already authored, the satellite/Resource is catching up).
	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024},
				{VolumeNumber: 1, SizeKib: 2048},
			},
		},
	}

	// Resource only has vol-0 so far (vol-1 add still in flight). The
	// prune must NOT touch vol-0, and must NOT invent/remove anything
	// for the not-yet-present vol-1.
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               nodeName,
			Volumes: []blockstoriov1alpha1.ResourceVolumeSpec{
				{VolumeNumber: 0},
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&blockstoriov1alpha1.Resource{},
			&blockstoriov1alpha1.ResourceDefinition{},
		).
		WithObjects(rd, res).
		Build()

	rec := &ResourceReconciler{Client: cli, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: resourceName}}

	for i := range 10 {
		if _, err := rec.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile %d: %v", i, err)
		}
	}

	got := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: resourceName}, got); err != nil {
		t.Fatalf("get: %v", err)
	}

	// vol-0 must survive. vol-1 (mid-add, in the RD) must never be
	// pruned away. The seed-GI add-path may legitimately not add vol-1
	// here (no UpToDate peer to seed from), but it must never be
	// REMOVED — assert vol-0 is present and no declared volume vanished.
	present := map[int32]bool{}
	for _, n := range volNumbers(got) {
		present[n] = true
	}

	if !present[0] {
		t.Errorf("vol-0 (still declared by RD) was pruned: Spec.Volumes = %v", volNumbers(got))
	}
}

// TestPruneStaleResourceVolumesUnit exercises the helper directly for
// the boundary cases the Reconcile-level tests don't isolate: a nil/
// empty RD must be a no-op (defensive — never blank Spec.Volumes during
// a create/cascade), and an already-in-sync Resource must report
// mutated=false (no apiserver write).
func TestPruneStaleResourceVolumesUnit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := bug399Scheme(t)

	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	rec := &ResourceReconciler{Client: cli, Scheme: scheme}

	twoVolRes := func() *blockstoriov1alpha1.Resource {
		return &blockstoriov1alpha1.Resource{
			Spec: blockstoriov1alpha1.ResourceSpec{
				Volumes: []blockstoriov1alpha1.ResourceVolumeSpec{
					{VolumeNumber: 0},
					{VolumeNumber: 1},
				},
			},
		}
	}

	// nil RD: defensive no-op, no error, no mutation.
	if mutated, err := rec.pruneStaleResourceVolumes(ctx, twoVolRes(), nil); err != nil || mutated {
		t.Errorf("nil RD: mutated=%v err=%v, want false/nil", mutated, err)
	}

	// RD with zero VolumeDefinitions (fresh / mid-cascade): no-op.
	emptyRD := &blockstoriov1alpha1.ResourceDefinition{}
	if mutated, err := rec.pruneStaleResourceVolumes(ctx, twoVolRes(), emptyRD); err != nil || mutated {
		t.Errorf("empty-RD: mutated=%v err=%v, want false/nil", mutated, err)
	}

	// Already in sync (RD declares both vol-0 and vol-1): no mutation.
	syncedRD := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024},
				{VolumeNumber: 1, SizeKib: 1024},
			},
		},
	}
	if mutated, err := rec.pruneStaleResourceVolumes(ctx, twoVolRes(), syncedRD); err != nil || mutated {
		t.Errorf("in-sync: mutated=%v err=%v, want false/nil", mutated, err)
	}
}
