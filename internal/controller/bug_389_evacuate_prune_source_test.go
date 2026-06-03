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
	"slices"
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestNodeReconciler_EvictedPrunesSourceAfterReplacementUpToDate pins
// Bug 389: `node evacuate` is an online drain that must, after placing
// a replacement on a healthy peer and once that replacement is observed
// UpToDate, DELETE the source diskful replica on the evacuated node —
// leaving the node empty so `node delete` completes cleanly. The old
// behaviour left the source pinned forever (place_count+1 diskful,
// storage never reclaimed, drain never finished).
//
// Strict add-before-drop is enforced by a two-phase drill against one
// fake client (mirroring the migration reconciler's Option-B test):
//
//  1. Replacement on n3 exists but its Resource CRD reports no UpToDate
//     volume yet → the source on n1 MUST survive (redundancy preserved).
//  2. Stamp the n3 replacement CRD Status UpToDate and reconcile again →
//     the source Resource CRD on n1 MUST be deleted.
func TestNodeReconciler_EvictedPrunesSourceAfterReplacementUpToDate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	st := store.NewInMemory()

	for _, name := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: name, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node: %v", err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        name,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool: %v", err)
		}
	}

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  2,
			StoragePool: "pool",
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "pvc-1",
		ResourceGroupName: "rg",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// Diskful replicas on n1 (the soon-to-be-evicted source) and n2.
	for _, node := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-1", NodeName: node}); err != nil {
			t.Fatalf("seed resource: %v", err)
		}
	}

	// Flag n1 EVICTED in both the store and the Node CRD.
	if err := st.Nodes().Update(ctx, &apiv1.Node{
		Name:  "n1",
		Type:  apiv1.NodeTypeSatellite,
		Flags: []string{apiv1.NodeFlagEvicted},
	}); err != nil {
		t.Fatalf("flag n1 evicted: %v", err)
	}

	// Resource CRDs for the existing replicas. n2 is the healthy peer
	// that must reach place_count once n3's replacement is UpToDate;
	// seed n2 UpToDate so the gate hinges solely on the new n3 copy.
	srcCRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n1",
		},
	}

	n2CRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n2"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n2",
		},
	}

	nodeCRD := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec: blockstoriov1alpha1.NodeSpec{
			Type:  apiv1.NodeTypeSatellite,
			Flags: []string{apiv1.NodeFlagEvicted},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		WithObjects(nodeCRD, srcCRD, n2CRD).
		Build()

	// n2 already UpToDate.
	n2CRD.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{
		{VolumeNumber: 0, DiskState: "UpToDate"},
	}
	if err := cli.Status().Update(ctx, n2CRD); err != nil {
		t.Fatalf("status update n2: %v", err)
	}

	rec := &controllerpkg.NodeReconciler{Client: cli, Scheme: scheme, Store: st}

	// ---- Phase 1: replacement on n3 not yet UpToDate ----
	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "n1"}})
	if err != nil {
		t.Fatalf("Reconcile (phase 1): %v", err)
	}

	// The placer created a store replica on n3; mirror it as a CRD so
	// the readiness gate has something to inspect — but leave its
	// Status empty (still syncing). The source on n1 MUST survive.
	srcLive := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-1.n1"}, srcLive); err != nil {
		t.Fatalf("source pruned while replacement still syncing (Bug 389 add-before-drop regression): %v", err)
	}

	n3CRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n3"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n3",
		},
		// No Status.Volumes — replacement still syncing.
	}
	if err := cli.Create(ctx, n3CRD); err != nil {
		t.Fatalf("create n3 replacement CRD: %v", err)
	}

	_, err = rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "n1"}})
	if err != nil {
		t.Fatalf("Reconcile (phase 1b): %v", err)
	}

	srcLive = &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-1.n1"}, srcLive); err != nil {
		t.Fatalf("source pruned while replacement still syncing (Bug 389 add-before-drop regression): %v", err)
	}

	// ---- Phase 2: replacement on n3 reaches UpToDate ----
	n3Live := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-1.n3"}, n3Live); err != nil {
		t.Fatalf("get n3: %v", err)
	}

	n3Live.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{
		{VolumeNumber: 0, DiskState: "UpToDate"},
	}
	if err := cli.Status().Update(ctx, n3Live); err != nil {
		t.Fatalf("status update n3: %v", err)
	}

	_, err = rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "n1"}})
	if err != nil {
		t.Fatalf("Reconcile (phase 2): %v", err)
	}

	// The source Resource CRD on the evacuated node MUST now be gone
	// (or marked for deletion if it carried a finalizer).
	srcAfter := &blockstoriov1alpha1.Resource{}

	err = cli.Get(ctx, types.NamespacedName{Name: "pvc-1.n1"}, srcAfter)
	if err == nil && srcAfter.DeletionTimestamp.IsZero() {
		t.Fatalf("source replica on evacuated node NOT pruned after replacement UpToDate (Bug 389): still present %+v", srcAfter)
	} else if err != nil && !errors.IsNotFound(err) {
		t.Fatalf("Get source after prune: unexpected err %v", err)
	}
}

// TestNodeReconciler_EvictedDfltRscGrpPlaceCountZeroNoDropWithoutAdd is the
// Bug 389 REWORK regression: the original fix defaulted filter.PlaceCount
// to 1 whenever the RG carried PlaceCount=0 (the default `DfltRscGrp`
// ships PlaceCount=0 and CLI-created RDs inherit it). With place_count=1:
//
//   - placer.Place sees the surviving peer (n2) already holds 1 valid
//     diskful → "satisfied" → it gap-fills NOTHING on n3;
//   - evacuationReplacementReady counts n2 (1) >= place_count (1) → TRUE;
//   - pruneSource deletes n1's source → the RD lands at ONE diskful.
//
// That is drop-without-add, strictly worse than the pre-fix bug (which at
// least kept the source). The rework anchors the effective target to the
// CURRENT diskful count (n1 + n2 = 2), so the placer MUST gap-fill n3 and
// the source MUST NOT be pruned until n3 is UpToDate — ending at 2 diskful.
//
// This test FAILS against the defective (PlaceCount-defaults-to-1) fix:
// either the source is pruned in phase 1 (no replacement placed) or the
// RD ends at 1 diskful.
func TestNodeReconciler_EvictedDfltRscGrpPlaceCountZeroNoDropWithoutAdd(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	st := store.NewInMemory()

	for _, name := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: name, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node: %v", err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        name,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool: %v", err)
		}
	}

	// The crux of the repro: the RG carries PlaceCount=0, exactly like the
	// default DfltRscGrp. The drain target must come from the actual
	// diskful replica count, not this zero/defaulted RG value.
	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "DfltRscGrp",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  0,
			StoragePool: "pool",
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "pvc-1",
		ResourceGroupName: "DfltRscGrp",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// Two diskful replicas: n1 (soon-to-be-evicted source) and n2.
	for _, node := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-1", NodeName: node}); err != nil {
			t.Fatalf("seed resource: %v", err)
		}
	}

	if err := st.Nodes().Update(ctx, &apiv1.Node{
		Name:  "n1",
		Type:  apiv1.NodeTypeSatellite,
		Flags: []string{apiv1.NodeFlagEvicted},
	}); err != nil {
		t.Fatalf("flag n1 evicted: %v", err)
	}

	srcCRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n1",
		},
	}

	n2CRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n2"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n2",
		},
	}

	nodeCRD := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec: blockstoriov1alpha1.NodeSpec{
			Type:  apiv1.NodeTypeSatellite,
			Flags: []string{apiv1.NodeFlagEvicted},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		WithObjects(nodeCRD, srcCRD, n2CRD).
		Build()

	// n2 already UpToDate so the gate hinges solely on the n3 replacement
	// the placer is REQUIRED to create (it would not, under place_count=1).
	n2CRD.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{
		{VolumeNumber: 0, DiskState: "UpToDate"},
	}
	if err := cli.Status().Update(ctx, n2CRD); err != nil {
		t.Fatalf("status update n2: %v", err)
	}

	rec := &controllerpkg.NodeReconciler{Client: cli, Scheme: scheme, Store: st}

	// ---- Phase 1: drain triggers; replacement must be gap-filled ----
	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "n1"}}); err != nil {
		t.Fatalf("Reconcile (phase 1): %v", err)
	}

	// The placer MUST have created a third diskful replica in the store on
	// the only non-evacuated free node, n3. Under the defective fix
	// (place_count defaults to 1) the surviving n2 makes the placer
	// "satisfied" and NO replacement is placed here — the assertion below
	// catches that drop-without-add precondition.
	replicas, err := st.Resources().ListByDefinition(ctx, "pvc-1")
	if err != nil {
		t.Fatalf("list replicas: %v", err)
	}

	if !hasDiskfulOn(replicas, "n3") {
		t.Fatalf("placer did not gap-fill a replacement on n3 (Bug 389 drop-without-add: RG PlaceCount=0 defaulted to 1, surviving peer treated as satisfied). replicas=%+v", replicas)
	}

	// Source on n1 MUST still be present — n3's replacement is not yet
	// UpToDate (no CRD/status), so add-before-drop forbids the prune.
	srcLive := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-1.n1"}, srcLive); err != nil {
		t.Fatalf("source pruned before replacement UpToDate (Bug 389 add-before-drop regression): %v", err)
	}

	// ---- Phase 2: n3 replacement reaches UpToDate ----
	n3CRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n3"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n3",
		},
	}
	if err := cli.Create(ctx, n3CRD); err != nil {
		t.Fatalf("create n3 replacement CRD: %v", err)
	}

	n3Live := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-1.n3"}, n3Live); err != nil {
		t.Fatalf("get n3: %v", err)
	}

	n3Live.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{
		{VolumeNumber: 0, DiskState: "UpToDate"},
	}
	if err := cli.Status().Update(ctx, n3Live); err != nil {
		t.Fatalf("status update n3: %v", err)
	}

	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "n1"}}); err != nil {
		t.Fatalf("Reconcile (phase 2): %v", err)
	}

	// Source on the evacuated node MUST now be pruned.
	srcAfter := &blockstoriov1alpha1.Resource{}

	err = cli.Get(ctx, types.NamespacedName{Name: "pvc-1.n1"}, srcAfter)
	if err == nil && srcAfter.DeletionTimestamp.IsZero() {
		t.Fatalf("source replica on evacuated node NOT pruned after replacement UpToDate (Bug 389): still present %+v", srcAfter)
	} else if err != nil && !errors.IsNotFound(err) {
		t.Fatalf("Get source after prune: unexpected err %v", err)
	}

	// End state: exactly 2 diskful replicas remain on the NON-evacuated
	// nodes (n2 + n3) — redundancy preserved, never dropped to 1.
	replicas, err = st.Resources().ListByDefinition(ctx, "pvc-1")
	if err != nil {
		t.Fatalf("list replicas (end): %v", err)
	}

	diskfulOnHealthy := 0

	for i := range replicas {
		if replicas[i].NodeName == "n1" {
			continue
		}

		if slices.Contains(replicas[i].Flags, apiv1.ResourceFlagDiskless) {
			continue
		}

		diskfulOnHealthy++
	}

	if diskfulOnHealthy != 2 {
		t.Fatalf("expected 2 diskful replicas on non-evacuated nodes after evacuate, got %d (Bug 389 drop-without-add). replicas=%+v", diskfulOnHealthy, replicas)
	}
}

// hasDiskfulOn reports whether `replicas` contains a diskful (no DISKLESS
// flag) replica pinned to `node`.
func hasDiskfulOn(replicas []apiv1.Resource, node string) bool {
	for i := range replicas {
		if replicas[i].NodeName != node {
			continue
		}

		if slices.Contains(replicas[i].Flags, apiv1.ResourceFlagDiskless) {
			continue
		}

		return true
	}

	return false
}
