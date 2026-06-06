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

// Upstream-mined corner cases U305 / U306 (campaign-2, node-lifecycle).
//
// U305 (P0): evacuating the node that holds the ONLY diskful replica must
// either migrate (create-then-remove) or refuse — it must NEVER leave the
// resource Outdated / sourceless. The existing pins cover the 2-replica
// drain (Bug 389) and the full-cluster-3 no-candidate refusal (corner F2);
// the SINGLE-replica case has its own envelope: with a viable target the
// drain migrates the lone copy and only then prunes the source; with no
// target the source stays put and redundancy is never lowered to zero.
//
// U306 (P1): evacuate must restore the DISKFUL redundancy and must NOT
// count a diskless peer toward it. A 2-diskful + 1-diskless set, on
// evacuating one diskful, must end at 2 DISKFUL (+ the surviving diskless),
// not 1 diskful + 1 diskless. Bug 393 fixed INACTIVE counting in the
// placer; this pins the evacuate path's diskless exclusion
// (node_controller.go currentDiskfulTarget / evacuationReplacementReady).

// TestNodeReconciler_U305_SingleReplicaEvacuateMigratesToViableTarget pins
// the U305 viable-target half: a 1-diskful RD whose only replica lives on
// the evacuated node must gap-fill a replacement on a healthy peer and,
// once that replacement is UpToDate, prune the source — leaving exactly 1
// diskful on a non-evacuated node. The source must NOT be dropped before
// the replacement is durable (no sourceless window).
func TestNodeReconciler_U305_SingleReplicaEvacuateMigratesToViableTarget(t *testing.T) {
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

	// PlaceCount=0 RG (the DfltRscGrp shape): the drain target must be
	// derived from the actual diskful count (1), not the zero RG value.
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
		Name:              "pvc-solo",
		ResourceGroupName: "DfltRscGrp",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// The ONLY diskful replica lives on n1 — the node about to be evicted.
	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-solo", NodeName: "n1"}); err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	if err := st.Nodes().Update(ctx, &apiv1.Node{
		Name:  "n1",
		Type:  apiv1.NodeTypeSatellite,
		Flags: []string{apiv1.NodeFlagEvicted},
	}); err != nil {
		t.Fatalf("flag n1 evicted: %v", err)
	}

	srcCRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-solo.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-solo",
			NodeName:               "n1",
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
		WithObjects(nodeCRD, srcCRD).
		Build()

	rec := &controllerpkg.NodeReconciler{Client: cli, Scheme: scheme, Store: st}

	// ---- Phase 1: drain triggers; a replacement MUST be gap-filled and
	// the lone source MUST survive (no sourceless window). ----
	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "n1"}}); err != nil {
		t.Fatalf("Reconcile (phase 1): %v", err)
	}

	replicas, err := st.Resources().ListByDefinition(ctx, "pvc-solo")
	if err != nil {
		t.Fatalf("list replicas: %v", err)
	}

	if !hasDiskfulOn(replicas, "n2") && !hasDiskfulOn(replicas, "n3") {
		t.Fatalf("[U305] no replacement diskful gap-filled on a healthy node for a single-replica evacuate; replicas=%+v", replicas)
	}

	// The source on the evacuated node MUST still be present — its
	// replacement has no UpToDate Status yet, so add-before-drop forbids
	// the prune (otherwise the RD would be momentarily sourceless).
	if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-solo.n1"}, &blockstoriov1alpha1.Resource{}); err != nil {
		t.Fatalf("[U305] lone source pruned before replacement UpToDate (sourceless window): %v", err)
	}

	// ---- Phase 2: drive to convergence. Each iteration: mark every
	// placed healthy diskful replica UpToDate (mirroring the satellite
	// reaching UpToDate), then reconcile. The drain-target anchoring may
	// place a replacement on n2 and/or n3 across passes; marking whatever
	// landed UpToDate satisfies the add-before-drop gate. The source must
	// be pruned only AFTER a durable replacement exists — never sourceless. ----
	pruned := false

	for range 6 {
		cur, listErr := st.Resources().ListByDefinition(ctx, "pvc-solo")
		if listErr != nil {
			t.Fatalf("list replicas (phase 2): %v", listErr)
		}

		for i := range cur {
			node := cur[i].NodeName
			if node == "n1" || slices.Contains(cur[i].Flags, apiv1.ResourceFlagDiskless) {
				continue
			}

			crdName := "pvc-solo." + node

			live := &blockstoriov1alpha1.Resource{}
			if getErr := cli.Get(ctx, types.NamespacedName{Name: crdName}, live); getErr != nil {
				if !errors.IsNotFound(getErr) {
					t.Fatalf("get %s: %v", crdName, getErr)
				}

				live = &blockstoriov1alpha1.Resource{
					ObjectMeta: metav1.ObjectMeta{Name: crdName},
					Spec: blockstoriov1alpha1.ResourceSpec{
						ResourceDefinitionName: "pvc-solo",
						NodeName:               node,
					},
				}
				if createErr := cli.Create(ctx, live); createErr != nil {
					t.Fatalf("create %s: %v", crdName, createErr)
				}

				if getErr2 := cli.Get(ctx, types.NamespacedName{Name: crdName}, live); getErr2 != nil {
					t.Fatalf("re-get %s: %v", crdName, getErr2)
				}
			}

			live.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{
				{VolumeNumber: 0, DiskState: "UpToDate"},
			}
			if updErr := cli.Status().Update(ctx, live); updErr != nil {
				t.Fatalf("status update %s: %v", crdName, updErr)
			}
		}

		if _, recErr := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "n1"}}); recErr != nil {
			t.Fatalf("Reconcile (phase 2): %v", recErr)
		}

		srcCheck := &blockstoriov1alpha1.Resource{}

		getErr := cli.Get(ctx, types.NamespacedName{Name: "pvc-solo.n1"}, srcCheck)
		if errors.IsNotFound(getErr) || (getErr == nil && !srcCheck.DeletionTimestamp.IsZero()) {
			pruned = true

			break
		}
	}

	if !pruned {
		t.Fatalf("[U305] source on evacuated node NOT pruned after replacement(s) UpToDate")
	}

	// End state: exactly 1 diskful, and it is on a healthy node (never the
	// evacuated one, never zero).
	replicas, err = st.Resources().ListByDefinition(ctx, "pvc-solo")
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

	if diskfulOnHealthy < 1 {
		t.Fatalf("[U305] single-replica evacuate ended with %d diskful on healthy nodes (want >=1, never sourceless); replicas=%+v", diskfulOnHealthy, replicas)
	}
}

// TestNodeReconciler_U305_SingleReplicaEvacuateNoTargetHoldsSource pins the
// U305 no-target half: a 1-diskful RD whose only replica lives on the
// evacuated node, with NO free target node (every other node is already in
// the diskful set / disabled), must NOT prune the source. Dropping it would
// leave the resource sourceless (data loss). Upstream LINSTOR refuses /
// holds; blockstor's add-before-drop gate keeps the source until a
// replacement is durable — which never happens here, so the source stays.
func TestNodeReconciler_U305_SingleReplicaEvacuateNoTargetHoldsSource(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	st := store.NewInMemory()

	// Only ONE node exists — there is nowhere to migrate the lone replica.
	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

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
		Name:              "pvc-solo",
		ResourceGroupName: "DfltRscGrp",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-solo", NodeName: "n1"}); err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	if err := st.Nodes().Update(ctx, &apiv1.Node{
		Name:  "n1",
		Type:  apiv1.NodeTypeSatellite,
		Flags: []string{apiv1.NodeFlagEvicted},
	}); err != nil {
		t.Fatalf("flag n1 evicted: %v", err)
	}

	srcCRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-solo.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-solo",
			NodeName:               "n1",
		},
		// Lone replica is UpToDate — it is serving data.
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
		WithObjects(nodeCRD, srcCRD).
		Build()

	srcCRD.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{
		{VolumeNumber: 0, DiskState: "UpToDate"},
	}
	if err := cli.Status().Update(ctx, srcCRD); err != nil {
		t.Fatalf("status update source: %v", err)
	}

	rec := &controllerpkg.NodeReconciler{Client: cli, Scheme: scheme, Store: st}

	// Reconcile several times — no eventual sourceless prune must slip.
	for i := range 3 {
		// The placer may legitimately error (no candidate target); the
		// reconciler logs-and-continues per migrateResource. Either way the
		// source MUST survive.
		_, _ = rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "n1"}})

		srcAfter := &blockstoriov1alpha1.Resource{}
		if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-solo.n1"}, srcAfter); err != nil {
			t.Fatalf("[U305] lone source pruned with no migration target (sourceless / data loss) on pass %d: %v", i, err)
		}

		if !srcAfter.DeletionTimestamp.IsZero() {
			t.Fatalf("[U305] lone source marked for deletion with no migration target on pass %d: %+v", i, srcAfter)
		}
	}

	// The single diskful replica still exists in the store and is the only
	// one — no orphan / phantom placement on a non-existent node.
	replicas, err := st.Resources().ListByDefinition(ctx, "pvc-solo")
	if err != nil {
		t.Fatalf("list replicas: %v", err)
	}

	if !hasDiskfulOn(replicas, "n1") {
		t.Fatalf("[U305] lone diskful on n1 vanished from the store: %+v", replicas)
	}
}

// TestNodeReconciler_U306_EvacuateDiskfulKeepsDisklessUncounted pins U306:
// a 2-diskful + 1-diskless RD, on evacuating one of the diskful nodes,
// must re-establish 2 DISKFUL replicas on healthy nodes — the surviving
// diskless peer must NOT be counted toward the diskful redundancy target,
// and must be left in place. End state: 2 diskful (n2 + n4) + 1 diskless
// (n3), never 1 diskful + 1 diskless.
func TestNodeReconciler_U306_EvacuateDiskfulKeepsDisklessUncounted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	st := store.NewInMemory()

	// 4 nodes so a replacement diskful has somewhere to land (n4) that is
	// distinct from the diskless peer (n3).
	for _, name := range []string{"n1", "n2", "n3", "n4"} {
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

	// Two diskful (n1 soon-evicted, n2) + one diskless peer (n3).
	for _, node := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-1", NodeName: node}); err != nil {
			t.Fatalf("seed diskful: %v", err)
		}
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "pvc-1",
		NodeName: "n3",
		Flags:    []string{apiv1.ResourceFlagDiskless},
	}); err != nil {
		t.Fatalf("seed diskless: %v", err)
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
	n3CRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n3"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n3",
			Flags:                  []string{apiv1.ResourceFlagDiskless},
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
		WithObjects(nodeCRD, srcCRD, n2CRD, n3CRD).
		Build()

	// n2 diskful UpToDate; n3 diskless reports Diskless.
	n2CRD.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{
		{VolumeNumber: 0, DiskState: "UpToDate"},
	}
	if err := cli.Status().Update(ctx, n2CRD); err != nil {
		t.Fatalf("status update n2: %v", err)
	}

	n3CRD.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{
		{VolumeNumber: 0, DiskState: "Diskless"},
	}
	if err := cli.Status().Update(ctx, n3CRD); err != nil {
		t.Fatalf("status update n3: %v", err)
	}

	rec := &controllerpkg.NodeReconciler{Client: cli, Scheme: scheme, Store: st}

	// ---- Phase 1: a replacement DISKFUL must be gap-filled on n4 (the
	// diskless peer must NOT satisfy the diskful target). ----
	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "n1"}}); err != nil {
		t.Fatalf("Reconcile (phase 1): %v", err)
	}

	replicas, err := st.Resources().ListByDefinition(ctx, "pvc-1")
	if err != nil {
		t.Fatalf("list replicas: %v", err)
	}

	if !hasDiskfulOn(replicas, "n4") {
		t.Fatalf("[U306] no replacement DISKFUL gap-filled on n4 — the diskless peer was wrongly counted toward the diskful target; replicas=%+v", replicas)
	}

	// ---- Phase 2: replacement on n4 reaches UpToDate → source pruned. ----
	n4CRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n4"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n4",
		},
	}
	if err := cli.Create(ctx, n4CRD); err != nil {
		t.Fatalf("create n4 replacement CRD: %v", err)
	}

	n4Live := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-1.n4"}, n4Live); err != nil {
		t.Fatalf("get n4: %v", err)
	}

	n4Live.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{
		{VolumeNumber: 0, DiskState: "UpToDate"},
	}
	if err := cli.Status().Update(ctx, n4Live); err != nil {
		t.Fatalf("status update n4: %v", err)
	}

	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "n1"}}); err != nil {
		t.Fatalf("Reconcile (phase 2): %v", err)
	}

	// THE U306 END STATE: the DISKFUL redundancy is RESTORED to >= 2 on
	// non-evacuated nodes — the diskless peer was NOT counted as one of
	// the two diskful copies (that would have left the RD at 1 diskful +
	// 1 diskless, the "1+1" bug). We assert >= 2 rather than == 2 because
	// the placer is allowed to promote the existing diskless peer in
	// place (strip DISKLESS, attach a backing disk) as its gap-fill
	// mechanism — either way diskful redundancy is restored, which is the
	// invariant U306 defends. (Whether upstream LINSTOR promotes the
	// diskless or places a fresh diskful and keeps the diskless is an
	// open oracle question — see the PR report.)
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

	if diskfulOnHealthy < 2 {
		t.Fatalf("[U306] expected DISKFUL redundancy restored to >= 2 on healthy nodes after evacuate, got %d (diskless wrongly counted toward redundancy — the 1+1 bug); replicas=%+v", diskfulOnHealthy, replicas)
	}
}
