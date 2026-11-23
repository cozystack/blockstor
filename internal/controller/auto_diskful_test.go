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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestAutoDisklessPromoted: a DISKLESS replica that becomes InUse on
// a node with a viable storage pool gets promoted to diskful when the
// volume has fewer diskful replicas than its resource group's place
// count — the reconciler removes the DISKLESS flag and stamps
// StorPoolName on Spec.Props. The satellite reconciler picks up the
// change on its next reconcile and creates the LV / runs drbdadm
// attach.
func TestAutoDisklessPromoted(t *testing.T) {
	t.Parallel()

	got := reconcileDisklessAttach(t, "pvc-promote", 2, []diskfulPeer{{node: "n2"}})

	if slices.Contains(got.Spec.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS flag still present: %v", got.Spec.Flags)
	}

	if got.Spec.Props["StorPoolName"] != "stand" {
		t.Errorf("StorPoolName: got %q, want stand", got.Spec.Props["StorPoolName"])
	}
}

// TestAutoDisklessStaysWhenPlacementCountMet: a volume that already
// has its place count of diskful replicas keeps a diskless attach
// diskless — a move to a node without a replica must not copy the
// whole volume there.
func TestAutoDisklessStaysWhenPlacementCountMet(t *testing.T) {
	t.Parallel()

	got := reconcileDisklessAttach(t, "pvc-met", 2, []diskfulPeer{{node: "n2"}, {node: "n3"}})

	if !slices.Contains(got.Spec.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS dropped although the place count is met: %v", got.Spec.Flags)
	}

	if got.Spec.Props["StorPoolName"] != "" {
		t.Errorf("StorPoolName set on a replica that stays diskless: %v", got.Spec.Props)
	}
}

// TestAutoDisklessRefillsLostCopy: diskful peers that are flagged
// DELETE or being deleted are not live copies, so the place count is
// not met and the node using the volume is promoted.
func TestAutoDisklessRefillsLostCopy(t *testing.T) {
	t.Parallel()

	got := reconcileDisklessAttach(t, "pvc-lost", 2, []diskfulPeer{
		{node: "n2"},
		{node: "n3", flags: []string{apiv1.ResourceFlagDelete}},
		{node: "n4", deleting: true},
	})

	if slices.Contains(got.Spec.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS kept although a copy was lost: %v", got.Spec.Flags)
	}
}

// TestAutoDisklessRefillsAfterEviction: a diskful peer on an EVICTED
// node is not a live copy, so the place count is not met and the node
// using the volume is promoted.
func TestAutoDisklessRefillsAfterEviction(t *testing.T) {
	t.Parallel()

	got := reconcileDisklessAttach(t, "pvc-evict", 2, []diskfulPeer{
		{node: "n2"},
		{node: "n3", evicted: true},
	})

	if slices.Contains(got.Spec.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS kept although a copy sits on an evicted node: %v", got.Spec.Flags)
	}
}

// TestAutoDisklessDisklessPeersDoNotCount: diskless peers (including a
// tiebreaker) are not diskful copies.
func TestAutoDisklessDisklessPeersDoNotCount(t *testing.T) {
	t.Parallel()

	got := reconcileDisklessAttach(t, "pvc-dl", 2, []diskfulPeer{
		{node: "n2"},
		{node: "n3", flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker}},
	})

	if slices.Contains(got.Spec.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS kept although only one copy is diskful: %v", got.Spec.Flags)
	}
}

// TestAutoDisklessZeroPlaceCountReadsAsOne: the default group ships
// with place count 0, which the placer treats as 1.
func TestAutoDisklessZeroPlaceCountReadsAsOne(t *testing.T) {
	t.Parallel()

	got := reconcileDisklessAttach(t, "pvc-zero", 0, []diskfulPeer{{node: "n2"}})

	if !slices.Contains(got.Spec.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS dropped although one diskful copy satisfies place count 0: %v", got.Spec.Flags)
	}
}

// TestAutoDisklessStaysWithoutResourceGroup: when the place count
// can't be read (no group, or the group is gone) nothing is promoted.
func TestAutoDisklessStaysWithoutResourceGroup(t *testing.T) {
	t.Parallel()

	got := reconcileDisklessAttach(t, "pvc-norg", -1, nil)

	if !slices.Contains(got.Spec.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS dropped without a readable resource group: %v", got.Spec.Flags)
	}
}

// diskfulPeer describes a sibling replica of the InUse diskless one.
type diskfulPeer struct {
	node     string
	flags    []string
	deleting bool
	evicted  bool
}

// reconcileDisklessAttach seeds an InUse diskless replica on n1 (which
// has a pool) next to the given peers, under a resource group with the
// given place count (negative: the definition's group doesn't exist),
// and reconciles it to convergence.
func reconcileDisklessAttach(
	t *testing.T,
	rdName string,
	placeCount int32,
	peers []diskfulPeer,
) *blockstoriov1alpha1.Resource {
	t.Helper()

	ctx := context.Background()
	scheme := newScheme(t)
	st := store.NewInMemory()

	_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "stand",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
	})

	if placeCount >= 0 {
		err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
			Name:         "rg",
			SelectFilter: apiv1.AutoSelectFilter{PlaceCount: apiv1.LaxInt32(placeCount)},
		})
		if err != nil {
			t.Fatalf("seed resource group: %v", err)
		}
	}

	for _, p := range peers {
		if !p.evicted {
			continue
		}

		err := st.Nodes().Create(ctx, &apiv1.Node{
			Name:  p.node,
			Flags: []string{apiv1.NodeFlagEvicted},
		})
		if err != nil {
			t.Fatalf("seed evicted node: %v", err)
		}
	}

	objs := make([]client.Object, 0, 2+len(peers))
	objs = append(objs,
		&blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: rdName},
			Spec:       blockstoriov1alpha1.ResourceDefinitionSpec{ResourceGroupName: "rg"},
		},
		&blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{
				Name:       rdName + ".n1",
				Finalizers: []string{"blockstor.org/resource"},
			},
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: rdName,
				NodeName:               "n1",
				Flags:                  []string{apiv1.ResourceFlagDiskless},
			},
			Status: blockstoriov1alpha1.ResourceStatus{InUse: true},
		},
	)

	for _, p := range peers {
		peer := &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: rdName + "." + p.node},
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: rdName,
				NodeName:               p.node,
				Flags:                  p.flags,
			},
		}

		if p.deleting {
			now := metav1.Now()
			peer.DeletionTimestamp = &now
			peer.Finalizers = []string{"blockstor.org/satellite-resource"}
		}

		objs = append(objs, peer)
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		WithObjects(objs...).
		Build()

	rec := &controllerpkg.ResourceReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	// Reconcile drives several converging passes (DRBD-id allocation
	// → Status update → requeue → auto-diskful → Spec update →
	// requeue → dispatchApply). Run until convergence so the final
	// state is what we assert on.
	for range 8 {
		_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: rdName + ".n1"}})
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
	}

	got := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, types.NamespacedName{Name: rdName + ".n1"}, got); err != nil {
		t.Fatalf("get: %v", err)
	}

	return got
}

// TestAutoDisklessSkipsTiebreaker: a TIE_BREAKER witness must NEVER
// be auto-promoted — its whole point is the network-only quorum
// presence. Promoting would defeat the purpose and waste storage.
func TestAutoDisklessSkipsTiebreaker(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	st := store.NewInMemory()

	_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "stand",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
	})

	resCRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-tb.n1",
			Finalizers: []string{"blockstor.org/resource"},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-tb",
			NodeName:               "n1",
			Flags:                  []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
		},
		Status: blockstoriov1alpha1.ResourceStatus{InUse: true},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		WithObjects(resCRD).
		Build()

	rec := &controllerpkg.ResourceReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-tb.n1"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &blockstoriov1alpha1.Resource{}
	_ = cli.Get(ctx, types.NamespacedName{Name: "pvc-tb.n1"}, got)

	if !slices.Contains(got.Spec.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS dropped from a TIE_BREAKER replica: %v", got.Spec.Flags)
	}
}

// TestAutoDisklessSkipsWhenNoPool: no storage pool on the hosting
// node → leave the diskless replica alone (a viable promotion isn't
// possible).
func TestAutoDisklessSkipsWhenNoPool(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	st := store.NewInMemory()
	// No StoragePool created — the node has no local storage.

	resCRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-nopool.n1",
			Finalizers: []string{"blockstor.org/resource"},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-nopool",
			NodeName:               "n1",
			Flags:                  []string{apiv1.ResourceFlagDiskless},
		},
		Status: blockstoriov1alpha1.ResourceStatus{InUse: true},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		WithObjects(resCRD).
		Build()

	rec := &controllerpkg.ResourceReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-nopool.n1"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &blockstoriov1alpha1.Resource{}
	_ = cli.Get(ctx, types.NamespacedName{Name: "pvc-nopool.n1"}, got)

	if !slices.Contains(got.Spec.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS dropped despite no available pool: %v", got.Spec.Flags)
	}

	if got.Spec.Props["StorPoolName"] != "" {
		t.Errorf("StorPoolName set without a pool: %v", got.Spec.Props)
	}
}
