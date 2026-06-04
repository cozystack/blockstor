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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestNodeReconciler_EvictedNoCandidateKeepsSource pins corner-case F2
// (UG9 §"Evacuating a node" — no replacement target available): when
// every healthy node already hosts a diskful replica (place_count ==
// node count), evacuating one node has nowhere to migrate the displaced
// replica. Upstream LINSTOR warns and LEAVES the replicas in place — the
// node hangs in EVACUATE and redundancy never drops.
//
// blockstor must mirror that fail-safe: the eviction reconciler's
// add-before-drop gate (evacuationReplacementReady) requires
// place_count diskful replicas UpToDate on NON-evacuated peers before
// the source is pruned. With place_count=3 and one of the three nodes
// evicted, only 2 healthy peers can ever be UpToDate, so the gate never
// passes and the source on the evacuated node MUST survive. Pruning it
// would be a drop-without-add that lowers redundancy from 3 to 2 with no
// replacement — exactly the data-availability hazard F2 guards against.
//
// 3 nodes, 3 diskful replicas (n1+n2+n3), RG place_count=3, n1 EVICTED:
// after reconcile the source on n1 is STILL present.
func TestNodeReconciler_EvictedNoCandidateKeepsSource(t *testing.T) {
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

	// place_count == 3 == node count: the cluster is already full, so an
	// evacuation has no spare node to land a replacement on.
	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  3,
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

	for _, node := range []string{"n1", "n2", "n3"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-1", NodeName: node}); err != nil {
			t.Fatalf("seed resource: %v", err)
		}
	}

	// Flag n1 EVICTED in the store.
	if err := st.Nodes().Update(ctx, &apiv1.Node{
		Name:  "n1",
		Type:  apiv1.NodeTypeSatellite,
		Flags: []string{apiv1.NodeFlagEvicted},
	}); err != nil {
		t.Fatalf("flag n1 evicted: %v", err)
	}

	// Resource CRDs for all three replicas. n2 + n3 are healthy and
	// UpToDate; if pruning were (wrongly) gated on "place_count peers
	// UpToDate" counting the evacuated node, the 2 healthy peers would
	// fall one short of place_count=3 and the source must stay.
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

	for _, crd := range []*blockstoriov1alpha1.Resource{n2CRD, n3CRD} {
		crd.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{
			{VolumeNumber: 0, DiskState: "UpToDate"},
		}
		if err := cli.Status().Update(ctx, crd); err != nil {
			t.Fatalf("status update %s: %v", crd.Name, err)
		}
	}

	rec := &controllerpkg.NodeReconciler{Client: cli, Scheme: scheme, Store: st}

	// Reconcile a few times to be sure no eventual prune slips through.
	for i := range 3 {
		_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "n1"}})
		if err != nil {
			t.Fatalf("Reconcile (pass %d): %v", i, err)
		}
	}

	// THE F2 ASSERTION: the source replica on the evacuated node is STILL
	// present and NOT marked for deletion. No candidate target existed,
	// so redundancy must not be lowered by a drop-without-add.
	srcAfter := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-1.n1"}, srcAfter); err != nil {
		t.Fatalf("[F2] source replica on evacuated node was pruned with no replacement available "+
			"(drop-without-add — redundancy lowered): %v", err)
	}

	if !srcAfter.DeletionTimestamp.IsZero() {
		t.Fatalf("[F2] source replica on evacuated node marked for deletion with no replacement "+
			"available: %+v", srcAfter)
	}

	// No replacement should have been created — there is no free node.
	// The store still holds exactly 3 diskful replicas (n1+n2+n3); a 4th
	// would mean the placer tried to over-place onto a full cluster.
	replicas, err := st.Resources().ListByDefinition(ctx, "pvc-1")
	if err != nil {
		t.Fatalf("list replicas: %v", err)
	}

	if len(replicas) != 3 {
		t.Fatalf("[F2] expected exactly 3 replicas (no replacement on a full cluster), got %d: %+v",
			len(replicas), replicas)
	}
}
