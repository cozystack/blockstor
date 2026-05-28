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

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// Bug 266 (identity-to-spec model): DRBD ports are allocated PER NODE
// and live on Resource.Spec.DRBDPort. The invariant: two replicas ON
// THE SAME NODE must get DISTINCT ports (a collision breaks drbdadm
// adjust), while the SAME port number is freely reused across
// DIFFERENT nodes — that is exactly what lets a node host 1000+
// resources inside the 20000-20999 window instead of the whole cluster
// sharing it.
//
// This is the inverse of the pre-refactor per-RD contract (which
// forced one cluster-unique port across every peer of an RD): peers of
// one RD live on different nodes, so each picks its own per-node port.
func TestBug266DRBDPortPerNodeReuseAndUniqueness(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&blockstoriov1alpha1.Resource{},
			&blockstoriov1alpha1.ResourceDefinition{},
		).
		Build()

	// Pre-seed RD-A on w1+w2 with port 20000 (Spec.DRBDPort) — the
	// lowest port in the default window — so the per-node taken-set on
	// both w1 and w2 already holds {20000}.
	preSeedPort(ctx, t, cli, "pvc-bug266-rdA", "w1", 20000)
	preSeedPort(ctx, t, cli, "pvc-bug266-rdA", "w2", 20000)

	// RD-B lands on w1 and w3. On w1 the taken-set is {20000} (from
	// RD-A.w1) → must pick 20001. On w3 nothing is taken → may pick
	// 20000 (reuse across nodes is fine).
	rdB := &blockstoriov1alpha1.ResourceDefinition{}
	rdB.Name = "pvc-bug266-rdB"
	rdB.Spec.VolumeDefinitions = []blockstoriov1alpha1.ResourceDefinitionVolume{
		{VolumeNumber: 0, SizeKib: 1024},
	}

	if err := cli.Create(ctx, rdB); err != nil {
		t.Fatalf("create rdB: %v", err)
	}

	for _, node := range []string{"w1", "w3"} {
		create(ctx, t, cli, rdB.Name, node)
	}

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}
	allocate(ctx, t, rec, cli, rdB.Name)

	list := &blockstoriov1alpha1.ResourceList{}
	if err := cli.List(ctx, list); err != nil {
		t.Fatalf("list: %v", err)
	}

	// Build per-node port buckets across ALL resources and assert no
	// two resources on one node share a port.
	portsByNode := map[string]map[int32]string{}

	for i := range list.Items {
		node := list.Items[i].Spec.NodeName
		name := list.Items[i].Name

		if list.Items[i].Spec.DRBDPort == nil {
			t.Fatalf("%s: port not allocated on Spec", name)
		}

		if portsByNode[node] == nil {
			portsByNode[node] = map[int32]string{}
		}

		port := *list.Items[i].Spec.DRBDPort
		if other, dup := portsByNode[node][port]; dup {
			t.Errorf("SAME-NODE port collision: port %d on node %q held by both %s and %s",
				port, node, other, name)
		}

		portsByNode[node][port] = name
	}

	// RD-B.w1 must NOT be 20000 (RD-A.w1 holds it on the same node).
	rbw1 := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: "pvc-bug266-rdB.w1"}, rbw1); err != nil {
		t.Fatalf("get RD-B.w1: %v", err)
	}

	if rbw1.Spec.DRBDPort == nil || *rbw1.Spec.DRBDPort == 20000 {
		t.Errorf("RD-B.w1 port=%v, must differ from RD-A.w1's 20000 (same node)", rbw1.Spec.DRBDPort)
	}
}

// TestBug266DRBDPortRecordedOnSpec pins that the allocator writes the
// chosen per-node port onto Resource.Spec.DRBDPort (authoritative,
// restore-safe) and mirrors it onto Status for backward-compat readers.
func TestBug266DRBDPortRecordedOnSpec(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&blockstoriov1alpha1.Resource{},
			&blockstoriov1alpha1.ResourceDefinition{},
		).
		Build()

	rdName := "pvc-bug266-rd-persist"

	rdObj := &blockstoriov1alpha1.ResourceDefinition{}
	rdObj.Name = rdName
	rdObj.Spec.VolumeDefinitions = []blockstoriov1alpha1.ResourceDefinitionVolume{
		{VolumeNumber: 0, SizeKib: 1024},
	}

	if err := cli.Create(ctx, rdObj); err != nil {
		t.Fatalf("create rd: %v", err)
	}

	for _, node := range []string{"w1", "w2"} {
		create(ctx, t, cli, rdName, node)
	}

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}
	allocate(ctx, t, rec, cli, rdName)

	list := &blockstoriov1alpha1.ResourceList{}
	if err := cli.List(ctx, list); err != nil {
		t.Fatalf("list: %v", err)
	}

	for i := range list.Items {
		if list.Items[i].Spec.ResourceDefinitionName != rdName {
			continue
		}

		if list.Items[i].Spec.DRBDPort == nil {
			t.Errorf("replica %q: Spec.DRBDPort not allocated", list.Items[i].Name)

			continue
		}

		// Status mirror must equal the authoritative Spec value.
		if list.Items[i].Status.DRBDPort == nil ||
			*list.Items[i].Status.DRBDPort != *list.Items[i].Spec.DRBDPort {
			t.Errorf("replica %q: Status.DRBDPort mirror %v != Spec.DRBDPort %d",
				list.Items[i].Name, list.Items[i].Status.DRBDPort, *list.Items[i].Spec.DRBDPort)
		}
	}
}

// preSeedPort creates a Resource with a stamped Spec.DRBDPort so the
// per-node allocator sees it as a port-taken on the named node.
func preSeedPort(ctx context.Context, t *testing.T, cli client.Client, rdName, node string, port int32) {
	t.Helper()

	r := &blockstoriov1alpha1.Resource{}
	r.Name = rdName + "." + node
	r.Spec.ResourceDefinitionName = rdName
	r.Spec.NodeName = node
	r.Spec.DRBDPort = int32Ptr(port)
	r.Spec.DRBDNodeID = int32Ptr(0)

	if err := cli.Create(ctx, r); err != nil {
		t.Fatalf("preSeedPort create %s.%s: %v", rdName, node, err)
	}
}

func int32Ptr(v int32) *int32 { return &v }
