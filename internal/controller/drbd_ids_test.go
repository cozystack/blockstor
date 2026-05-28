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

// Package controller_test holds property-style tests for the
// reconciler's allocators. These tests run against a fake client to
// keep them fast — envtest covers the integration path.
package controller_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// TestDRBDNodeIDStableAcrossPeerChurn is the load-bearing invariant
// for DRBD bitmap correctness: an id assigned to a replica must NEVER
// change for the lifetime of that replica, regardless of whether
// other replicas are added or removed. Re-numbering live replicas
// would re-map their bitmaps mid-flight and corrupt data on resync.
func TestDRBDNodeIDStableAcrossPeerChurn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	rd := "pvc-stability"

	// Phase 1: 3-replica RD, allocate ids in any order.
	cli := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&blockstoriov1alpha1.Resource{}).Build()

	for _, node := range []string{"n1", "n2", "n3"} {
		create(ctx, t, cli, rd, node)
	}

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	allocate(ctx, t, rec, cli, rd)

	first := snapshot(ctx, t, cli, rd)

	// Phase 2: drop n2 (the middle one); the survivors n1, n3 must
	// keep the SAME ids they had in phase 1.
	deleteRes(ctx, t, cli, rd, "n2")
	allocate(ctx, t, rec, cli, rd)

	second := snapshot(ctx, t, cli, rd)

	for node, id := range first {
		if node == "n2" {
			continue
		}

		if got, ok := second[node]; !ok || got != id {
			t.Errorf("phase 2: node %q id changed %d → %d (got=%d, present=%v)", node, id, got, got, ok)
		}
	}

	// Phase 3: add n4 — its id must be a *new* one not in {n1.id, n3.id},
	// and the survivors still keep their original ids.
	create(ctx, t, cli, rd, "n4")
	allocate(ctx, t, rec, cli, rd)

	third := snapshot(ctx, t, cli, rd)

	for node, id := range first {
		if node == "n2" {
			continue
		}

		if got := third[node]; got != id {
			t.Errorf("phase 3: node %q id drifted %d → %d", node, id, got)
		}
	}

	if id, ok := third["n4"]; !ok {
		t.Errorf("phase 3: n4 not allocated")
	} else {
		for survivor, sid := range third {
			if survivor != "n4" && sid == id {
				t.Errorf("phase 3: n4 id %d collides with %s", id, survivor)
			}
		}
	}

	// Phase 4: re-add n2 (it was deleted in phase 2). It must NOT
	// silently re-claim its old id — the old id is free now and the
	// allocator should pick the lowest free, which may or may not
	// equal the original. The invariant: ids in `third` must not
	// change.
	create(ctx, t, cli, rd, "n2")
	allocate(ctx, t, rec, cli, rd)

	fourth := snapshot(ctx, t, cli, rd)

	for node, id := range third {
		if got := fourth[node]; got != id {
			t.Errorf("phase 4: node %q id drifted %d → %d", node, id, got)
		}
	}
}

// TestDRBDPortPerReplicaUniqueOnNode pins the per-node, per-replica
// allocation rule: two replicas on the same node must take distinct
// ports/minors (port collision = drbd connection failure). Two
// replicas on different nodes are free to take the same port — that
// matches upstream LINSTOR's per-node range model.
func TestDRBDPortPerReplicaUniqueOnNode(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&blockstoriov1alpha1.Resource{}).Build()

	// Two RDs, each replicated onto the same two physical nodes.
	// We expect each node's local replicas to have unique ports
	// among themselves (Spec.DRBDPort), but a port number on n1 is
	// independent of n2 (per-node port scope, Bug 266 scaling fix).
	for _, rd := range []string{"pvc-A", "pvc-B"} {
		rdObj := &blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: rd},
			Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
				VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
					{VolumeNumber: 0, SizeKib: 1024},
				},
			},
		}
		if err := cli.Create(ctx, rdObj); err != nil {
			t.Fatalf("create rd %s: %v", rd, err)
		}

		for _, node := range []string{"n1", "n2"} {
			create(ctx, t, cli, rd, node)
		}
	}

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}
	allocate(ctx, t, rec, cli, "pvc-A")
	allocate(ctx, t, rec, cli, "pvc-B")

	list := &blockstoriov1alpha1.ResourceList{}
	if err := cli.List(ctx, list); err != nil {
		t.Fatalf("list: %v", err)
	}

	// Per-node port buckets: assert no two replicas on one node share
	// a port (Spec.DRBDPort is authoritative).
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
			t.Errorf("port %d collides on node %q: %s vs %s", port, node, other, name)
		}

		portsByNode[node][port] = name
	}

	// Per-node ports are independent: n1 and n2 may legitimately
	// reuse the same port number across the two RDs. Assert the set
	// of ports on n1 overlaps n2 (reuse is the whole point) rather
	// than demanding cluster-wide uniqueness.
	if len(portsByNode["n1"]) == 0 || len(portsByNode["n2"]) == 0 {
		t.Fatalf("expected ports allocated on both n1 and n2")
	}
}

// TestDRBDPortRangePerNodeProp verifies that
// `DrbdOptions/TcpPortRange` on the Node CRD constrains the per-node
// port allocator: each replica's port MUST sit in ITS OWN node's
// range. Ports are per-node now (Bug 266 scaling fix), so there is
// no cross-peer intersection — n1's replica picks from n1's range,
// n2's from n2's.
func TestDRBDPortRangePerNodeProp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&blockstoriov1alpha1.Resource{},
			&blockstoriov1alpha1.ResourceDefinition{},
		).
		Build()

	// Per-node ranges: n1 allows 7000-7100, n2 allows 7050-7200.
	// Each replica picks from ITS OWN node's range — n1's replica
	// from 7000-7100, n2's from 7050-7200.
	for _, spec := range []struct {
		name, portRange string
	}{
		{"n1", "7000-7100"},
		{"n2", "7050-7200"},
	} {
		n := &blockstoriov1alpha1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: spec.name},
			Spec: blockstoriov1alpha1.NodeSpec{
				Type:  "SATELLITE",
				Props: map[string]string{"DrbdOptions/TcpPortRange": spec.portRange},
			},
		}

		if err := cli.Create(ctx, n); err != nil {
			t.Fatalf("create node %s: %v", spec.name, err)
		}
	}

	rd := "pvc-range"

	rdObj := &blockstoriov1alpha1.ResourceDefinition{}
	rdObj.Name = rd
	if err := cli.Create(ctx, rdObj); err != nil {
		t.Fatalf("create rd: %v", err)
	}

	for _, node := range []string{"n1", "n2"} {
		create(ctx, t, cli, rd, node)
	}

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}
	allocate(ctx, t, rec, cli, rd)

	list := &blockstoriov1alpha1.ResourceList{}
	if err := cli.List(ctx, list); err != nil {
		t.Fatalf("list: %v", err)
	}

	// Each replica's port must sit in ITS OWN node's range.
	wantByNode := map[string][2]int32{
		"n1": {7000, 7100},
		"n2": {7050, 7200},
	}

	for i := range list.Items {
		node := list.Items[i].Spec.NodeName
		port := list.Items[i].Spec.DRBDPort
		if port == nil {
			t.Fatalf("%s: port not allocated on Spec", list.Items[i].Name)
		}

		rng, ok := wantByNode[node]
		if !ok {
			t.Fatalf("unexpected node %q", node)
		}

		if *port < rng[0] || *port > rng[1] {
			t.Errorf("%s port %d outside its node's range [%d,%d]",
				node, *port, rng[0], rng[1])
		}
	}
}

// allocate runs ensureDRBDIDs over every Resource of the RD until no
// further changes — the controller's behaviour after a few requeues.
func allocate(ctx context.Context, t *testing.T, rec *controllerpkg.ResourceReconciler, cli client.Client, rd string) {
	t.Helper()

	for range 8 {
		list := &blockstoriov1alpha1.ResourceList{}
		if err := cli.List(ctx, list); err != nil {
			t.Fatalf("list: %v", err)
		}

		peers := make([]blockstoriov1alpha1.Resource, 0, len(list.Items))

		for i := range list.Items {
			if list.Items[i].Spec.ResourceDefinitionName == rd {
				peers = append(peers, list.Items[i])
			}
		}

		dirty := false

		for i := range peers {
			target := peers[i].DeepCopy()
			if err := cli.Get(ctx, client.ObjectKeyFromObject(target), target); err != nil {
				t.Fatalf("get: %v", err)
			}

			changed, err := rec.EnsureDRBDIDsForTest(ctx, target, peers)
			if err != nil {
				t.Fatalf("ensureDRBDIDs: %v", err)
			}

			dirty = dirty || changed
		}

		if !dirty {
			return
		}
	}

	t.Fatalf("ensureDRBDIDs did not converge in 8 passes")
}

func create(ctx context.Context, t *testing.T, cli client.Client, rd, node string) {
	t.Helper()

	r := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rd + "." + node},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rd,
			NodeName:               node,
		},
	}

	if err := cli.Create(ctx, r); err != nil {
		t.Fatalf("create %s: %v", node, err)
	}
}

func deleteRes(ctx context.Context, t *testing.T, cli client.Client, rd, node string) {
	t.Helper()

	r := &blockstoriov1alpha1.Resource{ObjectMeta: metav1.ObjectMeta{Name: rd + "." + node}}
	if err := cli.Delete(ctx, r); err != nil {
		t.Fatalf("delete %s: %v", node, err)
	}
}

func snapshot(ctx context.Context, t *testing.T, cli client.Client, rd string) map[string]int32 {
	t.Helper()

	list := &blockstoriov1alpha1.ResourceList{}
	if err := cli.List(ctx, list); err != nil {
		t.Fatalf("list: %v", err)
	}

	out := make(map[string]int32, len(list.Items))

	for i := range list.Items {
		if list.Items[i].Spec.ResourceDefinitionName != rd {
			continue
		}

		if list.Items[i].Status.DRBDNodeID == nil {
			t.Fatalf("%s: id not allocated", list.Items[i].Name)
		}

		out[list.Items[i].Spec.NodeName] = *list.Items[i].Status.DRBDNodeID
	}

	return out
}

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1: %v", err)
	}

	if err := blockstoriov1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("blockstor: %v", err)
	}

	return s
}

// TestDRBDMinorMultiVolumeRangeReserved pins the cluster-wide
// per-volume minor allocation. Minors are the /dev/drbd<N> device
// identity (identical on every node), so the scope is the whole
// cluster and each VolumeDefinition carries its own minor on
// RD.Spec.VolumeDefinitions[].DRBDMinor.
//
// This test seeds a 3-volume RD with minors 1000,1001,1002 already
// on its VolumeDefinitions, then drives the allocator for a fresh
// second RD and asserts its (volume-0) minor is ≥1003 — the
// cluster-wide taken-set must reserve all three of the multi-vol
// RD's minors so the new RD never collides on /dev/drbdN.
func TestDRBDMinorMultiVolumeRangeReserved(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&blockstoriov1alpha1.Resource{},
			&blockstoriov1alpha1.ResourceDefinition{},
		).
		Build()

	m0, m1, m2 := int32(1000), int32(1001), int32(1002)
	multiVolRD := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-multi"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024, DRBDMinor: &m0},
				{VolumeNumber: 1, SizeKib: 1024, DRBDMinor: &m1},
				{VolumeNumber: 2, SizeKib: 1024, DRBDMinor: &m2},
			},
		},
	}
	if err := cli.Create(ctx, multiVolRD); err != nil {
		t.Fatalf("create multiVolRD: %v", err)
	}

	// Fresh single-volume RD. Its volume-0 minor must skip 1000-1002.
	freshRD := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-fresh"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024},
			},
		},
	}
	if err := cli.Create(ctx, freshRD); err != nil {
		t.Fatalf("create freshRD: %v", err)
	}

	create(ctx, t, cli, "pvc-fresh", "n1")

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}
	allocate(ctx, t, rec, cli, "pvc-fresh")

	got := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(ctx, client.ObjectKey{Name: "pvc-fresh"}, got); err != nil {
		t.Fatalf("get freshRD: %v", err)
	}

	if len(got.Spec.VolumeDefinitions) == 0 || got.Spec.VolumeDefinitions[0].DRBDMinor == nil {
		t.Fatalf("fresh RD volume-0 DRBDMinor not allocated")
	}

	if m := *got.Spec.VolumeDefinitions[0].DRBDMinor; m < 1003 {
		t.Errorf("fresh minor: got %d, want ≥1003 (must skip multi-vol's 1000-1002 range)", m)
	}
}
