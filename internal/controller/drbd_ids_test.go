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

	// Two RDs, three nodes each, packed onto two physical nodes.
	// We expect each node's local replicas to have unique ports
	// among themselves, but ports on n1 are independent of n2.
	for _, rd := range []string{"pvc-A", "pvc-B"} {
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

	// Build per-node {port,minor} buckets and assert no collisions.
	portsByNode := map[string]map[int32]string{}
	minorsByNode := map[string]map[int32]string{}

	for i := range list.Items {
		node := list.Items[i].Spec.NodeName
		name := list.Items[i].Name

		if list.Items[i].Status.DRBDPort == nil || list.Items[i].Status.DRBDMinor == nil {
			t.Fatalf("%s: port/minor not allocated", name)
		}

		if portsByNode[node] == nil {
			portsByNode[node] = map[int32]string{}
		}

		if minorsByNode[node] == nil {
			minorsByNode[node] = map[int32]string{}
		}

		port := *list.Items[i].Status.DRBDPort
		if other, dup := portsByNode[node][port]; dup {
			t.Errorf("port %d collides on node %q: %s vs %s", port, node, other, name)
		}

		portsByNode[node][port] = name

		minor := *list.Items[i].Status.DRBDMinor
		if other, dup := minorsByNode[node][minor]; dup {
			t.Errorf("minor %d collides on node %q: %s vs %s", minor, node, other, name)
		}

		minorsByNode[node][minor] = name
	}
}

// TestDRBDPortRangePerNodeProp verifies that
// `DrbdOptions/TcpPortRange` on the Node CRD constrains the
// per-RD allocator (Bug 266 contract): the chosen port MUST sit in
// the INTERSECTION of every hosting node's range — divergent ports
// across peers of the same RD would break drbdadm adjust, so the
// allocator picks one value that's allocatable on every node.
//
// Bug 266 (per-RD allocation) replaces the old per-node semantics
// where each replica picked its own port from its node's range —
// that produced the divergence the live stand hit. The new
// invariant: same port on every replica of one RD; per-node
// ranges still constrain the choice via intersection.
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

	// Overlapping ranges: n1 allows 7000-7100, n2 allows 7050-7200.
	// Intersection = 7050-7100. The per-RD allocator must pick a
	// port in that band.
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

	const wantLow, wantHigh int32 = 7050, 7100

	var (
		seenPort     *int32
		seenPortNode string
	)

	for i := range list.Items {
		port := list.Items[i].Status.DRBDPort
		if port == nil {
			t.Fatalf("%s: port not allocated", list.Items[i].Name)
		}

		if *port < wantLow || *port > wantHigh {
			t.Errorf("%s port %d outside intersection [%d,%d]",
				list.Items[i].Spec.NodeName, *port, wantLow, wantHigh)
		}

		if seenPort == nil {
			seenPort = port
			seenPortNode = list.Items[i].Spec.NodeName

			continue
		}

		if *port != *seenPort {
			t.Errorf("per-RD port differs across peers: %s=%d vs %s=%d",
				seenPortNode, *seenPort, list.Items[i].Spec.NodeName, *port)
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

// TestDRBDMinorMultiVolumeRangeReserved pins the load-bearing
// multi-volume expansion in takenMinorsOnNode. A multi-volume RD
// consumes N consecutive minors (the .res renderer emits volume k
// at base+k). When a NEW Resource lands on the same node, the
// allocator MUST treat all N consecutive slots as taken — not just
// the base — otherwise the new replica picks base+1 and DRBD ends
// up with two devices claiming the same /dev/drbdN.
//
// This test seeds a 3-volume RD with DRBDMinor=1000 already
// allocated on n1, then drives the allocator for a fresh second RD
// on n1 and asserts the new minor is ≥ 1003 (skipping 1000, 1001,
// 1002 from the multi-volume RD).
//
// Without the expansion loop, the allocator would happily return
// 1001 here and corrupt DRBD's device map.
func TestDRBDMinorMultiVolumeRangeReserved(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	multiVolRD := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-multi"},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024},
				{VolumeNumber: 1, SizeKib: 1024},
				{VolumeNumber: 2, SizeKib: 1024},
			},
		},
	}
	if err := cli.Create(ctx, multiVolRD); err != nil {
		t.Fatalf("create multiVolRD: %v", err)
	}

	// Pre-allocated multi-volume Resource on n1, base minor = 1000
	// → reserves 1000, 1001, 1002.
	multiResName := "pvc-multi.n1"
	multiRes := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: multiResName},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-multi",
			NodeName:               "n1",
		},
	}
	if err := cli.Create(ctx, multiRes); err != nil {
		t.Fatalf("create multiRes: %v", err)
	}

	base := int32(1000)
	zero := int32(0)
	port := int32(7000)

	multiRes.Status = blockstoriov1alpha1.ResourceStatus{
		DRBDNodeID: &zero,
		DRBDPort:   &port,
		DRBDMinor:  &base,
	}
	if err := cli.Status().Update(ctx, multiRes); err != nil {
		t.Fatalf("status update multiRes: %v", err)
	}

	// New single-volume RD's replica also lands on n1. Its allocator
	// must skip 1000-1002 and pick ≥1003.
	create(ctx, t, cli, "pvc-fresh", "n1")

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}
	allocate(ctx, t, rec, cli, "pvc-fresh")

	freshRes := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: "pvc-fresh.n1"}, freshRes); err != nil {
		t.Fatalf("get fresh: %v", err)
	}

	if freshRes.Status.DRBDMinor == nil {
		t.Fatalf("fresh DRBDMinor not allocated")
	}

	got := *freshRes.Status.DRBDMinor

	if got < 1003 {
		t.Errorf("fresh minor: got %d, want ≥1003 (must skip multi-vol's 1000-1002 range)", got)
	}
}
