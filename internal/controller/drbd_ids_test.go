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
// `DrbdOptions/TcpPortRange` on the Node CRD overrides the
// controller's default range for replicas hosted on that node. Two
// nodes with disjoint ranges produce non-overlapping ports — that's
// the operator-affordance reason upstream went per-node in the
// first place.
func TestDRBDPortRangePerNodeProp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&blockstoriov1alpha1.Resource{}).Build()

	// n1: 7000-7000 (one slot), n2: 8000-8001
	for _, spec := range []struct {
		name, portRange string
	}{
		{"n1", "7000-7000"},
		{"n2", "8000-8001"},
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

	for _, node := range []string{"n1", "n2"} {
		create(ctx, t, cli, rd, node)
	}

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}
	allocate(ctx, t, rec, cli, rd)

	list := &blockstoriov1alpha1.ResourceList{}
	if err := cli.List(ctx, list); err != nil {
		t.Fatalf("list: %v", err)
	}

	for i := range list.Items {
		port := *list.Items[i].Status.DRBDPort

		switch list.Items[i].Spec.NodeName {
		case "n1":
			if port != 7000 {
				t.Errorf("n1 port: got %d, want 7000", port)
			}
		case "n2":
			if port < 8000 || port > 8001 {
				t.Errorf("n2 port: got %d, want 8000..8001", port)
			}
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
