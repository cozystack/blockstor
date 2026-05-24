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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// i32 returns a pointer to the given value — terse helper for the
// nullable Status fields the allocator reads.
func i32(v int32) *int32 { return &v }

// seedReplica creates a Resource with a pre-stamped DRBDNodeID and an
// optional set of observed peer connections (PeerNodeName →
// PeerDRBDNodeID). Models a sibling whose controller-side id is
// allocated and whose satellite-observer has populated the
// connection table from `drbdsetup status -j`.
func seedReplica(
	ctx context.Context,
	t *testing.T,
	cli client.Client,
	rd, node string,
	ownID int32,
	peerIDs map[string]int32,
) {
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

	conns := make([]blockstoriov1alpha1.ResourceConnectionStatus, 0, len(peerIDs))
	for peerNode, id := range peerIDs {
		conns = append(conns, blockstoriov1alpha1.ResourceConnectionStatus{
			PeerNodeName:   peerNode,
			PeerDRBDNodeID: i32(id),
		})
	}

	r.Status = blockstoriov1alpha1.ResourceStatus{
		DRBDNodeID:  i32(ownID),
		Connections: conns,
	}
	if err := cli.Status().Update(ctx, r); err != nil {
		t.Fatalf("status update %s: %v", node, err)
	}
}

// newTarget builds the (not-yet-allocated) Resource the allocator is
// about to stamp an id onto. It is created in the cluster so the
// allocator's APIReader/Client list sees it, but carries no
// DRBDNodeID yet.
func newTarget(ctx context.Context, t *testing.T, cli client.Client, rd, node string) *blockstoriov1alpha1.Resource {
	t.Helper()

	create(ctx, t, cli, rd, node)

	target := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: rd + "." + node}, target); err != nil {
		t.Fatalf("get target %s: %v", node, err)
	}

	return target
}

// TestBug342NodeIDOccupiedUnion is the table-driven invariant-2 pin:
// the occupied node-id set for an RD is the UNION of every sibling's
// own Status.DRBDNodeID AND every observed PeerDRBDNodeID across all
// siblings — so a node-id that lives only as a zombie kernel
// connection slot (departed peer, forget-peer not yet run) is treated
// as taken and never handed to a new replica.
func TestBug342NodeIDOccupiedUnion(t *testing.T) {
	t.Parallel()

	type replica struct {
		node    string
		ownID   int32
		peerIDs map[string]int32
	}

	tests := []struct {
		name      string
		rd        string
		siblings  []replica
		wantTaken []int32
	}{
		{
			// Plain steady state: two siblings, each observing the
			// other. Union collapses to their own ids.
			name: "steady state two replicas",
			rd:   "pvc-steady",
			siblings: []replica{
				{node: "n0", ownID: 0, peerIDs: map[string]int32{"pvc-steady.n1": 1}},
				{node: "n1", ownID: 1, peerIDs: map[string]int32{"pvc-steady.n0": 0}},
			},
			wantTaken: []int32{0, 1},
		},
		{
			// Bug 342 core: n2 (id 2) was deleted, its Resource CRD
			// is gone, but surviving n0 and n1 still observe a live
			// kernel connection slot to the departed peer at
			// node-id 2 (forget-peer hasn't run). id 2 must stay
			// occupied even though no Resource carries it as own id.
			name: "zombie peer slot keeps id occupied",
			rd:   "pvc-zombie",
			siblings: []replica{
				{node: "n0", ownID: 0, peerIDs: map[string]int32{
					"pvc-zombie.n1":   1,
					"pvc-zombie.gone": 2, // departed peer, kernel slot lingers
				}},
				{node: "n1", ownID: 1, peerIDs: map[string]int32{
					"pvc-zombie.n0":   0,
					"pvc-zombie.gone": 2,
				}},
			},
			wantTaken: []int32{0, 1, 2},
		},
		{
			// Union across multiple siblings: only n0 still observes
			// the zombie slot (id 3); n1 already forgot it. The union
			// must still treat 3 as occupied because ANY sibling
			// seeing it is enough.
			name: "union across siblings any-one-sees-it",
			rd:   "pvc-union",
			siblings: []replica{
				{node: "n0", ownID: 0, peerIDs: map[string]int32{
					"pvc-union.n1":   1,
					"pvc-union.gone": 3, // only n0 still sees the zombie
				}},
				{node: "n1", ownID: 1, peerIDs: map[string]int32{
					"pvc-union.n0": 0,
				}},
			},
			wantTaken: []int32{0, 1, 3},
		},
		{
			// Once the slot is reclaimed everywhere (no sibling
			// observes it), the id drops out of the occupied set and
			// becomes free again — the normal-case reuse path.
			name: "id freed once no sibling observes it",
			rd:   "pvc-freed",
			siblings: []replica{
				{node: "n0", ownID: 0, peerIDs: map[string]int32{"pvc-freed.n1": 1}},
				{node: "n1", ownID: 1, peerIDs: map[string]int32{"pvc-freed.n0": 0}},
			},
			wantTaken: []int32{0, 1},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			scheme := newScheme(t)
			cli := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
				Build()

			for _, s := range tc.siblings {
				seedReplica(ctx, t, cli, tc.rd, s.node, s.ownID, s.peerIDs)
			}

			// A brand-new replica landing on a fresh node is the
			// allocation target.
			target := newTarget(ctx, t, cli, tc.rd, "newnode")

			rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

			got, err := rec.CollectTakenNodeIDsForTest(ctx, target)
			if err != nil {
				t.Fatalf("collectTakenNodeIDs: %v", err)
			}

			slices.Sort(got)

			want := slices.Clone(tc.wantTaken)
			slices.Sort(want)

			if !slices.Equal(got, want) {
				t.Errorf("taken set = %v, want %v", got, want)
			}
		})
	}
}

// TestBug342AllocatorSkipsZombieID drives the full allocator path and
// asserts a new replica gets the NEXT free id (not the zombie id) when
// a sibling's Status still observes the departed peer's node-id — the
// behaviour that prevents the Bug 342 wedge end-to-end.
func TestBug342AllocatorSkipsZombieID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	rd := "pvc-skip"

	// n0(id0) and n1(id1) survive; both still observe a zombie
	// connection slot to a departed peer at node-id 2.
	seedReplica(ctx, t, cli, rd, "n0", 0, map[string]int32{
		rd + ".n1":   1,
		rd + ".gone": 2,
	})
	seedReplica(ctx, t, cli, rd, "n1", 1, map[string]int32{
		rd + ".n0":   0,
		rd + ".gone": 2,
	})

	// A fresh replica relocates onto a new node. The allocator must
	// NOT reuse id 2 (still a live kernel slot) — it must pick 3.
	create(ctx, t, cli, rd, "n3")

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	target := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: rd + ".n3"}, target); err != nil {
		t.Fatalf("get target: %v", err)
	}

	if _, err := rec.EnsureDRBDIDsForTest(ctx, target, nil); err != nil {
		t.Fatalf("ensureDRBDIDs: %v", err)
	}

	got := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: rd + ".n3"}, got); err != nil {
		t.Fatalf("get result: %v", err)
	}

	if got.Status.DRBDNodeID == nil {
		t.Fatalf("DRBDNodeID not allocated")
	}

	if *got.Status.DRBDNodeID == 2 {
		t.Errorf("allocator reused zombie node-id 2 — Bug 342 wedge")
	}

	if *got.Status.DRBDNodeID != 3 {
		t.Errorf("allocator gave id %d, want next-free 3 (0,1 own + 2 zombie occupied)", *got.Status.DRBDNodeID)
	}
}

// TestBug342IDReusableAfterSlotReclaimed pins the self-healing tail:
// once no sibling observes the departed peer's slot anymore (kernel
// ran forget-peer, `destroy connection` pruned the cache), the freed
// id becomes the lowest free again and a new replica reuses it. This
// is the normal-case reuse the union must NOT permanently block.
func TestBug342IDReusableAfterSlotReclaimed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	rd := "pvc-reuse"

	// n0(id0) and n2(id2) survive; id 1's peer departed and the slot
	// has already been reclaimed everywhere (no Connections entry
	// references id 1).
	seedReplica(ctx, t, cli, rd, "n0", 0, map[string]int32{
		rd + ".n2": 2,
	})
	seedReplica(ctx, t, cli, rd, "n2", 2, map[string]int32{
		rd + ".n0": 0,
	})

	create(ctx, t, cli, rd, "n5")

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	target := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: rd + ".n5"}, target); err != nil {
		t.Fatalf("get target: %v", err)
	}

	if _, err := rec.EnsureDRBDIDsForTest(ctx, target, nil); err != nil {
		t.Fatalf("ensureDRBDIDs: %v", err)
	}

	got := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: rd + ".n5"}, got); err != nil {
		t.Fatalf("get result: %v", err)
	}

	if got.Status.DRBDNodeID == nil {
		t.Fatalf("DRBDNodeID not allocated")
	}

	// id 1 is free (own ids {0,2}, no observed peer references it) —
	// LowestFreeNodeID must hand out 1.
	if *got.Status.DRBDNodeID != 1 {
		t.Errorf("allocator gave id %d, want reclaimed lowest-free 1", *got.Status.DRBDNodeID)
	}
}
