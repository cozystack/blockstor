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

package controllers

import (
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/naming"
)

// A snapshot's node list carries the registered spelling, Node-1, and the
// satellite started as node-1 takes it, both in the event filter and in the
// reconcile.
func TestSatelliteTakesASnapshotListingItsNodeInAnotherSpelling(t *testing.T) {
	t.Parallel()

	pred := snapshotNodePredicate("node-1")

	for spelling, want := range map[string]bool{"Node-1": true, "node-1": true, "node-2": false} {
		snap := &blockstoriov1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "rd.snap"},
			Spec:       blockstoriov1alpha1.SnapshotSpec{Nodes: []string{spelling}},
		}

		if got := pred.Create(event.CreateEvent{Object: snap}); got != want {
			t.Errorf("predicate for Nodes [%s] = %v, want %v", spelling, got, want)
		}

		if got := slices.ContainsFunc(snap.Spec.Nodes, func(n string) bool { return naming.Same(n, "node-1") }); got != want {
			t.Errorf("slices.ContainsFunc([%s], func(n string) bool { return naming.Same(n, node-1) }) = %v, want %v", spelling, got, want)
		}
	}
}

// The fetcher reads a peer's address from the Node CR, named from the
// folded name, while the snapshot lists the peer as Node-2.
func TestFetcherResolvesAPeerListedInTheRegisteredSpelling(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	peer := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-2"},
		Spec: blockstoriov1alpha1.NodeSpec{
			NetInterfaces: []blockstoriov1alpha1.NodeNetInterface{{Name: "default", Address: "192.0.2.2"}},
		},
	}

	f := &SnapshotFetcher{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(peer).Build(), NodeName: "node-1"}

	addr, err := f.resolvePeerAddr(t.Context(), "Node-2")
	if err != nil {
		t.Fatalf("resolvePeerAddr(Node-2): %v", err)
	}

	if addr == "" {
		t.Error("resolvePeerAddr(Node-2) gave no address")
	}
}

// Status written before under Node-1 and node-1 holds two entries for one
// node. The acknowledgment folds them into one, so a reader that keeps any
// of them, the first or the last, sees it.
func TestSuspendAckFoldsTheNodesStatusEntries(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.snap"},
		Spec:       blockstoriov1alpha1.SnapshotSpec{Nodes: []string{"Node-1"}, SuspendIO: true},
		Status: blockstoriov1alpha1.SnapshotStatus{NodeStatus: []blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "Node-1"}, {NodeName: "node-2"}, {NodeName: "node-1", CreateTimestamp: 7},
		}},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).WithObjects(snap).Build()
	r := &SnapshotReconciler{Client: cli, Config: Config{NodeName: "Node-1"}}

	if err := r.stampSnapshotPerNodeSuspendAcked(t.Context(), snap); err != nil {
		t.Fatalf("stamp: %v", err)
	}

	var got blockstoriov1alpha1.Snapshot
	if err := cli.Get(t.Context(), client.ObjectKeyFromObject(snap), &got); err != nil {
		t.Fatalf("get: %v", err)
	}

	var mine []blockstoriov1alpha1.SnapshotPerNodeStatus

	for _, e := range got.Status.NodeStatus {
		if naming.Same(e.NodeName, "node-1") {
			mine = append(mine, e)
		}
	}

	if len(mine) != 1 || !mine[0].SuspendIOAcked || mine[0].CreateTimestamp != 7 || len(got.Status.NodeStatus) != 2 {
		t.Errorf("status %+v; want one acknowledged entry for node-1 keeping its timestamp, beside node-2's", got.Status.NodeStatus)
	}
}

func TestReadyStampFoldsTheNodesStatusEntries(t *testing.T) {
	t.Parallel()

	got := upsertPerNodeStatus([]blockstoriov1alpha1.SnapshotPerNodeStatus{
		{NodeName: "Node-1"}, {NodeName: "node-1", SuspendIOAcked: true},
	}, "node-1", 9)

	if len(got) != 1 || !got[0].Ready || got[0].CreateTimestamp != 9 || !got[0].SuspendIOAcked {
		t.Errorf("status %+v; want one ready entry for node-1 keeping its acknowledgment", got)
	}
}

// An acknowledgment already on the first spelling of this node does not
// leave a later spelling unacknowledged: the reconcile folds the entries
// before it picks a phase, so the controller, which reads the last entry
// per node, sees the acknowledgment.
func TestAReconcileFoldsAliasesBeforeItPicksAPhase(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.snap"},
		Spec:       blockstoriov1alpha1.SnapshotSpec{Nodes: []string{"Node-1"}, SuspendIO: true},
		Status: blockstoriov1alpha1.SnapshotStatus{NodeStatus: []blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "Node-1", SuspendIOAcked: true}, {NodeName: "node-1"},
		}},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).WithObjects(snap).Build()
	r := &SnapshotReconciler{Client: cli, Config: Config{NodeName: "Node-1"}}

	if _, err := r.handleCreate(t.Context(), snap); err != nil {
		t.Fatalf("handleCreate: %v", err)
	}

	var got blockstoriov1alpha1.Snapshot
	if err := cli.Get(t.Context(), client.ObjectKeyFromObject(snap), &got); err != nil {
		t.Fatalf("get: %v", err)
	}

	if len(got.Status.NodeStatus) != 1 || !got.Status.NodeStatus[0].SuspendIOAcked {
		t.Errorf("status %+v; want one acknowledged entry for the node", got.Status.NodeStatus)
	}
}

// A ready first spelling does not stop the Ready stamp from folding a later,
// unready one: the idempotence check does not count a node with aliases as
// stamped.
func TestTheReadyStampFoldsAnAliasBehindAReadyEntry(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "rd.snap"},
		Spec:       blockstoriov1alpha1.SnapshotSpec{Nodes: []string{"Node-1"}},
		Status: blockstoriov1alpha1.SnapshotStatus{NodeStatus: []blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "Node-1", Ready: true, CreateTimestamp: 5}, {NodeName: "node-1"},
		}},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).WithObjects(snap).Build()
	r := &SnapshotReconciler{Client: cli, Config: Config{NodeName: "Node-1"}}

	if err := r.stampSnapshotPerNodeReady(t.Context(), snap, 9); err != nil {
		t.Fatalf("stamp: %v", err)
	}

	var got blockstoriov1alpha1.Snapshot
	if err := cli.Get(t.Context(), client.ObjectKeyFromObject(snap), &got); err != nil {
		t.Fatalf("get: %v", err)
	}

	if len(got.Status.NodeStatus) != 1 || !got.Status.NodeStatus[0].Ready {
		t.Errorf("status %+v; want one ready entry for the node", got.Status.NodeStatus)
	}
}
