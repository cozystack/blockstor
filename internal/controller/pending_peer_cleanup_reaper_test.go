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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// rdWithPendingMarker builds an RD CRD carrying a single
// pending-peer-cleanup annotation for `peer` stamped at `at`.
func rdWithPendingMarker(name, peer string, at time.Time) *blockstoriov1alpha1.ResourceDefinition {
	return &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Annotations: map[string]string{
				apiv1.PendingPeerCleanupAnnotationPrefix + peer: at.UTC().Format(time.RFC3339Nano),
			},
		},
	}
}

// TestReapPendingPeerCleanupDropsWhenAllSurvivorsAck pins the
// happy-path behaviour: a 3-node RD where two survivors have stamped
// Status.ClearedPeers[$departed] with a timestamp >= the pending
// marker. The reaper must strip the marker from the RD's Annotations.
func TestReapPendingPeerCleanupDropsWhenAllSurvivorsAck(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
			ConnectionStatus: apiv1.NodeTypeOnline,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	// Marker stamped 1s ago — still within the stale window.
	stampedAt := time.Now().Add(-1 * time.Second)
	rd := rdWithPendingMarker("rdA", "n3", stampedAt)

	// Survivor Resources on n1 and n2 with ACK timestamps in the
	// future of the marker.
	ackedAt := stampedAt.Add(500 * time.Millisecond).UTC().Format(time.RFC3339Nano)

	survivors := []blockstoriov1alpha1.Resource{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "rdA.n1"},
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: "rdA",
				NodeName:               "n1",
			},
			Status: blockstoriov1alpha1.ResourceStatus{
				ClearedPeers: map[string]string{"n3": ackedAt},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "rdA.n2"},
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: "rdA",
				NodeName:               "n2",
			},
			Status: blockstoriov1alpha1.ResourceStatus{
				ClearedPeers: map[string]string{"n3": ackedAt},
			},
		},
	}

	// Node CRDs (online).
	nodes := []blockstoriov1alpha1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "n1"},
			Status:     blockstoriov1alpha1.NodeStatus{ConnectionStatus: blockstoriov1alpha1.NodeConnectionStatusOnline},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "n2"},
			Status:     blockstoriov1alpha1.NodeStatus{ConnectionStatus: blockstoriov1alpha1.NodeConnectionStatusOnline},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(rd, &survivors[0], &survivors[1], &nodes[0], &nodes[1]).
		Build()

	// Seed RD in the Store too so the production drop path
	// (Store.PatchResourceDefinitionSpec) is the one exercised.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:        "rdA",
		Annotations: rd.Annotations,
	}); err != nil {
		t.Fatalf("seed RD in store: %v", err)
	}

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	if err := rec.ReapPendingPeerCleanup(ctx, rd); err != nil {
		t.Fatalf("ReapPendingPeerCleanup: %v", err)
	}

	// Re-Get the RD from the Store — the marker should be gone.
	got, err := st.ResourceDefinitions().Get(ctx, "rdA")
	if err != nil {
		t.Fatalf("Get RD from store: %v", err)
	}

	if _, present := got.Annotations[apiv1.PendingPeerCleanupAnnotationPrefix+"n3"]; present {
		t.Errorf("pending marker for n3 still present in store: %v", got.Annotations)
	}
}

// TestReapPendingPeerCleanupKeepsWhenAckMissing pins the negative
// case: when at least one online survivor has NOT stamped ClearedPeers
// the reaper must leave the marker in place so the allocator gate
// keeps holding off the new-incarnation allocation.
func TestReapPendingPeerCleanupKeepsWhenAckMissing(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	stampedAt := time.Now().Add(-1 * time.Second)
	rd := rdWithPendingMarker("rdB", "n3", stampedAt)

	// n1 ACK'd; n2 has not.
	ackedAt := stampedAt.Add(500 * time.Millisecond).UTC().Format(time.RFC3339Nano)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			rd,
			&blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: "rdB.n1"},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: "rdB",
					NodeName:               "n1",
				},
				Status: blockstoriov1alpha1.ResourceStatus{
					ClearedPeers: map[string]string{"n3": ackedAt},
				},
			},
			&blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: "rdB.n2"},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: "rdB",
					NodeName:               "n2",
				},
				// no ClearedPeers entry
			},
			&blockstoriov1alpha1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "n1"},
				Status:     blockstoriov1alpha1.NodeStatus{ConnectionStatus: blockstoriov1alpha1.NodeConnectionStatusOnline},
			},
			&blockstoriov1alpha1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "n2"},
				Status:     blockstoriov1alpha1.NodeStatus{ConnectionStatus: blockstoriov1alpha1.NodeConnectionStatusOnline},
			},
		).
		Build()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:        "rdB",
		Annotations: rd.Annotations,
	}); err != nil {
		t.Fatalf("seed RD in store: %v", err)
	}

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	if err := rec.ReapPendingPeerCleanup(ctx, rd); err != nil {
		t.Fatalf("ReapPendingPeerCleanup: %v", err)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "rdB")
	if err != nil {
		t.Fatalf("Get RD from store: %v", err)
	}

	if _, present := got.Annotations[apiv1.PendingPeerCleanupAnnotationPrefix+"n3"]; !present {
		t.Errorf("pending marker for n3 missing — should have been kept (n2 has not ACK'd)")
	}
}

// TestReapPendingPeerCleanupDropsStale pins the escape-hatch
// behaviour: a marker older than PendingPeerCleanupStaleWindow gets
// dropped even when no survivors have ACK'd, so a wedged satellite
// can never indefinitely hold the allocator gate.
func TestReapPendingPeerCleanupDropsStale(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	// Marker stamped 30s ago — well past the 10s window.
	stampedAt := time.Now().Add(-30 * time.Second)
	rd := rdWithPendingMarker("rdC", "n3", stampedAt)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			rd,
			&blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: "rdC.n1"},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: "rdC",
					NodeName:               "n1",
				},
				// no ClearedPeers — survivor never ACK'd
			},
			&blockstoriov1alpha1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "n1"},
				Status:     blockstoriov1alpha1.NodeStatus{ConnectionStatus: blockstoriov1alpha1.NodeConnectionStatusOnline},
			},
		).
		Build()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:        "rdC",
		Annotations: rd.Annotations,
	}); err != nil {
		t.Fatalf("seed RD in store: %v", err)
	}

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	if err := rec.ReapPendingPeerCleanup(ctx, rd); err != nil {
		t.Fatalf("ReapPendingPeerCleanup: %v", err)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "rdC")
	if err != nil {
		t.Fatalf("Get RD from store: %v", err)
	}

	if _, present := got.Annotations[apiv1.PendingPeerCleanupAnnotationPrefix+"n3"]; present {
		t.Errorf("stale marker for n3 should have been dropped: %v", got.Annotations)
	}
}

// TestReapPendingPeerCleanupSkipsOfflineSurvivor pins the OFFLINE-skip
// path: an offline survivor's missing ACK MUST NOT block the reap.
// Otherwise a node drain or transient outage would deadlock new-replica
// allocation on the surviving cluster.
func TestReapPendingPeerCleanupSkipsOfflineSurvivor(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	stampedAt := time.Now().Add(-1 * time.Second)
	rd := rdWithPendingMarker("rdD", "n3", stampedAt)

	ackedAt := stampedAt.Add(500 * time.Millisecond).UTC().Format(time.RFC3339Nano)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			rd,
			&blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: "rdD.n1"},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: "rdD",
					NodeName:               "n1",
				},
				Status: blockstoriov1alpha1.ResourceStatus{
					ClearedPeers: map[string]string{"n3": ackedAt},
				},
			},
			&blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: "rdD.n2"},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: "rdD",
					NodeName:               "n2",
				},
				// no ClearedPeers — but n2 is OFFLINE
			},
			&blockstoriov1alpha1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "n1"},
				Status:     blockstoriov1alpha1.NodeStatus{ConnectionStatus: blockstoriov1alpha1.NodeConnectionStatusOnline},
			},
			&blockstoriov1alpha1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "n2"},
				Status:     blockstoriov1alpha1.NodeStatus{ConnectionStatus: blockstoriov1alpha1.NodeConnectionStatusOffline},
			},
		).
		Build()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:        "rdD",
		Annotations: rd.Annotations,
	}); err != nil {
		t.Fatalf("seed RD in store: %v", err)
	}

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	if err := rec.ReapPendingPeerCleanup(ctx, rd); err != nil {
		t.Fatalf("ReapPendingPeerCleanup: %v", err)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "rdD")
	if err != nil {
		t.Fatalf("Get RD from store: %v", err)
	}

	if _, present := got.Annotations[apiv1.PendingPeerCleanupAnnotationPrefix+"n3"]; present {
		t.Errorf("OFFLINE survivor's missing ACK should have been skipped; marker kept: %v", got.Annotations)
	}
}

// TestPendingPeerCleanupGateActiveFresh pins the allocator-gate
// predicate's active-marker branch: a fresh (<10s) marker for the
// target node returns true.
func TestPendingPeerCleanupGateActiveFresh(t *testing.T) {
	t.Parallel()

	rd := rdWithPendingMarker("rdE", "n3", time.Now().Add(-1*time.Second))

	if !controllerpkg.PendingPeerCleanupGateActive(rd, "n3") {
		t.Error("expected gate to be ACTIVE for fresh marker on n3")
	}
}

// TestPendingPeerCleanupGateActiveStale pins the stale-window
// escape-hatch behaviour: a marker older than 10s no longer holds
// the gate, so a wedged satellite can't block allocation forever.
func TestPendingPeerCleanupGateActiveStale(t *testing.T) {
	t.Parallel()

	rd := rdWithPendingMarker("rdF", "n3", time.Now().Add(-30*time.Second))

	if controllerpkg.PendingPeerCleanupGateActive(rd, "n3") {
		t.Error("expected gate to be INACTIVE for stale marker (>10s)")
	}
}

// TestPendingPeerCleanupGateActiveDifferentNode pins per-peer
// scoping: a marker for n3 does NOT gate allocation of a new
// Resource on n2.
func TestPendingPeerCleanupGateActiveDifferentNode(t *testing.T) {
	t.Parallel()

	rd := rdWithPendingMarker("rdG", "n3", time.Now().Add(-1*time.Second))

	if controllerpkg.PendingPeerCleanupGateActive(rd, "n2") {
		t.Error("expected gate to be INACTIVE for n2 when only n3 has a marker")
	}
}
