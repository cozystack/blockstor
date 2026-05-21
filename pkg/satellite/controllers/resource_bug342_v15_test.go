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
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// TestCurrentPeerDiskless_FlagsBranches pins the Bug 342 v15
// snapshot helper's discriminator semantics: DISKLESS or TIE_BREAKER
// in Spec.Flags -> true; no such flag (diskful) -> false. The
// snapshot is the only application-level record the tear-down path
// (tearDownRemovedPeers) consults to decide whether `drbdmeta
// forget-peer` is required after the peer departs.
func TestCurrentPeerDiskless_FlagsBranches(t *testing.T) {
	t.Parallel()

	peers := []blockstoriov1alpha1.Resource{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.worker-1"},
			Spec: blockstoriov1alpha1.ResourceSpec{
				NodeName: "worker-1",
				Flags:    []string{apiv1.ResourceFlagDiskless},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.worker-2"},
			Spec: blockstoriov1alpha1.ResourceSpec{
				NodeName: "worker-2",
				Flags:    []string{apiv1.ResourceFlagTieBreaker},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.worker-3"},
			Spec: blockstoriov1alpha1.ResourceSpec{
				NodeName: "worker-3",
				Flags:    []string{}, // diskful
			},
		},
	}

	got := currentPeerDiskless(peers)

	if !got["worker-1"] {
		t.Errorf("worker-1 DISKLESS: PeerDiskless=true want, got %v", got["worker-1"])
	}

	if !got["worker-2"] {
		t.Errorf("worker-2 TIE_BREAKER: PeerDiskless=true want, got %v", got["worker-2"])
	}

	if got["worker-3"] {
		t.Errorf("worker-3 diskful: PeerDiskless=false want, got %v", got["worker-3"])
	}

	if _, ok := got["worker-3"]; !ok {
		t.Errorf("worker-3 must have an explicit entry (false) so the tear-down path can read 'was-diskful', not default-to-true")
	}
}

// TestCurrentPeerDiskless_NilSafeOnEmptyPeers: nil-safe empty map
// for an isolated resource (no peers — single-replica case).
func TestCurrentPeerDiskless_NilSafeOnEmptyPeers(t *testing.T) {
	t.Parallel()

	got := currentPeerDiskless(nil)
	if got != nil {
		t.Errorf("currentPeerDiskless(nil) = %v, want nil", got)
	}

	got = currentPeerDiskless([]blockstoriov1alpha1.Resource{})
	if got != nil {
		t.Errorf("currentPeerDiskless([]) = %v, want nil", got)
	}
}

// TestCurrentPeerDiskless_DropsEmptyNodeName defensively drops peers
// whose Spec.NodeName hasn't fully landed yet (informer cache trail
// on a brand-new Resource). Stamping "" would later evaluate as
// "no entry for ”" which is harmless but pollutes the map.
func TestCurrentPeerDiskless_DropsEmptyNodeName(t *testing.T) {
	t.Parallel()

	peers := []blockstoriov1alpha1.Resource{
		{Spec: blockstoriov1alpha1.ResourceSpec{NodeName: "", Flags: []string{apiv1.ResourceFlagDiskless}}},
		{Spec: blockstoriov1alpha1.ResourceSpec{NodeName: "worker-1"}},
	}

	got := currentPeerDiskless(peers)
	if _, ok := got[""]; ok {
		t.Errorf("currentPeerDiskless must drop empty-NodeName entries; got %v", got)
	}

	if got["worker-1"] {
		t.Errorf("worker-1 diskful entry = true, want false")
	}
}

// TestStampAppliedPeerUIDs_StampsPeerDisklessMap pins the Bug 342
// v15 atomic-stamp contract: stampAppliedPeerUIDs writes both
// Status.AppliedPeerUIDs and Status.PeerDiskless in a single
// Status.Update round-trip. The PeerDiskless snapshot must land on
// the apiserver view before tearDownRemovedPeers ever reads it via
// the dispatcher's DesiredResource.
func TestStampAppliedPeerUIDs_StampsPeerDisklessMap(t *testing.T) {
	t.Parallel()

	scheme := newToggleDiskTestScheme(t)

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.worker-0"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               "worker-0",
			ResourceDefinitionName: "pvc-1",
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(res).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	r := &ResourceReconciler{
		Client: cli,
		Config: Config{
			NodeName:  "worker-0",
			APIReader: cli, // fake client doubles as APIReader in tests
		},
	}

	peerDiskless := map[string]bool{
		"worker-1": true,  // was DISKLESS
		"worker-2": true,  // was TIE_BREAKER
		"worker-3": false, // was diskful
	}

	err := r.stampAppliedPeerUIDs(context.Background(), res, nil, peerDiskless)
	if err != nil {
		t.Fatalf("stampAppliedPeerUIDs: %v", err)
	}

	var fresh blockstoriov1alpha1.Resource

	getErr := cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.worker-0"}, &fresh)
	if getErr != nil {
		t.Fatalf("post-stamp Get: %v", getErr)
	}

	if len(fresh.Status.PeerDiskless) != 3 {
		t.Fatalf("PeerDiskless len = %d, want 3 (full snapshot): %v",
			len(fresh.Status.PeerDiskless), fresh.Status.PeerDiskless)
	}

	if !fresh.Status.PeerDiskless["worker-1"] {
		t.Errorf("worker-1: PeerDiskless=true want, got false")
	}

	if !fresh.Status.PeerDiskless["worker-2"] {
		t.Errorf("worker-2: PeerDiskless=true want, got false")
	}

	if fresh.Status.PeerDiskless["worker-3"] {
		t.Errorf("worker-3: PeerDiskless=false want, got true")
	}
}

// TestStampAppliedPeerUIDs_UpsertOnlyOnPeerDiskless pins the v15
// CRITICAL invariant: entries for departed peers MUST be left intact
// when the stamper runs against a smaller peer set. The tear-down
// path reads the PeerDiskless map AFTER the peer is gone; if the
// stamper dropped the entry, tearDownRemovedPeers would default-to-
// true and run forget-peer regardless of the prior incarnation's
// flags — reproducing v9's Phase 2 regression on the first reconcile
// after peer departure.
func TestStampAppliedPeerUIDs_UpsertOnlyOnPeerDiskless(t *testing.T) {
	t.Parallel()

	scheme := newToggleDiskTestScheme(t)

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.worker-0"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               "worker-0",
			ResourceDefinitionName: "pvc-1",
		},
		Status: blockstoriov1alpha1.ResourceStatus{
			PeerDiskless: map[string]bool{
				// Pre-existing entry for a peer that has since departed.
				// The next reconcile sees a smaller peer set but MUST
				// NOT drop this entry — tearDownRemovedPeers will read
				// it on the same reconcile pass.
				"worker-departed": false,
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(res).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	r := &ResourceReconciler{
		Client: cli,
		Config: Config{
			NodeName:  "worker-0",
			APIReader: cli,
		},
	}

	// Snapshot of currently-alive peers — does NOT include worker-departed.
	currentSnapshot := map[string]bool{
		"worker-1": true,  // was DISKLESS
		"worker-3": false, // diskful
	}

	err := r.stampAppliedPeerUIDs(context.Background(), res, nil, currentSnapshot)
	if err != nil {
		t.Fatalf("stampAppliedPeerUIDs: %v", err)
	}

	var fresh blockstoriov1alpha1.Resource

	getErr := cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.worker-0"}, &fresh)
	if getErr != nil {
		t.Fatalf("post-stamp Get: %v", getErr)
	}

	// CRITICAL: worker-departed must still be in the map.
	if _, ok := fresh.Status.PeerDiskless["worker-departed"]; !ok {
		t.Fatalf("Bug 342 v15 CRITICAL: stampAppliedPeerUIDs dropped 'worker-departed' entry — tearDownRemovedPeers cannot read the discriminator value. Map: %v",
			fresh.Status.PeerDiskless)
	}

	if fresh.Status.PeerDiskless["worker-departed"] {
		t.Errorf("worker-departed: prior value=false (diskful) must be preserved verbatim, got true")
	}

	// New entries must also have landed.
	if !fresh.Status.PeerDiskless["worker-1"] {
		t.Errorf("worker-1: new DISKLESS entry not stamped")
	}

	if fresh.Status.PeerDiskless["worker-3"] {
		t.Errorf("worker-3: new diskful entry should be false, got true")
	}
}

// TestStampAppliedPeerUIDs_FlipsValueOnPeerRevival pins the
// reciprocal of the upsert-only invariant: if a peer DOES reappear,
// the snapshot must overwrite the stale entry with the current
// state. Otherwise a TIE_BREAKER-then-diskful conversion (Bug 261
// in-place flip) would forever read as TIE_BREAKER through the
// PeerDiskless lens.
func TestStampAppliedPeerUIDs_FlipsValueOnPeerRevival(t *testing.T) {
	t.Parallel()

	scheme := newToggleDiskTestScheme(t)

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.worker-0"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               "worker-0",
			ResourceDefinitionName: "pvc-1",
		},
		Status: blockstoriov1alpha1.ResourceStatus{
			PeerDiskless: map[string]bool{
				// Prior incarnation was TIE_BREAKER.
				"worker-1": true,
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(res).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	r := &ResourceReconciler{
		Client: cli,
		Config: Config{
			NodeName:  "worker-0",
			APIReader: cli,
		},
	}

	// New snapshot — worker-1 is now diskful (in-place TIE_BREAKER -> diskful flip).
	newSnapshot := map[string]bool{"worker-1": false}

	err := r.stampAppliedPeerUIDs(context.Background(), res, nil, newSnapshot)
	if err != nil {
		t.Fatalf("stampAppliedPeerUIDs: %v", err)
	}

	var fresh blockstoriov1alpha1.Resource

	getErr := cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.worker-0"}, &fresh)
	if getErr != nil {
		t.Fatalf("post-stamp Get: %v", getErr)
	}

	if fresh.Status.PeerDiskless["worker-1"] {
		t.Errorf("worker-1: TIE_BREAKER -> diskful flip must overwrite stale 'true' with 'false', got %v",
			fresh.Status.PeerDiskless["worker-1"])
	}
}
