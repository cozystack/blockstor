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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	"github.com/cozystack/blockstor/pkg/drbd"
)

// reconcileToSteadyState drives the reconciler until it stops asking
// to be requeued (or a bounded iteration cap is hit). The skip-init-sync
// stamp + identity allocation each requeue once, so convergence takes a
// few passes — this loops until the controller reports a steady state.
func reconcileToSteadyState(t *testing.T, ctx context.Context, rec *controllerpkg.ResourceReconciler, name string) {
	t.Helper()

	for i := range 20 {
		res, err := rec.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Name: name},
		})
		if err != nil {
			t.Fatalf("Reconcile[%d] %s: %v", i, name, err)
		}

		if !res.Requeue && res.RequeueAfter == 0 { //nolint:staticcheck // Requeue read mirrors the controller's own return shape
			return
		}
	}

	t.Fatalf("reconcile %s did not reach steady state within cap", name)
}

// TestSkipInitSyncFreshRDStampsSkipTrue pins invariant 1: a replica
// created into a genuinely-fresh RD (RD.Spec.Initialized nil, no peer
// holds data) is stamped Spec.SkipInitialSync=true so the satellite
// seeds the day0 skip → instant UpToDate, no SyncTarget.
func TestSkipInitSyncFreshRDStampsSkipTrue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	const (
		rdName       = "fresh-rd"
		nodeName     = "n1"
		resourceName = rdName + "." + nodeName
	)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024},
			},
		},
	}

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               nodeName,
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&blockstoriov1alpha1.Resource{},
			&blockstoriov1alpha1.ResourceDefinition{},
		).
		WithObjects(rd, res).
		Build()

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	reconcileToSteadyState(t, ctx, rec, resourceName)

	got := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: resourceName}, got); err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Spec.SkipInitialSync == nil {
		t.Fatalf("SkipInitialSync not stamped on fresh RD replica")
	}

	if !*got.Spec.SkipInitialSync {
		t.Errorf("fresh RD replica must get SkipInitialSync=true, got false")
	}

	// The RD must NOT have latched initialized — nothing holds data yet.
	gotRD := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(ctx, client.ObjectKey{Name: rdName}, gotRD); err != nil {
		t.Fatalf("get rd: %v", err)
	}

	if gotRD.Spec.Initialized != nil && *gotRD.Spec.Initialized {
		t.Errorf("RD must not be initialized while no replica holds data")
	}
}

// TestSkipInitSyncDataBearingPeerLatchesAndNewReplicaSyncs pins
// invariant 2 + the RD latch: once a diskful peer reports real data
// (UpToDate, GI past day0), the controller latches RD.Spec.Initialized
// and a NEWLY-added replica is stamped Spec.SkipInitialSync=false so it
// must SyncTarget instead of day0-skip.
func TestSkipInitSyncDataBearingPeerLatchesAndNewReplicaSyncs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	const (
		rdName  = "established-rd"
		newNode = "n2"
		newRes  = rdName + "." + newNode
		oldNode = "n1"
		oldRes  = rdName + "." + oldNode
	)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024},
			},
		},
	}

	// Existing diskful replica that holds real data: UpToDate with a
	// CurrentGI that is NOT the deterministic day0 (a genuine write).
	existing := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: oldRes},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               oldNode,
		},
		Status: blockstoriov1alpha1.ResourceStatus{
			Volumes: []blockstoriov1alpha1.ResourceVolumeStatus{
				{
					VolumeNumber: 0,
					DiskState:    string(drbd.DiskStateUpToDate),
					CurrentGI:    "DEADBEEFCAFEBABE", // real, past-day0
				},
			},
		},
	}

	// Freshly-added replica on a new node (relocate / extra replica).
	added := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: newRes},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               newNode,
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&blockstoriov1alpha1.Resource{},
			&blockstoriov1alpha1.ResourceDefinition{},
		).
		WithObjects(rd, existing, added).
		Build()

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	// Reconcile the existing data-bearing replica first — this latches
	// RD.Spec.Initialized=true via ensureSkipInitSyncDecision.
	reconcileToSteadyState(t, ctx, rec, oldRes)

	gotRD := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(ctx, client.ObjectKey{Name: rdName}, gotRD); err != nil {
		t.Fatalf("get rd: %v", err)
	}

	if gotRD.Spec.Initialized == nil || !*gotRD.Spec.Initialized {
		t.Fatalf("RD must latch initialized=true once a peer holds data, got %v", gotRD.Spec.Initialized)
	}

	// Now reconcile the freshly-added replica — it must be stamped
	// SkipInitialSync=false (must SyncTarget).
	reconcileToSteadyState(t, ctx, rec, newRes)

	gotAdded := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: newRes}, gotAdded); err != nil {
		t.Fatalf("get added: %v", err)
	}

	if gotAdded.Spec.SkipInitialSync == nil {
		t.Fatalf("SkipInitialSync not stamped on replica added to initialized RD")
	}

	if *gotAdded.Spec.SkipInitialSync {
		t.Errorf("replica added to an initialized RD must get SkipInitialSync=false (must sync), got true")
	}
}

// TestSkipInitSyncOfflineHolderStillForcesSync pins the CORE
// offline-safety fix: when RD.Spec.Initialized is ALREADY latched (set
// while the data-holder was online, persisted in Spec) and NO peer
// currently reports data (the sole data-holder is offline), a
// newly-added replica is STILL stamped SkipInitialSync=false. The
// decision reads the persisted RD latch, not live peer state, so the
// new replica can never come up UpToDate-empty.
func TestSkipInitSyncOfflineHolderStillForcesSync(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	const (
		rdName  = "offline-holder-rd"
		newNode = "n2"
		newRes  = rdName + "." + newNode
	)

	initialized := true
	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			// Latched true earlier, while the (now-offline) data-holder
			// was online and writing — persisted in Spec, survives the
			// holder going away (and survives backup/restore).
			Initialized: &initialized,
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024},
			},
		},
	}

	// The new replica is the ONLY Resource present — the data-holder's
	// Resource is gone/offline (no peer reports data). The naïve
	// live-state probe would see "no data peer" and wrongly skip.
	added := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: newRes},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               newNode,
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&blockstoriov1alpha1.Resource{},
			&blockstoriov1alpha1.ResourceDefinition{},
		).
		WithObjects(rd, added).
		Build()

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	reconcileToSteadyState(t, ctx, rec, newRes)

	got := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: newRes}, got); err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Spec.SkipInitialSync == nil {
		t.Fatalf("SkipInitialSync not stamped")
	}

	if *got.Spec.SkipInitialSync {
		t.Errorf("replica added to an initialized RD with an OFFLINE data-holder " +
			"must still get SkipInitialSync=false (offline-safety), got true")
	}
}

// TestSkipInitSyncAppendOnlyNotReStamped pins the append-only /
// settable-once contract: a Resource that already carries a
// SkipInitialSync value is NEVER re-stamped, even if the RD's
// initialized state would now imply a different value. This is what
// makes the field restore-safe and 3-way-merge-safe.
func TestSkipInitSyncAppendOnlyNotReStamped(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	const (
		rdName       = "preset-rd"
		nodeName     = "n1"
		resourceName = rdName + "." + nodeName
	)

	// RD is initialized (would imply skip=false for a new replica)...
	initialized := true
	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			Initialized: &initialized,
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024},
			},
		},
	}

	// ...but the Resource was already stamped skip=true (e.g. restored
	// from a backup taken when the RD was still fresh). The controller
	// MUST NOT overwrite it.
	preset := true
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               nodeName,
			SkipInitialSync:        &preset,
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&blockstoriov1alpha1.Resource{},
			&blockstoriov1alpha1.ResourceDefinition{},
		).
		WithObjects(rd, res).
		Build()

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	reconcileToSteadyState(t, ctx, rec, resourceName)

	got := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: resourceName}, got); err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Spec.SkipInitialSync == nil || !*got.Spec.SkipInitialSync {
		t.Errorf("preset SkipInitialSync=true must be preserved (append-only), got %v",
			got.Spec.SkipInitialSync)
	}
}

// TestSkipInitSyncStampedAlongsideNodeID pins the controller half of the
// CI run 26500468866 deadlock fix: when the parent RD is observable,
// allocateResourceSpecFields must stamp Spec.SkipInitialSync in the SAME
// allocation pass as the DRBD node-id / port — never a state where
// node-id is committed but SkipInitialSync is left nil. The satellite's
// node-id gate would pass on a node-id-stamped Resource, so if
// SkipInitialSync lagged behind node-id the satellite could seed with a
// nil decision (the deadlock). Driving the reconciler to steady state
// and asserting both are non-nil pins that they land together.
func TestSkipInitSyncStampedAlongsideNodeID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	const (
		rdName       = "together-rd"
		nodeName     = "n1"
		resourceName = rdName + "." + nodeName
	)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024},
			},
		},
	}

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               nodeName,
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&blockstoriov1alpha1.Resource{},
			&blockstoriov1alpha1.ResourceDefinition{},
		).
		WithObjects(rd, res).
		Build()

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	reconcileToSteadyState(t, ctx, rec, resourceName)

	got := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: resourceName}, got); err != nil {
		t.Fatalf("get: %v", err)
	}

	// The DRBD node-id must have been allocated...
	if got.Spec.DRBDNodeID == nil {
		t.Fatalf("DRBDNodeID not stamped — allocation never ran")
	}

	// ...and once node-id is stamped, SkipInitialSync MUST also be
	// non-nil. A node-id-stamped-but-skip-nil Resource is exactly the
	// window the satellite's seed could observe and deadlock on.
	if got.Spec.SkipInitialSync == nil {
		t.Fatalf("SkipInitialSync left nil while DRBDNodeID=%d was stamped — the deadlock window",
			*got.Spec.DRBDNodeID)
	}

	// Fresh RD (no data-bearing peer) → the decision is skip=true.
	if !*got.Spec.SkipInitialSync {
		t.Errorf("fresh RD replica must stamp SkipInitialSync=true alongside node-id, got false")
	}
}
