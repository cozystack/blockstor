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
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// Bug 302: the controller-side allocator must run on EVERY Reconcile
// pass against a live Resource, not only when the apply chain reaches
// the runApply housekeeping branch. The satellite's
// `waitForControllerAllocation` gate stalls until
// Status.DRBD{NodeID,Port,Minor} land — anything that prevents
// allocation from happening (orphan-detection short-circuit, a
// transient error deeper in runApply, etc.) leaves the satellite stuck
// on "waiting for controller-side DRBD-ID allocation" forever.
//
// The three tests below pin the load-bearing invariant that
// reconciling a Resource ALWAYS results in stamped DRBD-IDs:
//
//  1. A `kubectl apply`-created Resource (no REST autoplace path)
//     gets IDs allocated on first Reconcile.
//  2. A Diskless-flag Resource (the migrate-disk destination-intent
//     shape) ALSO gets IDs allocated — the allocator MUST NOT gate on
//     the diskful flag set.
//  3. Re-reconciling a Resource with IDs already stamped is a no-op
//     (idempotent): no Status update, no resourceVersion churn.

// TestBug302AllocatesForKubectlAppliedResource pins that a Resource
// created via plain `kubectl apply` (no REST autoplace, no controller-
// driven create flow) gets DRBD-IDs allocated on the first Reconcile
// pass. The lifecycle tests construct Resources this way via the
// `rd_apply` helper.
func TestBug302AllocatesForKubectlAppliedResource(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	const (
		rdName       = "kubectl-applied-rd"
		nodeName     = "n1"
		resourceName = rdName + "." + nodeName
	)

	// Parent RD exists (`kubectl apply` creates it alongside the
	// Resource), but neither has any Status stamped — the
	// apiserver-side default.
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

	_, err := rec.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: resourceName},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &blockstoriov1alpha1.Resource{}

	err = cli.Get(ctx, client.ObjectKey{Name: resourceName}, got)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Status.DRBDNodeID == nil {
		t.Errorf("DRBDNodeID not allocated after Reconcile")
	}

	if got.Status.DRBDPort == nil {
		t.Errorf("DRBDPort not allocated after Reconcile")
	}

	if got.Status.DRBDMinor == nil {
		t.Errorf("DRBDMinor not allocated after Reconcile")
	}
}

// TestBug302AllocatesForDisklessResource pins that a Resource with
// `Spec.Flags=[DISKLESS]` (the migrate-disk destination-intent shape
// and the operator-issued `linstor r c --diskless` shape) ALSO gets
// DRBD-IDs allocated. Without this the dispatcher renders an .res
// claiming the local node is `node-id 0` (zero-default), drbdsetup
// new-resource burns that into kernel state at first adjust, and the
// next reconcile (after the allocator catches up) hits "peer node id
// cannot be my own node id" forever.
func TestBug302AllocatesForDisklessResource(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	const (
		rdName       = "diskless-intent-rd"
		nodeName     = "n3"
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
			Flags:                  []string{apiv1.ResourceFlagDiskless},
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

	_, err := rec.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: resourceName},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &blockstoriov1alpha1.Resource{}

	err = cli.Get(ctx, client.ObjectKey{Name: resourceName}, got)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Status.DRBDNodeID == nil {
		t.Errorf("DRBDNodeID not allocated for DISKLESS Resource")
	}

	if got.Status.DRBDPort == nil {
		t.Errorf("DRBDPort not allocated for DISKLESS Resource")
	}

	if got.Status.DRBDMinor == nil {
		t.Errorf("DRBDMinor not allocated for DISKLESS Resource")
	}
}

// TestBug302IdempotentReReconcile pins that re-reconciling a Resource
// with IDs already stamped is a true no-op: the apiserver-side
// resourceVersion stays stable across the second Reconcile call so
// re-reconcile bursts (RD-watch fan-out, sibling-watch fan-out,
// peer-changed bumps) don't churn the SSA owner or thrash the
// apiserver.
func TestBug302IdempotentReReconcile(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	const (
		rdName       = "idempotent-rd"
		nodeName     = "n2"
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

	// First Reconcile: allocates IDs.
	_, err := rec.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: resourceName},
	})
	if err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}

	first := &blockstoriov1alpha1.Resource{}

	err = cli.Get(ctx, client.ObjectKey{Name: resourceName}, first)
	if err != nil {
		t.Fatalf("get after first: %v", err)
	}

	if first.Status.DRBDNodeID == nil ||
		first.Status.DRBDPort == nil ||
		first.Status.DRBDMinor == nil {
		t.Fatalf("first Reconcile did not allocate all three IDs: nodeID=%v port=%v minor=%v",
			first.Status.DRBDNodeID, first.Status.DRBDPort, first.Status.DRBDMinor)
	}

	firstNodeID := *first.Status.DRBDNodeID
	firstPort := *first.Status.DRBDPort
	firstMinor := *first.Status.DRBDMinor
	firstVersion := first.ResourceVersion

	// Second Reconcile: IDs already stamped, allocation must be a
	// no-op. The resourceVersion stays stable because no SSA Patch
	// is issued.
	_, err = rec.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: resourceName},
	})
	if err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}

	second := &blockstoriov1alpha1.Resource{}

	err = cli.Get(ctx, client.ObjectKey{Name: resourceName}, second)
	if err != nil {
		t.Fatalf("get after second: %v", err)
	}

	if second.Status.DRBDNodeID == nil || *second.Status.DRBDNodeID != firstNodeID {
		t.Errorf("DRBDNodeID changed across idempotent Reconcile: first=%d second=%v",
			firstNodeID, second.Status.DRBDNodeID)
	}

	if second.Status.DRBDPort == nil || *second.Status.DRBDPort != firstPort {
		t.Errorf("DRBDPort changed across idempotent Reconcile: first=%d second=%v",
			firstPort, second.Status.DRBDPort)
	}

	if second.Status.DRBDMinor == nil || *second.Status.DRBDMinor != firstMinor {
		t.Errorf("DRBDMinor changed across idempotent Reconcile: first=%d second=%v",
			firstMinor, second.Status.DRBDMinor)
	}

	if second.ResourceVersion != firstVersion {
		t.Errorf("ResourceVersion bumped across idempotent Reconcile: %q → %q",
			firstVersion, second.ResourceVersion)
	}
}
