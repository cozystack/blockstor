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
	"fmt"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	"github.com/cozystack/blockstor/pkg/store"
)

// driftResizePendingPrefix mirrors the production constant in
// pkg/rest/volume_definitions.go (`resizePendingAnnotationPrefix`).
// Re-declared here so a rename on the production side trips this
// test — the annotation key is the operator-visible contract that
// Bug 136 stamps and Bug 148 must also stamp on the kubectl-edit
// path.
const driftResizePendingPrefix = "bug136.blockstor.cozystack.io/resize-pending-size-kib-vol-"

// TestBug148ControllerReconcileStampsResizeOnDirectSpecEdit pins
// the Bug 148 reconciler-side resize-pending stamp.
//
// Bug 136's REST handler stamps the annotation on every grow
// routed through `PUT /v1/resource-definitions/.../volume-definitions/N`
// — but `kubectl edit resourcedefinition` mutates
// `spec.volumeDefinitions[].sizeKib` directly, bypassing the REST
// handler entirely. Without a reconciler-side equivalent, the
// satellite has no resize-pending breadcrumb and the on-disk block
// device stays at the old size indefinitely.
//
// This test seeds an RD with one 32 MiB volume + two child
// Resources, mutates the RD's `spec.volumeDefinitions[0].sizeKib`
// to 64 MiB directly via the fake client (simulating
// kubectl edit), fires Reconcile, and asserts the annotation
// `bug136.blockstor.cozystack.io/resize-pending-size-kib-vol-0=65536`
// lands on every child Resource.
func TestBug148ControllerReconcileStampsResizeOnDirectSpecEdit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	const (
		rdName  = "pvc-bug148"
		nodeA   = "node-a"
		nodeB   = "node-b"
		volNum  = int32(0)
		newSize = int64(64 * 1024) // 64 MiB
		expect  = "65536"
	)

	rdCRD := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: volNum, SizeKib: newSize},
			},
		},
	}

	resA := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rdName + "." + nodeA},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               nodeA,
		},
		Status: blockstoriov1alpha1.ResourceStatus{
			Volumes: []blockstoriov1alpha1.ResourceVolumeStatus{
				// Stale UsableKib reflects the pre-edit size; the
				// reconciler must detect the mismatch and stamp.
				{VolumeNumber: volNum, UsableKib: 32 * 1024},
			},
		},
	}
	resB := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rdName + "." + nodeB},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               nodeB,
		},
		Status: blockstoriov1alpha1.ResourceStatus{
			Volumes: []blockstoriov1alpha1.ResourceVolumeStatus{
				{VolumeNumber: volNum, UsableKib: 32 * 1024},
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}, &blockstoriov1alpha1.ResourceDefinition{}).
		WithObjects(rdCRD, resA, resB).
		Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  store.NewInMemory(),
	}

	_, err := rec.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: rdName},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	wantKey := driftResizePendingPrefix + fmt.Sprint(volNum)

	for _, nodeName := range []string{nodeA, nodeB} {
		got := &blockstoriov1alpha1.Resource{}

		err := cli.Get(ctx, types.NamespacedName{Name: rdName + "." + nodeName}, got)
		if err != nil {
			t.Fatalf("re-get Resource %s: %v", nodeName, err)
		}

		if got.Annotations == nil {
			t.Errorf("Resource %s: annotations nil after direct RD spec edit", nodeName)

			continue
		}

		if got.Annotations[wantKey] != expect {
			t.Errorf("Resource %s: annotation %q = %q, want %q",
				nodeName, wantKey, got.Annotations[wantKey], expect)
		}
	}
}

// TestBug148ControllerSkipsStampWhenStatusAlreadyAtTarget pins the
// idempotency contract: the reconciler MUST NOT re-stamp the
// annotation when the Resource's status.volumes[n].usableKib
// already equals the RD's target sizeKib. Without this, every
// periodic reconcile would re-touch every Resource and create
// thrash on the apiserver write path.
func TestBug148ControllerSkipsStampWhenStatusAlreadyAtTarget(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	const (
		rdName    = "pvc-bug148-idem"
		nodeA     = "node-a"
		volNum    = int32(0)
		sameSize  = int64(64 * 1024)
		legacyKey = driftResizePendingPrefix + "0"
	)

	rdCRD := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: volNum, SizeKib: sameSize},
			},
		},
	}

	resA := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name: rdName + "." + nodeA,
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               nodeA,
		},
		Status: blockstoriov1alpha1.ResourceStatus{
			Volumes: []blockstoriov1alpha1.ResourceVolumeStatus{
				// UsableKib already matches the RD spec.
				{VolumeNumber: volNum, UsableKib: sameSize},
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}, &blockstoriov1alpha1.ResourceDefinition{}).
		WithObjects(rdCRD, resA).
		Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  store.NewInMemory(),
	}

	_, err := rec.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: rdName},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, types.NamespacedName{Name: rdName + "." + nodeA}, got); err != nil {
		t.Fatalf("re-get: %v", err)
	}

	if _, has := got.Annotations[legacyKey]; has {
		t.Errorf("annotation %q stamped when status already at target — should be a no-op; got %+v",
			legacyKey, got.Annotations)
	}
}

// TestBug149OrphanResourceGarbageCollected pins the Bug 149 orphan
// cleanup: a Resource CRD whose parent RD is gone (kubectl delete rd
// --cascade=orphan) must be cleaned up by the controller-side
// Resource reconciler within a bounded reconcile window. The
// reconciler must mark the orphan for deletion so the satellite
// finalizer can run its Bug 107 annotation-fallback teardown chain.
//
// Test shape:
//  1. Seed a Resource CRD with no parent RD (the orphan).
//  2. The Resource carries the satellite finalizer (real
//     production state — satellite-side teardown is what reclaims
//     the backing storage).
//  3. Reconcile fires.
//  4. Assert the Resource now has a DeletionTimestamp — i.e. the
//     reconciler invoked Delete on the orphan. The satellite
//     reconciler is what actually strips the finalizer; that path
//     is covered separately in pkg/satellite/controllers/.
func TestBug149OrphanResourceGarbageCollected(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	const (
		rdName       = "pvc-bug149-orphan"
		nodeA        = "node-a"
		resourceName = rdName + "." + nodeA
	)

	// No parent RD in the fake client — this is the orphan state
	// that `kubectl delete rd --cascade=orphan` leaves behind.
	orphan := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name: resourceName,
			Finalizers: []string{
				// Satellite-side finalizer; the controller-side
				// orphan-detection path triggers Delete and the
				// satellite reconciler runs its teardown chain
				// with the Bug 107 annotation fallback.
				"blockstor.io.blockstor.io/satellite-resource",
			},
			Annotations: map[string]string{
				// Bug 107: stamped on the last successful apply
				// so the satellite teardown finds the volume
				// numbers even when the parent RD is gone.
				blockstoriov1alpha1.ResourceAnnotationVolumeNumbers: "0",
			},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               nodeA,
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		WithObjects(orphan).
		Build()

	rec := &controllerpkg.ResourceReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  store.NewInMemory(),
	}

	_, err := rec.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: resourceName},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &blockstoriov1alpha1.Resource{}

	err = cli.Get(ctx, types.NamespacedName{Name: resourceName}, got)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Either the satellite finalizer was stripped and
			// K8s GC reaped the orphan, OR the fake client
			// collapsed straight to deletion. Either way the
			// Bug 149 invariant ("orphan goes away") holds.
			return
		}

		t.Fatalf("re-get: %v", err)
	}

	if got.DeletionTimestamp.IsZero() {
		t.Errorf("orphan Resource still alive without DeletionTimestamp after reconcile; finalizers=%+v, annotations=%+v",
			got.Finalizers, got.Annotations)
	}
}

// TestBug149OrphanResourceWithDeletionTimestampAlsoGCs pins the
// re-entrance contract: a Resource with both a DeletionTimestamp
// AND no parent RD must still be picked up by the reconciler's
// orphan-detection path — running through Reconcile on this state
// must not error out and must not unnecessarily stamp a duplicate
// Delete request (which the fake client would reject as a no-op
// anyway, but we still want a clean return).
//
// This covers the race where `kubectl delete rd --cascade=orphan`
// followed by a transient apiserver flicker leaves a Resource
// already mid-delete (DeletionTimestamp set) but the satellite
// hasn't yet picked it up via the watch — the controller's
// reconcile passes should remain idempotent and harmless.
func TestBug149OrphanResourceWithDeletionTimestampAlsoGCs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	const (
		rdName       = "pvc-bug149-dying"
		nodeA        = "node-a"
		resourceName = rdName + "." + nodeA
	)

	now := metav1.Now()

	orphan := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:              resourceName,
			DeletionTimestamp: &now,
			Finalizers: []string{
				"blockstor.io.blockstor.io/satellite-resource",
			},
			Annotations: map[string]string{
				blockstoriov1alpha1.ResourceAnnotationVolumeNumbers: "0",
			},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               nodeA,
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		WithObjects(orphan).
		Build()

	rec := &controllerpkg.ResourceReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  store.NewInMemory(),
	}

	// Reconcile must remain a clean no-op on an already-dying orphan
	// — the satellite reconciler owns the finalizer-strip path.
	_, err := rec.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: resourceName},
	})
	if err != nil {
		t.Fatalf("Reconcile on already-deleting orphan: %v", err)
	}

	got := &blockstoriov1alpha1.Resource{}

	err = cli.Get(ctx, types.NamespacedName{Name: resourceName}, got)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return
		}

		t.Fatalf("re-get: %v", err)
	}

	if got.DeletionTimestamp.IsZero() {
		t.Errorf("DeletionTimestamp got cleared by reconcile — should never happen on an orphan that was already mid-delete")
	}
}
