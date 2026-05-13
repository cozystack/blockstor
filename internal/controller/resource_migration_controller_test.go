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

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// TestMigrationReconcilerDeletesSrcAfterDstUpToDate pins the Option B
// (strict add-before-drop) tail of `linstor r td --migrate-from`:
// once the destination Resource's Status.Volumes report
// DiskState=UpToDate, the reconciler deletes the source Resource CRD
// and clears the BlockstorMigratingFrom prop on the destination.
//
// Two-phase drill, both run against the same fake client:
//
//  1. Reconcile while dst is still syncing (no Status.Volumes yet) —
//     src MUST live, prop MUST stay set. This is the load-bearing
//     redundancy-invariant guarantee Option B exists to deliver:
//     between REST stamp and dst UpToDate, the diskful count never
//     drops below the pre-migration value.
//  2. Stamp dst.Status.Volumes[0].DiskState = UpToDate via the
//     Status subresource and Reconcile again — src MUST be deleted,
//     dst.Spec.Props[BlockstorMigratingFrom] MUST be gone.
func TestMigrationReconcilerDeletesSrcAfterDstUpToDate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	srcCRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-mig.src-node"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-mig",
			NodeName:               "src-node",
			// Diskful; no DISKLESS flag.
		},
	}

	dstCRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-mig.dst-node"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-mig",
			NodeName:               "dst-node",
			StoragePool:            "zfs-thin",
			Props: map[string]string{
				"StorPoolName":                  "zfs-thin",
				controllerpkg.MigratingFromProp: "src-node",
			},
		},
		// No Status.Volumes yet — dst hasn't finished initial sync.
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		WithObjects(srcCRD, dstCRD).
		Build()

	rec := &controllerpkg.ResourceMigrationReconciler{
		Client: cli,
		Scheme: scheme,
	}

	dstKey := types.NamespacedName{Name: "pvc-mig.dst-node"}
	srcKey := types.NamespacedName{Name: "pvc-mig.src-node"}

	// ---- Phase 1: dst still syncing, src MUST persist ----
	got, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: dstKey})
	if err != nil {
		t.Fatalf("Reconcile (syncing): %v", err)
	}

	if got.RequeueAfter == 0 {
		t.Errorf("syncing dst must requeue; got %+v", got)
	}

	// src must still exist (the whole point of Option B).
	srcLive := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, srcKey, srcLive); err != nil {
		t.Fatalf("src pruned while dst was still syncing (Option A regression): %v", err)
	}

	dstLive := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, dstKey, dstLive); err != nil {
		t.Fatalf("dst missing: %v", err)
	}

	if dstLive.Spec.Props[controllerpkg.MigratingFromProp] != "src-node" {
		t.Errorf("migrating-from prop cleared prematurely: %q",
			dstLive.Spec.Props[controllerpkg.MigratingFromProp])
	}

	// ---- Phase 2: stamp dst UpToDate, reconcile again ----
	dstLive.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{
		{VolumeNumber: 0, DiskState: "UpToDate"},
	}
	if err := cli.Status().Update(ctx, dstLive); err != nil {
		t.Fatalf("status update: %v", err)
	}

	got, err = rec.Reconcile(ctx, ctrl.Request{NamespacedName: dstKey})
	if err != nil {
		t.Fatalf("Reconcile (UpToDate): %v", err)
	}

	if got.RequeueAfter != 0 {
		t.Errorf("UpToDate dst must not requeue; got %+v", got)
	}

	// src must be deleted.
	srcAfter := &blockstoriov1alpha1.Resource{}

	err = cli.Get(ctx, srcKey, srcAfter)
	if err == nil {
		t.Errorf("src Resource still present after dst UpToDate")
	} else if !errors.IsNotFound(err) {
		t.Errorf("Get src: unexpected err %v", err)
	}

	// dst migrating-from prop must be cleared.
	dstAfter := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, dstKey, dstAfter); err != nil {
		t.Fatalf("Get dst: %v", err)
	}

	if v, ok := dstAfter.Spec.Props[controllerpkg.MigratingFromProp]; ok {
		t.Errorf("migrating-from prop not cleared after src prune: %q", v)
	}
}

// TestMigrationReconcilerNoOpWithoutProp pins the cheap-path:
// Resources without BlockstorMigratingFrom never trigger any
// mutation. The reconciler watches every Resource event, so the
// no-op branch runs constantly; it MUST be free of side effects.
func TestMigrationReconcilerNoOpWithoutProp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-plain.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-plain",
			NodeName:               "n1",
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		WithObjects(res).
		Build()

	rec := &controllerpkg.ResourceMigrationReconciler{Client: cli, Scheme: scheme}

	got, err := rec.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-plain.n1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got.RequeueAfter != 0 {
		t.Errorf("plain Resource must not requeue; got %+v", got)
	}
}

// TestMigrationReconcilerSrcAlreadyGone covers the idempotency
// edge: a previous reconcile deleted src but crashed before
// clearing the migrating-from prop. The next pass must clear the
// prop without errors — it's the only way the resource recovers
// from a partial step.
func TestMigrationReconcilerSrcAlreadyGone(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	dst := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-mig2.dst"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-mig2",
			NodeName:               "dst",
			Props: map[string]string{
				controllerpkg.MigratingFromProp: "ghost-src",
			},
		},
		Status: blockstoriov1alpha1.ResourceStatus{
			Volumes: []blockstoriov1alpha1.ResourceVolumeStatus{
				{VolumeNumber: 0, DiskState: "UpToDate"},
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		WithObjects(dst).
		Build()

	rec := &controllerpkg.ResourceMigrationReconciler{Client: cli, Scheme: scheme}

	_, err := rec.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-mig2.dst"},
	})
	if err != nil {
		t.Fatalf("Reconcile with missing src: %v", err)
	}

	dstAfter := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-mig2.dst"}, dstAfter); err != nil {
		t.Fatalf("Get dst: %v", err)
	}

	if _, ok := dstAfter.Spec.Props[controllerpkg.MigratingFromProp]; ok {
		t.Errorf("prop not cleared after missing-src reconcile")
	}
}
