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
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestAutoDisklessPromoted: a DISKLESS replica that becomes InUse on
// a node with a viable storage pool gets promoted to diskful — the
// reconciler removes the DISKLESS flag and stamps StorPoolName on
// Spec.Props. The satellite reconciler picks up the change on its
// next reconcile and creates the LV / runs drbdadm attach.
func TestAutoDisklessPromoted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	st := store.NewInMemory()

	_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "stand",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
	})

	resCRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-promote.n1",
			Finalizers: []string{"blockstor.io.blockstor.io/resource"},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-promote",
			NodeName:               "n1",
			Flags:                  []string{apiv1.ResourceFlagDiskless},
		},
		Status: blockstoriov1alpha1.ResourceStatus{InUse: true},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		WithObjects(resCRD).
		Build()

	rec := &controllerpkg.ResourceReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	// Reconcile drives several converging passes (DRBD-id allocation
	// → Status update → requeue → auto-diskful → Spec update →
	// requeue → dispatchApply). Run until convergence so the final
	// state is what we assert on.
	for range 8 {
		_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-promote.n1"}})
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
	}

	got := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-promote.n1"}, got); err != nil {
		t.Fatalf("get: %v", err)
	}

	if slices.Contains(got.Spec.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS flag still present: %v", got.Spec.Flags)
	}

	if got.Spec.Props["StorPoolName"] != "stand" {
		t.Errorf("StorPoolName: got %q, want stand", got.Spec.Props["StorPoolName"])
	}
}

// TestAutoDisklessSkipsTiebreaker: a TIE_BREAKER witness must NEVER
// be auto-promoted — its whole point is the network-only quorum
// presence. Promoting would defeat the purpose and waste storage.
func TestAutoDisklessSkipsTiebreaker(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	st := store.NewInMemory()

	_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "stand",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
	})

	resCRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-tb.n1",
			Finalizers: []string{"blockstor.io.blockstor.io/resource"},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-tb",
			NodeName:               "n1",
			Flags:                  []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
		},
		Status: blockstoriov1alpha1.ResourceStatus{InUse: true},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		WithObjects(resCRD).
		Build()

	rec := &controllerpkg.ResourceReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-tb.n1"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &blockstoriov1alpha1.Resource{}
	_ = cli.Get(ctx, types.NamespacedName{Name: "pvc-tb.n1"}, got)

	if !slices.Contains(got.Spec.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS dropped from a TIE_BREAKER replica: %v", got.Spec.Flags)
	}
}

// TestAutoDisklessSkipsWhenNoPool: no storage pool on the hosting
// node → leave the diskless replica alone (a viable promotion isn't
// possible).
func TestAutoDisklessSkipsWhenNoPool(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	st := store.NewInMemory()
	// No StoragePool created — the node has no local storage.

	resCRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-nopool.n1",
			Finalizers: []string{"blockstor.io.blockstor.io/resource"},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-nopool",
			NodeName:               "n1",
			Flags:                  []string{apiv1.ResourceFlagDiskless},
		},
		Status: blockstoriov1alpha1.ResourceStatus{InUse: true},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		WithObjects(resCRD).
		Build()

	rec := &controllerpkg.ResourceReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-nopool.n1"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &blockstoriov1alpha1.Resource{}
	_ = cli.Get(ctx, types.NamespacedName{Name: "pvc-nopool.n1"}, got)

	if !slices.Contains(got.Spec.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS dropped despite no available pool: %v", got.Spec.Flags)
	}

	if got.Spec.Props["StorPoolName"] != "" {
		t.Errorf("StorPoolName set without a pool: %v", got.Spec.Props)
	}
}
