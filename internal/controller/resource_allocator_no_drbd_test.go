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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// noDRBDFixture is one replica of a definition with the given stack in a
// group with the given stack, reconciled once by the Resource controller.
type noDRBDFixture struct {
	rgStack, rdStack []string
}

const (
	noDRBDGroup    = "local"
	noDRBDDef      = "pvc-local"
	noDRBDNode     = "n1"
	noDRBDResource = noDRBDDef + "." + noDRBDNode
)

func (f noDRBDFixture) objects() (*blockstoriov1alpha1.ResourceGroup, *blockstoriov1alpha1.ResourceDefinition, *blockstoriov1alpha1.Resource) {
	const (
		rgName   = noDRBDGroup
		rdName   = noDRBDDef
		nodeName = noDRBDNode
		resName  = noDRBDResource
	)

	rg := &blockstoriov1alpha1.ResourceGroup{
		ObjectMeta: metav1.ObjectMeta{Name: rgName},
		Spec: blockstoriov1alpha1.ResourceGroupSpec{
			SelectFilter: blockstoriov1alpha1.ResourceGroupSelectFilter{PlaceCount: 1, LayerStack: f.rgStack},
		},
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: rgName,
			LayerStack:        f.rdStack,
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024},
			},
		},
	}

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: resName},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               nodeName,
		},
	}

	return rg, rd, res
}

func (f noDRBDFixture) reconcile(t *testing.T) (ctrl.Result, *blockstoriov1alpha1.Resource, *blockstoriov1alpha1.ResourceDefinition) {
	t.Helper()

	scheme := newScheme(t)
	rg, rd, res := f.objects()
	cli := noDRBDClient(t, scheme, rg, rd, res)

	return reconcileNoDRBD(t, &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}, cli)
}

func noDRBDClient(t *testing.T, scheme *runtime.Scheme, objs ...client.Object) client.Client {
	t.Helper()

	return fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}, &blockstoriov1alpha1.ResourceDefinition{}).
		WithObjects(objs...).
		Build()
}

// reconcileNoDRBD reconciles the fixture's replica once and reads the
// replica and its definition back through cli.
func reconcileNoDRBD(
	t *testing.T, rec *controllerpkg.ResourceReconciler, cli client.Client,
) (ctrl.Result, *blockstoriov1alpha1.Resource, *blockstoriov1alpha1.ResourceDefinition) {
	t.Helper()

	ctx := context.Background()

	const (
		rdName  = noDRBDDef
		resName = noDRBDResource
	)

	result, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: resName}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	gotRes := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: resName}, gotRes); err != nil {
		t.Fatalf("get resource: %v", err)
	}

	gotRD := &blockstoriov1alpha1.ResourceDefinition{}
	if err := cli.Get(ctx, client.ObjectKey{Name: rdName}, gotRD); err != nil {
		t.Fatalf("get definition: %v", err)
	}

	return result, gotRes, gotRD
}

func assertNoDRBDIDs(t *testing.T, res *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition) {
	t.Helper()

	if res.Spec.DRBDPort != nil || res.Status.DRBDPort != nil {
		t.Errorf("DRBD port allocated: spec=%v status=%v", res.Spec.DRBDPort, res.Status.DRBDPort)
	}

	if res.Spec.DRBDNodeID != nil || res.Status.DRBDNodeID != nil {
		t.Errorf("DRBD node-id allocated: spec=%v status=%v", res.Spec.DRBDNodeID, res.Status.DRBDNodeID)
	}

	if res.Status.DRBDMinor != nil || rd.Spec.VolumeDefinitions[0].DRBDMinor != nil {
		t.Errorf("DRBD minor allocated: replica=%v definition=%v", res.Status.DRBDMinor, rd.Spec.VolumeDefinitions[0].DRBDMinor)
	}
}

// TestAllocatorSkipsDefinitionWithoutDRBD: LINSTOR gives a resource a
// DRBD port, node-id and minor only with its DRBD layer data, so a
// replica of a STORAGE or LUKS,STORAGE definition holds none. blockstor
// allocated all three, taking them from a finite per-node range.
func TestAllocatorSkipsDefinitionWithoutDRBD(t *testing.T) {
	t.Parallel()

	for name, stack := range map[string][]string{
		"storage":         {apiv1.LayerKindStorage},
		"luks on storage": {apiv1.LayerKindLUKS, apiv1.LayerKindStorage},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, res, rd := noDRBDFixture{rgStack: stack, rdStack: stack}.reconcile(t)
			assertNoDRBDIDs(t, res, rd)
		})
	}
}

// TestAllocatorStillAllocatesForDRBDDefinition is the control: an
// explicit DRBD stack, and an empty one in a group without a stack
// (the default), still get every ID.
func TestAllocatorStillAllocatesForDRBDDefinition(t *testing.T) {
	t.Parallel()

	for name, fx := range map[string]noDRBDFixture{
		"explicit DRBD": {rdStack: []string{apiv1.LayerKindDRBD, apiv1.LayerKindStorage}},
		"default stack": {},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, res, rd := fx.reconcile(t)

			if res.Spec.DRBDPort == nil || res.Spec.DRBDNodeID == nil || rd.Spec.VolumeDefinitions[0].DRBDMinor == nil {
				t.Errorf("DRBD IDs not allocated: port=%v node-id=%v minor=%v",
					res.Spec.DRBDPort, res.Spec.DRBDNodeID, rd.Spec.VolumeDefinitions[0].DRBDMinor)
			}
		})
	}
}

// TestAllocatorWaitsForLayerStackPin: a definition that leaves its stack
// to a group that sets one is not decided until the RD controller pins
// it, and the pin may keep the default for one that holds data. The
// allocator must neither allocate on a guess nor skip on one; it waits.
func TestAllocatorWaitsForLayerStackPin(t *testing.T) {
	t.Parallel()

	result, res, rd := noDRBDFixture{rgStack: []string{apiv1.LayerKindStorage}}.reconcile(t)

	assertNoDRBDIDs(t, res, rd)

	if result.RequeueAfter == 0 {
		t.Errorf("unpinned definition was not requeued: %+v", result)
	}
}

// countingReader counts the definition and group reads that reach the
// apiserver.
type countingReader struct {
	client.Reader

	defs, groups int
}

func (c *countingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	switch obj.(type) {
	case *blockstoriov1alpha1.ResourceDefinition:
		c.defs++
	case *blockstoriov1alpha1.ResourceGroup:
		c.groups++
	}

	return c.Reader.Get(ctx, key, obj, opts...)
}

// TestAllocatorReadsLayerStackFromCache: the layer-stack check runs on
// every live Resource reconcile, RD-watch and sibling fan-out included,
// so it must not add uncached apiserver reads. A stack is set once and
// never cleared, so the cached copy gives the same answer.
func TestAllocatorReadsLayerStackFromCache(t *testing.T) {
	t.Parallel()

	for name, fx := range map[string]noDRBDFixture{
		"stack on the definition": {rgStack: []string{apiv1.LayerKindStorage}, rdStack: []string{apiv1.LayerKindStorage}},
		"stack not pinned yet":    {rgStack: []string{apiv1.LayerKindStorage}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			scheme := newScheme(t)
			rg, rd, res := fx.objects()
			cli := noDRBDClient(t, scheme, rg, rd, res)
			live := &countingReader{Reader: cli}

			_, gotRes, gotRD := reconcileNoDRBD(t,
				&controllerpkg.ResourceReconciler{Client: cli, APIReader: live, Scheme: scheme}, cli)

			assertNoDRBDIDs(t, gotRes, gotRD)

			if live.defs != 0 || live.groups != 0 {
				t.Errorf("layer-stack check went to the apiserver: %d definition and %d group reads, want 0",
					live.defs, live.groups)
			}
		})
	}
}

// cacheMissingDefinitions is a cache that has not seen any definition
// yet; everything else reads through.
type cacheMissingDefinitions struct {
	client.Client
}

func (c cacheMissingDefinitions) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*blockstoriov1alpha1.ResourceDefinition); ok {
		return apierrors.NewNotFound(blockstoriov1alpha1.GroupVersion.WithResource("resourcedefinitions").GroupResource(), key.Name)
	}

	return c.Client.Get(ctx, key, obj, opts...)
}

// TestAllocatorFallsBackToLiveReadOnCacheMiss: a replica can reach the
// controller before the cache has its definition, which was created a
// moment earlier. Reading that miss as a definition that is gone would
// allocate DRBD IDs for a storage-only replica, so a miss goes to the
// apiserver.
func TestAllocatorFallsBackToLiveReadOnCacheMiss(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	rg, rd, res := noDRBDFixture{
		rgStack: []string{apiv1.LayerKindStorage},
		rdStack: []string{apiv1.LayerKindStorage},
	}.objects()

	live := noDRBDClient(t, scheme, rg, rd, res)

	_, gotRes, gotRD := reconcileNoDRBD(t,
		&controllerpkg.ResourceReconciler{Client: cacheMissingDefinitions{live}, APIReader: live, Scheme: scheme}, live)

	assertNoDRBDIDs(t, gotRes, gotRD)
}
