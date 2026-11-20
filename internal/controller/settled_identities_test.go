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
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// countingReader stands in for the uncached apiserver reader and
// counts the round trips a reconcile makes through it.
type countingReader struct {
	client.Reader

	calls atomic.Int64
}

func (c *countingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.calls.Add(1)

	return c.Reader.Get(ctx, key, obj, opts...)
}

func (c *countingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.calls.Add(1)

	return c.Reader.List(ctx, list, opts...)
}

// TestReconcileOfAllocatedReplicaSkipsApiserverReads pins the cost of
// the reconcile nearly every event produces: a replica whose node id,
// port and minors were allocated long ago. It used to read the
// Resource and its definition past the cache under the process-wide
// allocation lock on every pass, which on a cluster of a few hundred
// replicas kept the single worker busy enough that a new Resource
// waited for its own allocation.
func TestReconcileOfAllocatedReplicaSkipsApiserverReads(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	const (
		rdName       = "settled-rd"
		resourceName = rdName + ".n1"
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
			NodeName:               "n1",
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&blockstoriov1alpha1.Resource{},
			&blockstoriov1alpha1.ResourceDefinition{},
		).
		WithObjects(rd, res).
		Build()

	reader := &countingReader{Reader: cli}
	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme, APIReader: reader}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: resourceName}}

	// Allocate, then let every follow-up pass the allocation asks for
	// run, so the replica ends in its steady state.
	for range 5 {
		_, err := rec.Reconcile(ctx, req)
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
	}

	if reader.calls.Load() == 0 {
		t.Fatal("allocation made no uncached read; the test no longer exercises the allocator")
	}

	got := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(ctx, client.ObjectKey{Name: resourceName}, got); err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Spec.DRBDNodeID == nil || got.Spec.DRBDPort == nil || got.Status.DRBDMinor == nil {
		t.Fatalf("replica not allocated: spec=%+v status=%+v", got.Spec, got.Status)
	}

	reader.calls.Store(0)

	_, err := rec.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if n := reader.calls.Load(); n != 0 {
		t.Fatalf("reconcile of an allocated replica made %d uncached reads, want 0", n)
	}
}
