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
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
)

// unscopedPeerLists counts the Resource lists sent without a field
// selector: reads of every replica in the cluster.
type unscopedPeerLists struct {
	client.Reader

	n atomic.Int64
}

func (c *unscopedPeerLists) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	var o client.ListOptions
	o.ApplyOptions(opts)

	if _, ok := list.(*blockstoriov1alpha1.ResourceList); ok && o.FieldSelector == nil {
		c.n.Add(1)
	}

	return c.Reader.List(ctx, list, opts...) //nolint:wrapcheck // test double
}

// Every pass of every replica on a node reads its peers past the cache.
// Reading every Resource in the cluster there took the satellite's CPU
// on a node with a hundred replicas, and a new replica's passes waited
// behind it. The read asks for the replica's definition only, and still
// finds every peer.
func TestPassReadsOnlyItsDefinitionsPeers(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	objs := make([]client.Object, 0, 51)
	objs = append(objs, &blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "pvc-a"}})

	for i := range 50 {
		rd := fmt.Sprintf("pvc-%02d", i)
		if i < 3 {
			rd = "pvc-a"
		}

		objs = append(objs, &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s.n%d", rd, i)},
			Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: rd, NodeName: fmt.Sprintf("n%d", i)},
		})
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithIndex(&blockstoriov1alpha1.Resource{}, storek8s.FieldResourceDefinitionName, func(o client.Object) []string {
			return []string{o.(*blockstoriov1alpha1.Resource).Spec.ResourceDefinitionName}
		}).Build()

	reader := &unscopedPeerLists{Reader: cli}
	r := &ResourceReconciler{Client: cli, Config: Config{NodeName: "n0", APIReader: reader}}

	target := objs[1].(*blockstoriov1alpha1.Resource)

	peers, err := r.listPeerResources(context.Background(), target)
	if err != nil {
		t.Fatalf("listPeerResources: %v", err)
	}

	if len(peers) != 3 {
		t.Errorf("peers of pvc-a = %d, want 3 (the target and its two peers)", len(peers))
	}

	if n := reader.n.Load(); n != 0 {
		t.Errorf("read every Resource in the cluster %d time(s) for one pass", n)
	}
}
