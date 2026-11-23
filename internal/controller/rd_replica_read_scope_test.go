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

package controller

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
)

// unscopedResourceLists counts the Resource lists sent without a field
// selector: reads of every replica in the cluster.
type unscopedResourceLists struct {
	client.Reader

	n atomic.Int64
}

func (c *unscopedResourceLists) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	var o client.ListOptions
	o.ApplyOptions(opts)

	if _, ok := list.(*blockstoriov1alpha1.ResourceList); ok && o.FieldSelector == nil {
		c.n.Add(1)
	}

	return c.Reader.List(ctx, list, opts...)
}

// Every definition reconciles every few seconds and reads its replicas
// past the cache. Reading every Resource in the cluster there kept the
// controller at its CPU limit on a cluster with a few hundred replicas,
// and a burst of new volumes then waited for their DRBD identities. The
// read asks for the definition's replicas only.
func TestDefinitionReadsOnlyItsOwnReplicas(t *testing.T) {
	t.Parallel()

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

	cli := fake.NewClientBuilder().WithScheme(bug048Scheme(t)).WithObjects(objs...).
		WithIndex(&blockstoriov1alpha1.Resource{}, storek8s.FieldResourceDefinitionName, func(o client.Object) []string {
			return []string{o.(*blockstoriov1alpha1.Resource).Spec.ResourceDefinitionName}
		}).Build()

	reader := &unscopedResourceLists{Reader: cli}
	r := &ResourceDefinitionReconciler{Client: cli, APIReader: reader}

	got, err := r.listReplicasDirect(context.Background(), "pvc-a")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 3 {
		t.Errorf("replicas of pvc-a = %v, want 3", got)
	}

	if n := reader.n.Load(); n != 0 {
		t.Errorf("read every Resource in the cluster %d time(s) for one definition", n)
	}
}
