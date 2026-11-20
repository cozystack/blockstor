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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// listCountingReader counts the lists sent past the cache.
type listCountingReader struct {
	client.Reader

	lists atomic.Int64
}

func (c *listCountingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.lists.Add(1)

	return c.Reader.List(ctx, list, opts...)
}

// burstObjects builds definitions with one volume and a replica on each
// node, none of them allocated yet: what a burst of new volumes leaves
// for the controller.
func burstObjects(definitions int, nodes ...string) []client.Object {
	objs := make([]client.Object, 0, definitions*(len(nodes)+1))

	for i := range definitions {
		rdName := fmt.Sprintf("pvc-burst-%02d", i)

		objs = append(objs, &blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: rdName, UID: types.UID("rd-" + rdName)},
			Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
				VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{{VolumeNumber: 0, SizeKib: 1024}},
			},
		})

		for _, node := range nodes {
			name := rdName + "." + node
			objs = append(objs, &blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("res-" + name)},
				Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: rdName, NodeName: node},
			})
		}
	}

	return objs
}

func newBurstClient(t *testing.T, objs []client.Object) client.WithWatch {
	t.Helper()

	return fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}, &blockstoriov1alpha1.ResourceDefinition{}).
		WithObjects(objs...).
		Build()
}

// TestAllocationReadsTakenSetsFromCache pins where the allocator reads
// the node ids, ports and minors already taken. Listing every Resource
// and definition from the apiserver on each allocation, under the lock
// that serialises allocations, made a burst of new replicas wait for
// their DRBD identities one list after another: on a cluster with
// about 400 replicas, 16 new volumes left replicas waiting more than a
// minute, and the CSI provisioner gave up on them.
func TestAllocationReadsTakenSetsFromCache(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cli := newBurstClient(t, burstObjects(1, "w1", "w2"))
	apiReader := &listCountingReader{Reader: cli}
	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: cli.Scheme(), APIReader: apiReader}

	var target blockstoriov1alpha1.Resource
	if err := cli.Get(ctx, client.ObjectKey{Name: "pvc-burst-00.w1"}, &target); err != nil {
		t.Fatalf("get: %v", err)
	}

	if _, err := rec.EnsureDRBDIDsForTest(ctx, &target, nil); err != nil {
		t.Fatalf("allocate: %v", err)
	}

	if err := cli.Get(ctx, client.ObjectKey{Name: "pvc-burst-00.w1"}, &target); err != nil {
		t.Fatalf("get: %v", err)
	}

	if target.Spec.DRBDNodeID == nil || target.Spec.DRBDPort == nil {
		t.Fatalf("replica not allocated: node id %v, port %v", target.Spec.DRBDNodeID, target.Spec.DRBDPort)
	}

	if n := apiReader.lists.Load(); n != 0 {
		t.Errorf("allocation listed past the cache %d times, want 0", n)
	}
}

// TestAllocationSafeWhileCacheLags allocates a burst of replicas, all
// at once, against a cache that shows none of the allocator's own
// writes: the worst case of the cache trailing them. Every replica of a
// definition still needs its own node id, every replica on a node its
// own port, and every definition its own minor.
func TestAllocationSafeWhileCacheLags(t *testing.T) {
	t.Parallel()

	const definitions = 12

	ctx := context.Background()
	nodes := []string{"w1", "w2", "w3"}
	objs := burstObjects(definitions, nodes...)

	live := newBurstClient(t, objs)
	frozen := newBurstClient(t, burstObjects(definitions, nodes...))

	// Reads through the reconciler's client come from the frozen copy,
	// as from a cache that has not seen any write yet; writes land on
	// the live store, which the APIReader reads.
	cache := interceptor.NewClient(live, interceptor.Funcs{
		Get: func(ctx context.Context, _ client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			return frozen.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, _ client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			return frozen.List(ctx, list, opts...)
		},
	})

	rec := &controllerpkg.ResourceReconciler{Client: cache, Scheme: live.Scheme(), APIReader: live}

	var wg sync.WaitGroup

	errs := make(chan error, len(objs))

	for _, obj := range objs {
		if _, ok := obj.(*blockstoriov1alpha1.Resource); !ok {
			continue
		}

		wg.Go(func() {
			var target blockstoriov1alpha1.Resource
			if err := live.Get(ctx, client.ObjectKeyFromObject(obj), &target); err != nil {
				errs <- err

				return
			}

			if _, err := rec.EnsureDRBDIDsForTest(ctx, &target, nil); err != nil {
				errs <- err
			}
		})
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("allocate: %v", err)
	}

	assertBurstUnique(ctx, t, live, definitions)
}

func assertBurstUnique(ctx context.Context, t *testing.T, cli client.Client, definitions int) {
	t.Helper()

	var replicas blockstoriov1alpha1.ResourceList
	if err := cli.List(ctx, &replicas); err != nil {
		t.Fatalf("list: %v", err)
	}

	ports := map[string]string{}
	nodeIDs := map[string]string{}

	for i := range replicas.Items {
		res := &replicas.Items[i]
		if res.Spec.DRBDPort == nil || res.Spec.DRBDNodeID == nil {
			t.Errorf("%s: not allocated", res.Name)

			continue
		}

		portKey := fmt.Sprintf("%s/%d", res.Spec.NodeName, *res.Spec.DRBDPort)
		if other, dup := ports[portKey]; dup {
			t.Errorf("%s and %s share port %d on %s", res.Name, other, *res.Spec.DRBDPort, res.Spec.NodeName)
		}

		ports[portKey] = res.Name

		idKey := fmt.Sprintf("%s/%d", res.Spec.ResourceDefinitionName, *res.Spec.DRBDNodeID)
		if other, dup := nodeIDs[idKey]; dup {
			t.Errorf("%s and %s share node id %d", res.Name, other, *res.Spec.DRBDNodeID)
		}

		nodeIDs[idKey] = res.Name
	}

	minors := map[int32]string{}

	for i := range definitions {
		var rd blockstoriov1alpha1.ResourceDefinition
		if err := cli.Get(ctx, client.ObjectKey{Name: fmt.Sprintf("pvc-burst-%02d", i)}, &rd); err != nil {
			t.Fatalf("get definition: %v", err)
		}

		minor := rd.Spec.VolumeDefinitions[0].DRBDMinor
		if minor == nil {
			t.Errorf("%s: no minor", rd.Name)

			continue
		}

		if other, dup := minors[*minor]; dup {
			t.Errorf("%s and %s share minor %d", rd.Name, other, *minor)
		}

		minors[*minor] = rd.Name
	}
}
