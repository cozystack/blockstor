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
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// cacheOf answers the reconciler's reads from frozen, as an informer
// cache that has not seen any write yet, and sends its writes to live.
func cacheOf(live, frozen client.WithWatch, funcs interceptor.Funcs) client.WithWatch {
	funcs.Get = func(ctx context.Context, _ client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		return frozen.Get(ctx, key, obj, opts...)
	}
	funcs.List = func(ctx context.Context, _ client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		return frozen.List(ctx, list, opts...)
	}

	return interceptor.NewClient(live, funcs)
}

func onlyKind[T client.Object](objs []client.Object) []client.Object {
	var out []client.Object

	for _, obj := range objs {
		if _, ok := obj.(T); ok {
			out = append(out, obj)
		}
	}

	return out
}

// allocateEach runs the allocator over every replica in turn, one after
// another, the way one worker would.
func allocateEach(ctx context.Context, t *testing.T, rec *controllerpkg.ResourceReconciler, live client.Client, replicas []client.Object) {
	t.Helper()

	for _, obj := range replicas {
		var target blockstoriov1alpha1.Resource
		if err := live.Get(ctx, client.ObjectKeyFromObject(obj), &target); err != nil {
			t.Fatalf("get: %v", err)
		}

		if _, err := rec.EnsureDRBDIDsForTest(ctx, &target, nil); err != nil {
			t.Fatalf("allocate %s: %v", target.Name, err)
		}
	}
}

// TestAllocationSafeWhileDefinitionsAreNotCachedYet pins the case of a
// cache that lists the new replicas but not their freshly created
// definitions: definitions and replicas come through separate
// informers, and the replica's event is what starts the allocation. A
// definition the cache does not list is not a deleted one; taking it
// for one handed its minor to the next definition, and a minor is
// never reallocated, so the second volume could never come up on a
// node it shares with the first.
func TestAllocationSafeWhileDefinitionsAreNotCachedYet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	objs := burstObjects(4, "w1")
	live := newBurstClient(t, objs)
	frozen := newBurstClient(t, onlyKind[*blockstoriov1alpha1.Resource](objs))
	rec := &controllerpkg.ResourceReconciler{Client: cacheOf(live, frozen, interceptor.Funcs{}), Scheme: live.Scheme(), APIReader: live}

	allocateEach(ctx, t, rec, live, onlyKind[*blockstoriov1alpha1.Resource](objs))
	assertBurstUnique(ctx, t, live, 4)
}

// TestAllocationSafeWhileReplicasAreNotCachedYet is the same for
// replicas: a replica the cache does not list yet keeps the node id
// and port it was given.
func TestAllocationSafeWhileReplicasAreNotCachedYet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	objs := burstObjects(4, "w1", "w2")
	live := newBurstClient(t, objs)
	frozen := newBurstClient(t, onlyKind[*blockstoriov1alpha1.ResourceDefinition](objs))
	rec := &controllerpkg.ResourceReconciler{Client: cacheOf(live, frozen, interceptor.Funcs{}), Scheme: live.Scheme(), APIReader: live}

	allocateEach(ctx, t, rec, live, onlyKind[*blockstoriov1alpha1.Resource](objs))
	assertBurstUnique(ctx, t, live, 4)
}

// TestAllocationSafeWhenAWriteAnswerIsLost pins a write that lands
// while its caller gets an error, as a timeout under a loaded apiserver
// does. The retry finds the values already written and has nothing to
// allocate, so a reservation made only after a successful write was
// never made at all, and the next allocation, reading a cache that
// trails, handed the same port and minor out again.
func TestAllocationSafeWhenAWriteAnswerIsLost(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	objs := burstObjects(3, "w1")
	live := newBurstClient(t, objs)
	frozen := newBurstClient(t, objs)

	var (
		mu   sync.Mutex
		lost = map[string]bool{}
	)

	cache := cacheOf(live, frozen, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if err := c.Patch(ctx, obj, patch, opts...); err != nil {
				return err
			}

			mu.Lock()
			defer mu.Unlock()

			key := obj.GetObjectKind().GroupVersionKind().Kind + "/" + obj.GetName()
			if lost[key] {
				return nil
			}

			lost[key] = true

			return apierrors.NewTimeoutError("answer lost", 1)
		},
	})
	rec := &controllerpkg.ResourceReconciler{Client: cache, Scheme: live.Scheme(), APIReader: live}

	for _, obj := range onlyKind[*blockstoriov1alpha1.Resource](objs) {
		var allocated bool

		for range 4 {
			var target blockstoriov1alpha1.Resource
			if err := live.Get(ctx, client.ObjectKeyFromObject(obj), &target); err != nil {
				t.Fatalf("get: %v", err)
			}

			if _, err := rec.EnsureDRBDIDsForTest(ctx, &target, nil); err == nil {
				allocated = true

				break
			}
		}

		if !allocated {
			t.Fatalf("%s: allocation kept failing", obj.GetName())
		}
	}

	assertBurstUnique(ctx, t, live, 3)
}

// TestAllocationSafeWhileCacheShowsTheReplacedReplica pins a replica
// deleted and created again under the same name: the cache still shows
// the old object, with its old port, while the new one already has its
// own. That is the cache trailing, not the new object gone, so its
// port stays reserved.
func TestAllocationSafeWhileCacheShowsTheReplacedReplica(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	objs := burstObjects(2, "w1")
	live := newBurstClient(t, objs)

	stale := burstObjects(2, "w1")

	for _, obj := range stale {
		if res, ok := obj.(*blockstoriov1alpha1.Resource); ok && res.Name == "pvc-burst-00.w1" {
			res.UID = types.UID("res-old")
			port, nodeID := int32(20050), int32(0)
			res.Spec.DRBDPort, res.Spec.DRBDNodeID = &port, &nodeID
		}
	}

	frozen := newBurstClient(t, stale)
	rec := &controllerpkg.ResourceReconciler{Client: cacheOf(live, frozen, interceptor.Funcs{}), Scheme: live.Scheme(), APIReader: live}

	allocateEach(ctx, t, rec, live, onlyKind[*blockstoriov1alpha1.Resource](objs))
	assertBurstUnique(ctx, t, live, 2)
}

// TestRetryAfterAFailedWriteKeepsTheLowestValues pins a write that did
// not land: the retry allocates the replica again, and its own earlier
// reservation must not count as taken, or a replica whose first write
// failed would come up on node id 1 and the next port for no reason.
func TestRetryAfterAFailedWriteKeepsTheLowestValues(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	objs := burstObjects(1, "w1")
	live := newBurstClient(t, objs)
	frozen := newBurstClient(t, objs)

	var (
		mu     sync.Mutex
		failed bool
	)

	cache := cacheOf(live, frozen, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			mu.Lock()
			defer mu.Unlock()

			if _, ok := obj.(*blockstoriov1alpha1.Resource); ok && !failed {
				failed = true

				return apierrors.NewServiceUnavailable("not written")
			}

			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	rec := &controllerpkg.ResourceReconciler{Client: cache, Scheme: live.Scheme(), APIReader: live}

	var target blockstoriov1alpha1.Resource
	if err := live.Get(ctx, client.ObjectKey{Name: "pvc-burst-00.w1"}, &target); err != nil {
		t.Fatalf("get: %v", err)
	}

	if _, err := rec.EnsureDRBDIDsForTest(ctx, &target, nil); err == nil {
		t.Fatal("first write was meant to fail")
	}

	allocateEach(ctx, t, rec, live, onlyKind[*blockstoriov1alpha1.Resource](objs))

	if err := live.Get(ctx, client.ObjectKey{Name: "pvc-burst-00.w1"}, &target); err != nil {
		t.Fatalf("get: %v", err)
	}

	if *target.Spec.DRBDNodeID != 0 || *target.Spec.DRBDPort != 20000 {
		t.Errorf("retried replica got node id %d, port %d; want 0, 20000", *target.Spec.DRBDNodeID, *target.Spec.DRBDPort)
	}
}

// TestAllocationDoesNotWaitForAnotherReplicasWrites pins that the
// allocation lock covers choosing a replica's identities, not the API
// writes that persist them. Held across the writes, it made every new
// replica of a provisioning burst wait for the round trips of all the
// replicas before it, so the last ones got their identities, and the
// satellites started building them, many seconds after they were
// created. The replica allocated meanwhile must still get its own port
// and minor: the first one's are reserved while their writes are in
// flight.
func TestAllocationDoesNotWaitForAnotherReplicasWrites(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	objs := burstObjects(2, "w1")
	live := newBurstClient(t, objs)
	frozen := newBurstClient(t, objs)

	blocked := make(chan struct{})
	release := make(chan struct{})

	var once sync.Once

	cache := cacheOf(live, frozen, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if strings.HasPrefix(obj.GetName(), "pvc-burst-00") {
				once.Do(func() {
					close(blocked)
					<-release
				})
			}

			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	rec := &controllerpkg.ResourceReconciler{Client: cache, Scheme: live.Scheme(), APIReader: live}

	allocate := func(name string) <-chan error {
		done := make(chan error, 1)

		go func() {
			var target blockstoriov1alpha1.Resource
			if err := live.Get(ctx, client.ObjectKey{Name: name}, &target); err != nil {
				done <- err

				return
			}

			_, err := rec.EnsureDRBDIDsForTest(ctx, &target, nil)
			done <- err
		}()

		return done
	}

	first := allocate("pvc-burst-00.w1")
	<-blocked

	select {
	case err := <-allocate("pvc-burst-01.w1"):
		if err != nil {
			t.Fatalf("allocate pvc-burst-01.w1: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("pvc-burst-01.w1 waited for the writes of pvc-burst-00.w1")
	}

	close(release)

	if err := <-first; err != nil {
		t.Fatalf("allocate pvc-burst-00.w1: %v", err)
	}

	assertBurstUnique(ctx, t, live, 2)
}

// TestDefinitionWrittenMeanwhileStillGetsItsMinor pins a write to the
// definition, any write, landing between the allocator's read of it
// and the write of the minors. The minor write carries the
// resourceVersion it read, so it fails with a conflict; the allocator
// took that for a sibling having written the minors first and went on
// without them. The replica then came up with no minor on its
// definition until some later event reconciled it again, a whole
// pass of a provisioning burst lost.
func TestDefinitionWrittenMeanwhileStillGetsItsMinor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	objs := burstObjects(1, "w1")
	live := newBurstClient(t, objs)

	var (
		mu      sync.Mutex
		touched bool
	)

	cache := interceptor.NewClient(live, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			mu.Lock()
			defer mu.Unlock()

			if _, ok := obj.(*blockstoriov1alpha1.ResourceDefinition); ok && !touched {
				touched = true

				var rd blockstoriov1alpha1.ResourceDefinition
				if err := c.Get(ctx, client.ObjectKeyFromObject(obj), &rd); err != nil {
					return err
				}

				rd.Annotations = map[string]string{"example.com/touched": "true"}
				if err := c.Update(ctx, &rd); err != nil {
					return err
				}
			}

			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	rec := &controllerpkg.ResourceReconciler{Client: cache, Scheme: live.Scheme(), APIReader: live}

	allocateEach(ctx, t, rec, live, onlyKind[*blockstoriov1alpha1.Resource](objs))

	if !touched {
		t.Fatal("the definition was never written meanwhile")
	}

	var rd blockstoriov1alpha1.ResourceDefinition
	if err := live.Get(ctx, client.ObjectKey{Name: "pvc-burst-00"}, &rd); err != nil {
		t.Fatalf("get: %v", err)
	}

	if rd.Spec.VolumeDefinitions[0].DRBDMinor == nil {
		t.Fatal("definition has no minor after the allocation pass")
	}

	var res blockstoriov1alpha1.Resource
	if err := live.Get(ctx, client.ObjectKey{Name: "pvc-burst-00.w1"}, &res); err != nil {
		t.Fatalf("get: %v", err)
	}

	if res.Status.DRBDMinor == nil || *res.Status.DRBDMinor != *rd.Spec.VolumeDefinitions[0].DRBDMinor {
		t.Errorf("replica mirrors minor %v, definition has %d", res.Status.DRBDMinor, *rd.Spec.VolumeDefinitions[0].DRBDMinor)
	}
}
