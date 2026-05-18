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
	"strconv"
	"sync"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// Bug 306 (HIGH, data-correctness): batch autoplace creates RDs in
// parallel — `for i in $(seq 10); do curl POST /rd; curl POST
// /autoplace & done; wait` — and the controller's per-RD port
// allocator races on the cluster-wide taken-set read. Two RDs
// reconciling concurrently both observe taken=∅, both pick the
// lowest free port (7000), and both Status().Update succeed
// because Kubernetes optimistic concurrency is per-object: two
// different RDs writing to their OWN statuses don't conflict.
// Result: N RDs get the SAME DRBD port, the satellite-side .res
// files collide, neither resource connects. This is the realistic
// production hazard for CI pipelines, GitOps batch apply, and
// mass-import flows.
//
// Bug 266's existing test only covers the sequential / fan-in
// shape (one RD then another). The fix is a process-wide
// `clusterAllocMu` held across {APIReader-list taken → pick free
// → Status().Update} so cross-RD allocation is strictly serial.
//
// This test exercises the batch shape: N=10 RDs reconciling
// concurrently must all receive DIFFERENT ports, all within the
// configured DRBD port range.
func TestBug306BatchAutoplacePortsUnique(t *testing.T) {
	t.Parallel()

	const numRDs = 10

	ctx := context.Background()
	scheme := newScheme(t)

	// Build the cluster: N RDs, each with two replicas (w1, w2).
	// All RDs start with no Status stamped — the exact shape after
	// `POST /resource-definitions` + `POST /autoplace` lands the
	// Resources but before the controller has reconciled any.
	objs := make([]client.Object, 0, numRDs*3)

	for i := range numRDs {
		rdName := rdNameFor(i)

		rd := &blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: rdName},
		}
		objs = append(objs, rd)

		for _, node := range []string{"w1", "w2"} {
			res := &blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: rdName + "." + node},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: rdName,
					NodeName:               node,
				},
			}
			objs = append(objs, res)
		}
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&blockstoriov1alpha1.Resource{},
			&blockstoriov1alpha1.ResourceDefinition{},
		).
		WithObjects(objs...).
		Build()

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	// Fire ensureDRBDIDs concurrently for every replica of every RD.
	// This is the worst case: every replica goroutine drives the
	// same allocator at the same time. The cluster mutex must
	// serialise cross-RD port picks so no two RDs land on the same
	// port.
	var wg sync.WaitGroup

	errCh := make(chan error, numRDs*2)

	for i := range numRDs {
		rdName := rdNameFor(i)

		for _, node := range []string{"w1", "w2"} {
			wg.Add(1)

			go func(rdName, node string) {
				defer wg.Done()

				target := &blockstoriov1alpha1.Resource{}
				if err := cli.Get(ctx, client.ObjectKey{Name: rdName + "." + node}, target); err != nil {
					errCh <- err

					return
				}

				if _, err := rec.EnsureDRBDIDsForTest(ctx, target, nil); err != nil {
					errCh <- err

					return
				}
			}(rdName, node)
		}
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("parallel allocator: %v", err)
		}
	}

	// A single allocator pass per replica may not converge — the
	// loser of a race reads back the winner's value on the second
	// pass. Run the convergence loop until every Resource has
	// Status.DRBDPort stamped.
	for range 8 {
		stable := true

		for i := range numRDs {
			rdName := rdNameFor(i)

			for _, node := range []string{"w1", "w2"} {
				target := &blockstoriov1alpha1.Resource{}
				if err := cli.Get(ctx, client.ObjectKey{Name: rdName + "." + node}, target); err != nil {
					t.Fatalf("get: %v", err)
				}

				if target.Status.DRBDPort != nil && target.Status.DRBDMinor != nil && target.Status.DRBDNodeID != nil {
					continue
				}

				stable = false

				if _, err := rec.EnsureDRBDIDsForTest(ctx, target, nil); err != nil {
					t.Fatalf("converge ensureDRBDIDs: %v", err)
				}
			}
		}

		if stable {
			break
		}
	}

	// Collect the per-RD ports and assert:
	//   1. Every RD has a non-nil port stamped.
	//   2. All replicas of one RD share the same port.
	//   3. Across RDs, every port is unique (cluster-wide).
	//   4. Every port is inside the default 7000-7999 range.
	portByRD := make(map[string]int32, numRDs)
	minorByRD := make(map[string]int32, numRDs)

	resList := &blockstoriov1alpha1.ResourceList{}
	if err := cli.List(ctx, resList); err != nil {
		t.Fatalf("list resources: %v", err)
	}

	for i := range resList.Items {
		res := &resList.Items[i]

		if res.Status.DRBDPort == nil {
			t.Errorf("%s: port not allocated", res.Name)

			continue
		}

		port := *res.Status.DRBDPort

		if port < 7000 || port > 7999 {
			t.Errorf("%s: port %d outside default 7000-7999 range",
				res.Name, port)
		}

		rdName := res.Spec.ResourceDefinitionName

		if existing, ok := portByRD[rdName]; ok {
			if existing != port {
				t.Errorf("RD %q port diverges across peers: %d vs %d "+
					"(per-RD scope violated)", rdName, existing, port)
			}
		} else {
			portByRD[rdName] = port
		}

		if res.Status.DRBDMinor != nil {
			minor := *res.Status.DRBDMinor

			if existing, ok := minorByRD[rdName]; ok {
				if existing != minor {
					t.Errorf("RD %q minor diverges across peers: %d vs %d",
						rdName, existing, minor)
				}
			} else {
				minorByRD[rdName] = minor
			}
		}
	}

	// Cross-RD uniqueness: every RD's port must differ.
	seenPort := make(map[int32]string, numRDs)

	for rdName, port := range portByRD {
		if other, ok := seenPort[port]; ok {
			t.Errorf("PORT COLLISION (Bug 306): RDs %q and %q both got port %d "+
				"under parallel batch autoplace — satellite-side .res files "+
				"would collide, neither resource would connect",
				rdName, other, port)
		}

		seenPort[port] = rdName
	}

	if len(portByRD) != numRDs {
		t.Errorf("expected %d RDs with ports, got %d", numRDs, len(portByRD))
	}

	// Cross-RD uniqueness for minors.
	seenMinor := make(map[int32]string, numRDs)

	for rdName, minor := range minorByRD {
		if other, ok := seenMinor[minor]; ok {
			t.Errorf("MINOR COLLISION: RDs %q and %q both got minor %d",
				rdName, other, minor)
		}

		seenMinor[minor] = rdName
	}
}

// rdNameFor returns a stable RD name for the i-th RD in the batch.
// Pulled out to keep the test body readable.
func rdNameFor(i int) string {
	return "pvc-bug306-rd-" + strconv.Itoa(i)
}
