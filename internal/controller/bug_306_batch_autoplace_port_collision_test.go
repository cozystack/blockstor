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
// lowest free port (20000), and both Status().Update succeed
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
			Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
				VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
					{VolumeNumber: 0, SizeKib: 1024},
				},
			},
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

	// Assert (identity-to-spec / per-node port model):
	//   1. Every replica has a non-nil Spec.DRBDPort, inside 20000-20999.
	//   2. PER-NODE port uniqueness: no two replicas on the same node
	//      share a port (this is the same-node collision that would
	//      break drbdadm adjust). Reuse across nodes is fine.
	//   3. Cluster-wide minor uniqueness: every RD's volume-0 minor
	//      differs (minors are the cluster-wide /dev/drbdN identity).
	portsByNode := make(map[string]map[int32]string)

	resList := &blockstoriov1alpha1.ResourceList{}
	if err := cli.List(ctx, resList); err != nil {
		t.Fatalf("list resources: %v", err)
	}

	rdsWithPort := make(map[string]bool, numRDs)

	for i := range resList.Items {
		res := &resList.Items[i]

		if res.Spec.DRBDPort == nil {
			t.Errorf("%s: port not allocated on Spec", res.Name)

			continue
		}

		port := *res.Spec.DRBDPort

		if port < 20000 || port > 20999 {
			t.Errorf("%s: port %d outside default 20000-20999 range", res.Name, port)
		}

		node := res.Spec.NodeName
		if portsByNode[node] == nil {
			portsByNode[node] = make(map[int32]string)
		}

		if other, dup := portsByNode[node][port]; dup {
			t.Errorf("SAME-NODE PORT COLLISION (Bug 306): %s and %s both got "+
				"port %d on node %q under parallel batch autoplace — their "+
				".res files would collide, neither resource would connect",
				res.Name, other, port, node)
		}

		portsByNode[node][port] = res.Name
		rdsWithPort[res.Spec.ResourceDefinitionName] = true
	}

	if len(rdsWithPort) != numRDs {
		t.Errorf("expected %d RDs with ports, got %d", numRDs, len(rdsWithPort))
	}

	// Cluster-wide minor uniqueness: each RD's volume-0 minor differs.
	seenMinor := make(map[int32]string, numRDs)

	for i := range numRDs {
		rdName := rdNameFor(i)

		rd := &blockstoriov1alpha1.ResourceDefinition{}
		if err := cli.Get(ctx, client.ObjectKey{Name: rdName}, rd); err != nil {
			t.Fatalf("get rd %s: %v", rdName, err)
		}

		if len(rd.Spec.VolumeDefinitions) == 0 || rd.Spec.VolumeDefinitions[0].DRBDMinor == nil {
			t.Errorf("RD %q: volume-0 minor not allocated", rdName)

			continue
		}

		minor := *rd.Spec.VolumeDefinitions[0].DRBDMinor

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
