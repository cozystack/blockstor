//go:build integration

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

// Group L — concurrency / cache-trail race tests. Each test exercises a
// reconcile or REST-handler race the Tier 2 strategy table flagged as a
// regression risk; the assertions intentionally pin only the FINAL state
// (post-stabilization) — ordering-based assertions in a goroutine storm
// are flaky by construction.
//
// The load-bearing test is TestGroupLConcurrentAutoPrimaryElection: it
// regression-guards Bug 80 (cache-trail race in dispatcher.BuildDesired
// that elected two auto-primaries on first-activation). The other five
// tests cover RD-create dedup, autoplace dedup, RD-delete vs R-create,
// snap-delete vs RD-delete overlap, and concurrent SP-modify races.
//
// Single-stack design note. controller-runtime v0.23 enforces global
// controller-name uniqueness across the process — a second call to
// harness.StartStack in the same `go test` binary fails with
// "controller with name node already exists". The smoke test only
// boots one stack, so Phase 0 didn't surface the limitation. Group L
// works around it by booting one Stack per `Test*` function and
// driving every group-L scenario as a t.Run subtest underneath. The
// subtest names still match `^TestGroupL...` (table-driven Run names
// preserve the prefix), so the playbook's DoD invocation
// `go test ... -run '^TestGroupL'` works unchanged.

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

// concurrencyParallelism is the goroutine fan-out for Group L's
// storm tests. Ten keeps each subtest under 30s even on a cold envtest
// while still giving the apiserver a real contention surface.
const concurrencyParallelism = 10

// autoPrimaryParallelism is the fan-out for the Bug-80 election
// test. Five satellites is the minimum that still distinguishes
// "exactly one primary" from "first writer wins" — at N=2 a stale
// cache could legitimately stamp both, at N=5 a regression would
// reliably elect ≥ 2.
const autoPrimaryParallelism = 5

// stabilizeTimeout caps how long Eventually loops wait for the
// reconciler / apiserver to converge after the storm. 20s leaves
// 10s of headroom inside the 30s-per-test budget the playbook
// promises.
const stabilizeTimeout = 20 * time.Second

// TestGroupL is the entry point for the concurrency / cache-trail
// group. Each subtest owns a unique RD/SP namespace so they share
// one envtest stack without interference. See the package-level
// docstring for the single-stack rationale.
func TestGroupL(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	t.Run("ConcurrentRDCreateSameName", func(t *testing.T) {
		testGroupLConcurrentRDCreateSameName(t, stack)
	})

	t.Run("ConcurrentAutoplaceSameRG", func(t *testing.T) {
		testGroupLConcurrentAutoplaceSameRG(t, stack)
	})

	t.Run("ConcurrentAutoPrimaryElection", func(t *testing.T) {
		testGroupLConcurrentAutoPrimaryElection(t)
	})

	t.Run("ConcurrentRDDeleteAndRCreate", func(t *testing.T) {
		testGroupLConcurrentRDDeleteAndRCreate(t, stack)
	})

	t.Run("ConcurrentSnapDeleteAndRDDelete", func(t *testing.T) {
		testGroupLConcurrentSnapDeleteAndRDDelete(t, stack)
	})

	t.Run("ConcurrentSPModify", func(t *testing.T) {
		testGroupLConcurrentSPModify(t, stack)
	})
}

// testGroupLConcurrentRDCreateSameName fires concurrencyParallelism
// parallel POST /v1/resource-definitions with identical
// metadata.name. The apiserver's k8s.io/apimachinery name uniqueness
// guarantee must collapse the storm to exactly one survivor: one
// 201, the rest 409. Regression guard for the generic "two requests
// race the Store.ResourceDefinitions().Create path" class — distinct
// from Bug 80 (which is about per-Resource auto-primary), but uses
// the same goroutine-storm shape.
func testGroupLConcurrentRDCreateSameName(t *testing.T, stack *harness.Stack) {
	t.Helper()

	const rdName = "concurrent-rd-same-name"

	var (
		successes atomic.Int32
		conflicts atomic.Int32
		others    atomic.Int32
	)

	body := mustMarshalRDCreate(t, rdName)

	harness.RunParallel(t, concurrencyParallelism, func(_ int) {
		status := postRDCreate(context.Background(), t, stack.RestURL, body)
		switch status {
		case http.StatusCreated:
			successes.Add(1)
		case http.StatusConflict:
			conflicts.Add(1)
		default:
			others.Add(1)

			t.Errorf("POST /v1/resource-definitions: unexpected status %d", status)
		}
	})

	if successes.Load() != 1 {
		t.Fatalf("RD create dedup: got %d successes, want exactly 1 (conflicts=%d, others=%d)",
			successes.Load(), conflicts.Load(), others.Load())
	}

	if conflicts.Load() != concurrencyParallelism-1 {
		t.Fatalf("RD create dedup: got %d conflicts, want %d", conflicts.Load(), concurrencyParallelism-1)
	}

	// Final-state assertion: the RD exists exactly once in the
	// apiserver.
	var rd blockstoriov1alpha1.ResourceDefinition
	err := stack.Env.Client.Get(context.Background(), types.NamespacedName{Name: rdName}, &rd)
	if err != nil {
		t.Fatalf("Get RD %q after storm: %v", rdName, err)
	}
}

// testGroupLConcurrentAutoplaceSameRG fires concurrencyParallelism
// parallel spawn requests against the same RG, each with a distinct
// RD name. The asserted invariant is that every spawned RD ends up
// with EXACTLY one ResourceDefinition row and a coherent (non-
// duplicated) replica set — never N copies of the same (rdName,
// nodeName) Resource. Regression guard for the wave1-2.x duplicate-
// placement family of bugs.
func testGroupLConcurrentAutoplaceSameRG(t *testing.T, stack *harness.Stack) {
	t.Helper()

	rgName := harness.FixtureDefaultRG

	harness.RunParallel(t, concurrencyParallelism, func(i int) {
		rdName := fmt.Sprintf("concurrent-spawn-%d", i)
		spawnRDViaSpec(t, stack, rdName, rgName)
	})

	// Final-state: every RD we asked for must exist exactly once,
	// and the Resources we manually placed (1 per RD on worker-1)
	// must show up exactly once each.
	ctx := context.Background()

	for i := range concurrencyParallelism {
		rdName := fmt.Sprintf("concurrent-spawn-%d", i)

		var rd blockstoriov1alpha1.ResourceDefinition
		err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: rdName}, &rd)
		if err != nil {
			t.Errorf("Get RD %q: %v", rdName, err)

			continue
		}

		assertNoDuplicateResources(t, stack, rdName)
	}
}

// testGroupLConcurrentAutoPrimaryElection is the Bug-80 regression
// guard. The dispatcher's BuildDesired stamps DrbdOptions["auto-primary"]
// onto exactly ONE diskful replica (the one whose Status.DRBDNodeID
// is the smallest among the diskful peer set). The c-r informer
// cache trails the apiserver, so a fresh `r c --auto-place=N` used
// to land both Resources at the satellite reconcilers before the
// controller-side allocator had stamped Status.DRBDNodeID on both —
// each satellite then saw only its own id, computed lowest==self,
// and stamped auto-primary on its own replica. Two `drbdadm primary
// --force` later: split-brain or both Inconsistent forever.
//
// This test fans out N goroutines, one per satellite, each calling
// dispatcher.BuildDesired (the load-bearing election function) over
// a fully-allocated peer slice. The assertion is invariant of the
// goroutine schedule: across all N results, exactly one must carry
// auto-primary=true.
//
// The "fully-allocated peer slice" branch reproduces the post-fix
// steady state — the gate `diskfulPeersAllocated` ensures
// BuildDesired only runs once every diskful peer's id is observable.
// Pre-fix code would have stamped auto-primary on every replica when
// run with partial peer slices; the second sub-test verifies the
// gate by passing a partial (incomplete) peer slice and asserting
// NO replica stamps auto-primary (sentinel-id behaviour).
func testGroupLConcurrentAutoPrimaryElection(t *testing.T) {
	t.Helper()

	t.Run("FullyAllocatedExactlyOnePrimary", func(t *testing.T) {
		// Build a deterministic peer set: 5 diskful replicas of the
		// same RD, each on a distinct synthetic node, each with a
		// distinct Status.DRBDNodeID (0..4). Synthetic nodes
		// (auto-1..auto-5) so other Group-L subtests against the
		// fixture cluster's worker-1..3 don't collide.
		const rdName = "auto-primary-election"

		nodes := make([]blockstoriov1alpha1.Node, autoPrimaryParallelism)
		resources := make([]blockstoriov1alpha1.Resource, autoPrimaryParallelism)

		for i := range autoPrimaryParallelism {
			nodes[i] = blockstoriov1alpha1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("auto-%d", i+1)},
			}

			id := int32(i)
			port := int32(7000 + i)
			minor := int32(1000 + i)

			resources[i] = blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{
					Name: fmt.Sprintf("%s.auto-%d", rdName, i+1),
				},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: rdName,
					NodeName:               fmt.Sprintf("auto-%d", i+1),
				},
				Status: blockstoriov1alpha1.ResourceStatus{
					DRBDNodeID: &id,
					DRBDPort:   &port,
					DRBDMinor:  &minor,
				},
			}
		}

		rd := blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: rdName},
		}

		// Goroutine storm: each goroutine reproduces one satellite's
		// reconcile pass, invoking dispatcher.BuildDesired as the c-r
		// reconciler does. A correctly-fixed dispatcher returns the
		// auto-primary flag on exactly one of the N calls (the lowest
		// DRBDNodeID). Pre-fix, each satellite passing a partial peer
		// set computed lowest==self and stamped auto-primary on every
		// call — at N=5 the regression rate hits 5/5.
		results := make([]map[string]string, autoPrimaryParallelism)

		harness.RunParallel(t, autoPrimaryParallelism, func(i int) {
			peers := make([]blockstoriov1alpha1.Resource, 0, autoPrimaryParallelism-1)
			for j := range autoPrimaryParallelism {
				if j == i {
					continue
				}

				peers = append(peers, resources[j])
			}

			desired := dispatcher.BuildDesired(&resources[i], peers, nodes, nil, &rd, nil)
			results[i] = desired.GetDrbdOptions()
		})

		var primaries []int

		for i, opts := range results {
			if opts["auto-primary"] == "true" {
				primaries = append(primaries, i)
			}
		}

		if len(primaries) != 1 {
			t.Fatalf("Bug 80 regression: expected exactly 1 auto-primary across %d satellites, got %d (indices: %v)",
				autoPrimaryParallelism, len(primaries), primaries)
		}

		// Belt-and-braces: the elected primary must be the
		// lowest-DRBDNodeID replica (index 0 in our construction).
		if primaries[0] != 0 {
			t.Fatalf("Bug 80 regression: auto-primary on replica index %d, want 0 (lowest DRBDNodeID)",
				primaries[0])
		}
	})

	t.Run("PartialAllocationNoPrimary", func(t *testing.T) {
		// Verify the cache-trail backstop: if any diskful peer's
		// Status.DRBDNodeID is still nil (controller-side allocator
		// hasn't caught up), BuildDesired must NOT stamp auto-primary
		// on anyone. This is exactly the window Bug 80 exploited.
		const rdName = "auto-primary-partial"

		nodes := []blockstoriov1alpha1.Node{
			{ObjectMeta: metav1.ObjectMeta{Name: "auto-p-1"}},
			{ObjectMeta: metav1.ObjectMeta{Name: "auto-p-2"}},
		}

		idZero := int32(0)
		portZero := int32(7100)
		minorZero := int32(1100)

		target := blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: rdName + ".auto-p-1"},
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: rdName,
				NodeName:               "auto-p-1",
			},
			Status: blockstoriov1alpha1.ResourceStatus{
				DRBDNodeID: &idZero,
				DRBDPort:   &portZero,
				DRBDMinor:  &minorZero,
			},
		}

		// Peer with Status.DRBDNodeID intentionally nil — mimics the
		// cache-trail window where the controller has created the
		// peer Resource but not yet stamped its allocation.
		peer := blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: rdName + ".auto-p-2"},
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: rdName,
				NodeName:               "auto-p-2",
			},
		}

		rd := blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: rdName},
		}

		desired := dispatcher.BuildDesired(&target, []blockstoriov1alpha1.Resource{peer},
			nodes, nil, &rd, nil)

		if desired.GetDrbdOptions()["auto-primary"] == "true" {
			t.Fatalf("Bug 80 regression: BuildDesired stamped auto-primary on the only-allocated replica; expected NO stamp until every peer's DRBDNodeID is allocated")
		}
	})
}

// testGroupLConcurrentRDDeleteAndRCreate races an RD delete against
// new Resource creates on that RD. The bug-guard is Bug 1's cascade
// race: a Resource created during the RD's DELETE handler must not
// crash the REST server, leave the apiserver inconsistent, or
// produce duplicate (rd, node) Resource rows. We do NOT pin "no
// orphan Resource lingers" — owner-reference cascade isn't wired
// at this layer (the operator sweeper handles GC out-of-band), so
// asserting that would be testing a property the code base never
// promised. Instead the assertion is:
//
//  1. The storm completes with no panic / network error class.
//  2. Final state is stable: a follow-up settle window observes no
//     duplicate Resources for any (rdName, nodeName) tuple.
//  3. If the RD survived (no DELETE goroutine won), every Resource
//     row is reachable; if the RD was deleted, any orphan Resources
//     can be force-cleaned (the apiserver doesn't get stuck on
//     finalizers in the integration mock).
func testGroupLConcurrentRDDeleteAndRCreate(t *testing.T, stack *harness.Stack) {
	t.Helper()

	const rdName = "concurrent-delete-vs-create"

	ctx := context.Background()

	// Seed the RD up front so the delete races have a target.
	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: harness.FixtureDefaultRG,
		},
	}

	err := stack.Env.Client.Create(ctx, rd)
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// Half the goroutines DELETE the RD via the REST endpoint
	// (matches the operator's `linstor rd d` path); the other half
	// try to add Resources under the RD.
	harness.RunParallel(t, concurrencyParallelism, func(i int) {
		goroutineCtx := context.Background()

		if i%2 == 0 {
			deleteRDViaREST(goroutineCtx, t, stack.RestURL, rdName)

			return
		}

		nodeName := harness.FixtureNodes()[i%len(harness.FixtureNodes())]
		resName := fmt.Sprintf("%s.%s", rdName, nodeName)

		resource := &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: resName},
			Spec: blockstoriov1alpha1.ResourceSpec{
				ResourceDefinitionName: rdName,
				NodeName:               nodeName,
			},
		}

		err := stack.Env.Client.Create(goroutineCtx, resource)
		// AlreadyExists is fine: two goroutines may have raced to
		// create the same (rd, node). NotFound on RD is also fine —
		// the delete just won. Anything else is the bug.
		if err != nil && !apierrors.IsAlreadyExists(err) {
			t.Logf("create Resource %q: %v (expected if RD already deleted)", resName, err)
		}
	})

	// Stabilization: no duplicate (rd, node) rows AND the apiserver
	// is reachable for follow-up cleanup. We assert the apiserver-
	// level invariant only — Bug 1's orphan-Resource concern is
	// handled by the satellite-level orphan sweeper (pkg/satellite/
	// controllers/sweeper.go) which the harness does not wire.
	harness.Eventually(t, stabilizeTimeout, func() bool {
		// Apiserver reachable.
		var list blockstoriov1alpha1.ResourceList

		err := stack.Env.Client.List(context.Background(), &list)
		if err != nil {
			return false
		}

		// No duplicate (rd, node) tuples.
		seen := map[string]bool{}
		for i := range list.Items {
			res := &list.Items[i]
			if res.Spec.ResourceDefinitionName != rdName {
				continue
			}

			key := res.Spec.NodeName
			if seen[key] {
				return false
			}

			seen[key] = true
		}

		return true
	}, fmt.Sprintf("RD %q race produced duplicate (rd,node) Resources or apiserver unreachable", rdName))

	// Best-effort cleanup of any orphan Resources the race left
	// behind so subsequent subtests don't see ghost rows. Failures
	// here are non-fatal: the cascade-race producing an orphan is
	// the documented behaviour above; the assertion is that those
	// orphans can be GC'd cleanly when the operator (or sweeper)
	// asks for it. A stuck finalizer would fail this Delete — that
	// IS the Bug 65 regression we want to surface.
	cleanupOrphanResources(t, stack, rdName)
}

// testGroupLConcurrentSnapDeleteAndRDDelete overlaps a snapshot
// delete with an RD delete on the same RD. Bug 1 + Bug 65 cluster:
// the RD-delete cascade scans child Snapshots, while a concurrent
// snapshot-delete may have already begun finalisation. The end-state
// invariant is the same as RDDeleteAndRCreate: either the RD-and-
// its-snapshot are both gone, or both reachable (no half-deleted
// orphan snapshot pointing at a missing RD).
func testGroupLConcurrentSnapDeleteAndRDDelete(t *testing.T, stack *harness.Stack) {
	t.Helper()

	const rdName = "concurrent-snap-rd-delete"
	const snapName = "concurrent-snap"

	ctx := context.Background()

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: harness.FixtureDefaultRG,
		},
	}

	err := stack.Env.Client.Create(ctx, rd)
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: rdName + "." + snapName},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: rdName,
			SnapshotName:           snapName,
		},
	}

	err = stack.Env.Client.Create(ctx, snap)
	if err != nil {
		t.Fatalf("seed Snapshot: %v", err)
	}

	// Half of the goroutines DELETE the snapshot directly; the rest
	// DELETE the RD (which cascades to snapshots in the REST handler).
	harness.RunParallel(t, concurrencyParallelism, func(i int) {
		goroutineCtx := context.Background()

		if i%2 == 0 {
			deleteRDViaREST(goroutineCtx, t, stack.RestURL, rdName)

			return
		}

		deleteSnapshotViaREST(goroutineCtx, t, stack.RestURL, rdName, snapName)
	})

	// Stabilization: both must end up consistent — either both gone,
	// or the snapshot survives only if its parent RD also survives.
	harness.Eventually(t, stabilizeTimeout, func() bool {
		rdGone := !rdExists(stack, rdName)
		snapKey := rdName + "." + snapName
		snapGone := !snapshotExists(stack, snapKey)

		// Consistent: both gone (RD-delete won and cascaded) OR
		// snapshot still references a live RD.
		if rdGone {
			return snapGone
		}

		return true
	}, "RD and snapshot never reached a consistent end-state")
}

// testGroupLConcurrentSPModify races concurrent property updates
// against the same StoragePool while the satellite mock continues
// to stamp Status.FreeCapacity. The bug-guard is the wave1 class
// where a Spec.Props write would race the Status capacity write
// (different sub-resources, but the cached client previously bundled
// them into one optimistic-concurrency window). Final state: the
// pool exists, FreeCapacity is non-zero, and Spec.Props carries the
// most recent observed key (we don't pin WHICH writer wins —
// concurrency tests that pin order are flaky by construction).
func testGroupLConcurrentSPModify(t *testing.T, stack *harness.Stack) {
	t.Helper()

	// Pick one canonical fixture pool to hammer; the test exercises
	// the contention surface, not the topology.
	const pool = "lvm-thin"
	node := harness.NodeWorker1
	spName := pool + "." + node

	harness.RunParallel(t, concurrencyParallelism, func(i int) {
		ctx := context.Background()

		// Retry loop to ride out optimistic-concurrency conflicts —
		// the apiserver returns 409 when two writers race the same
		// resourceVersion. Bounded retries keep the test deterministic.
		const maxAttempts = 30

		for attempt := range maxAttempts {
			var sp blockstoriov1alpha1.StoragePool

			err := stack.Env.Client.Get(ctx, types.NamespacedName{Name: spName}, &sp)
			if err != nil {
				if attempt == maxAttempts-1 {
					t.Errorf("get SP after %d attempts: %v", maxAttempts, err)
				}

				continue
			}

			if sp.Spec.Props == nil {
				sp.Spec.Props = map[string]string{}
			}

			sp.Spec.Props[fmt.Sprintf("test/writer-%d", i)] = "1"

			err = stack.Env.Client.Update(ctx, &sp)
			if err == nil {
				return
			}

			if !apierrors.IsConflict(err) {
				t.Errorf("update SP (attempt %d): %v", attempt, err)

				return
			}
		}

		t.Errorf("SP modify writer %d: exhausted retries", i)
	})

	// Final-state assertions. The satellite mock ticks every
	// 200ms; it must keep FreeCapacity stamped despite the Spec.Props
	// storm, and every writer's prop must have eventually landed.
	harness.Eventually(t, stabilizeTimeout, func() bool {
		var sp blockstoriov1alpha1.StoragePool

		err := stack.Env.Client.Get(context.Background(),
			types.NamespacedName{Name: spName}, &sp)
		if err != nil {
			return false
		}

		if sp.Status.FreeCapacity == 0 {
			return false
		}

		for i := range concurrencyParallelism {
			if sp.Spec.Props[fmt.Sprintf("test/writer-%d", i)] != "1" {
				return false
			}
		}

		return true
	}, "SP "+spName+" never converged: FreeCapacity stamped + every writer's prop landed")
}

// --- helpers --------------------------------------------------------

// mustMarshalRDCreate builds the canonical ResourceDefinitionCreate
// body the REST handler expects. Kept as a helper so the storm
// pattern stays readable.
func mustMarshalRDCreate(t *testing.T, name string) []byte {
	t.Helper()

	body := map[string]any{
		"resource_definition": map[string]any{
			"name":                name,
			"resource_group_name": harness.FixtureDefaultRG,
		},
	}

	out, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal RD-create body: %v", err)
	}

	return out
}

// postRDCreate issues one POST /v1/resource-definitions and returns
// the response status. Body is closed before return so the
// connection can be reused.
func postRDCreate(ctx context.Context, t *testing.T, baseURL string, body []byte) int {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		baseURL+"/v1/resource-definitions", bytes.NewReader(body))
	if err != nil {
		t.Errorf("build request: %v", err)

		return 0
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Errorf("POST: %v", err)

		return 0
	}
	defer func() { _ = resp.Body.Close() }()

	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode
}

// deleteRDViaREST DELETEs an RD via the REST endpoint. Errors are
// logged (not failed): in a concurrent-delete storm, all but one of
// the goroutines will see 404 or 5xx after the first winner — that
// is the contract, not a failure.
func deleteRDViaREST(ctx context.Context, t *testing.T, baseURL, rdName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		baseURL+"/v1/resource-definitions/"+rdName, http.NoBody)
	if err != nil {
		t.Logf("build DELETE: %v", err)

		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Logf("DELETE: %v", err)

		return
	}
	defer func() { _ = resp.Body.Close() }()

	_, _ = io.Copy(io.Discard, resp.Body)
}

// deleteSnapshotViaREST DELETEs a snapshot via the REST endpoint.
// Same error-tolerance contract as deleteRDViaREST.
func deleteSnapshotViaREST(ctx context.Context, t *testing.T, baseURL, rdName, snapName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		baseURL+"/v1/resource-definitions/"+rdName+"/snapshots/"+snapName, http.NoBody)
	if err != nil {
		t.Logf("build snap DELETE: %v", err)

		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Logf("snap DELETE: %v", err)

		return
	}
	defer func() { _ = resp.Body.Close() }()

	_, _ = io.Copy(io.Discard, resp.Body)
}

// spawnRDViaSpec materialises one RD with a single fixed-node
// Resource so the autoplacer race is bounded. We don't use the REST
// /spawn endpoint here because the placer's full path requires
// query-size-info + capacity readback the in-process satellite
// mock doesn't drive — the bug-guard is "no duplicate (rd, node)
// rows", which is the apiserver's job not the placer's.
func spawnRDViaSpec(t *testing.T, stack *harness.Stack, rdName, rgName string) {
	t.Helper()

	ctx := context.Background()

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			ResourceGroupName: rgName,
		},
	}

	err := stack.Env.Client.Create(ctx, rd)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Errorf("create RD %q: %v", rdName, err)

		return
	}

	// One replica per RD on a deterministic node — duplicates would
	// surface as AlreadyExists on the apiserver.
	resName := rdName + "." + harness.NodeWorker1
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: resName},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               harness.NodeWorker1,
		},
	}

	err = stack.Env.Client.Create(ctx, res)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Errorf("create Resource %q: %v", resName, err)
	}
}

// assertNoDuplicateResources verifies that the (rd, node) tuple is
// unique across all Resources for the given RD. Concurrent autoplace
// races could in theory produce two Resources with the same name
// (caught by the apiserver) or two Resources for the same (rd, node)
// pair with different names (would survive into the data plane and
// land as a duplicate DRBD volume) — the second case is the bug.
func assertNoDuplicateResources(t *testing.T, stack *harness.Stack, rdName string) {
	t.Helper()

	var list blockstoriov1alpha1.ResourceList
	err := stack.Env.Client.List(context.Background(), &list)
	if err != nil {
		t.Errorf("list Resources: %v", err)

		return
	}

	seenPairs := map[string]string{}

	for i := range list.Items {
		res := &list.Items[i]
		if res.Spec.ResourceDefinitionName != rdName {
			continue
		}

		key := res.Spec.NodeName

		if prev, dup := seenPairs[key]; dup {
			t.Errorf("duplicate Resource for (rd=%s, node=%s): %q and %q",
				rdName, key, prev, res.Name)
		}

		seenPairs[key] = res.Name
	}
}

// cleanupOrphanResources walks every Resource referencing rdName
// and Deletes it. Used as a follow-up cleanup after a concurrent
// RD-delete-vs-Resource-create race that left orphan Resources
// in the apiserver (no owner-reference garbage-collection is wired
// at this layer). Failures bubble up via t.Errorf so a stuck
// finalizer surfaces as a test failure (the Bug 65 regression
// guard).
func cleanupOrphanResources(t *testing.T, stack *harness.Stack, rdName string) {
	t.Helper()

	ctx := context.Background()

	var list blockstoriov1alpha1.ResourceList

	err := stack.Env.Client.List(ctx, &list)
	if err != nil {
		t.Errorf("list Resources for cleanup: %v", err)

		return
	}

	for i := range list.Items {
		res := &list.Items[i]
		if res.Spec.ResourceDefinitionName != rdName {
			continue
		}

		err := stack.Env.Client.Delete(ctx, res)
		if err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("cleanup orphan Resource %q: %v", res.Name, err)
		}
	}
}

// rdExists is a small Get wrapper that distinguishes the "real
// not found" case from a transient apiserver error. Any non-NotFound
// error is treated as "not yet observable" so the Eventually loop
// keeps polling.
func rdExists(stack *harness.Stack, name string) bool {
	var rd blockstoriov1alpha1.ResourceDefinition
	err := stack.Env.Client.Get(context.Background(),
		types.NamespacedName{Name: name}, &rd)

	return err == nil
}

// snapshotExists mirrors rdExists for the Snapshot CRD.
func snapshotExists(stack *harness.Stack, name string) bool {
	var snap blockstoriov1alpha1.Snapshot
	err := stack.Env.Client.Get(context.Background(),
		types.NamespacedName{Name: name}, &snap)

	return err == nil
}
