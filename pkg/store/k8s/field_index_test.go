// SPDX-License-Identifier: Apache-2.0

package k8s_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// countingClient tells a scoped read from the exhaustive one. A List with no
// options is the whole collection; the store only issues one when a scoped
// read has failed and it fell back.
type countingClient struct {
	ctrlclient.Client

	exhaustive atomic.Int64
}

func (c *countingClient) List(ctx context.Context, list ctrlclient.ObjectList, opts ...ctrlclient.ListOption) error {
	if len(opts) == 0 {
		c.exhaustive.Add(1)
	}

	return c.Client.List(ctx, list, opts...) //nolint:wrapcheck // test decorator
}

// A manager's client answers a field selector from a local index or not at
// all: an unindexed field is not a slow query, it is a failed one. The store
// falls back to reading every object when that happens, which is the
// whole-cluster read the scoped one exists to replace — taken silently, on
// every call, on both server binaries.
//
// So the acceptance is not that the answer is right. A fallback answers right
// too. It is that the scoped read was actually served.
func TestRegisteredFieldIndexesServeTheScopedReads(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	t.Cleanup(func() { wipeAll(t, fixture.client) })

	seed := k8s.New(fixture.client)
	ctx := t.Context()

	if err := seed.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-idx"}); err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	for _, node := range []string{"node-a", "node-b"} {
		if err := seed.Nodes().Create(ctx, &apiv1.Node{Name: node, Type: "SATELLITE"}); err != nil {
			t.Fatalf("seed node %s: %v", node, err)
		}

		if err := seed.Resources().Create(ctx,
			&apiv1.Resource{Name: "pvc-idx", NodeName: node}); err != nil {
			t.Fatalf("seed replica on %s: %v", node, err)
		}

		if err := seed.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool-1",
			NodeName:        node,
			ProviderKind:    "LVM_THIN",
		}); err != nil {
			t.Fatalf("seed pool on %s: %v", node, err)
		}
	}

	counted := &countingClient{Client: startedCachedClient(t)}
	cached := k8s.New(counted)

	// The cache trails the writes above, so the reads are retried until it
	// has caught up. Every one of them is scoped: a fallback would show up in
	// the counter whichever attempt took it.
	waitFor(t, func() bool {
		replicas, err := cached.Resources().ListByNode(t.Context(), "node-a")

		return err == nil && len(replicas) == 1
	}, "the node's replica")

	waitFor(t, func() bool {
		pools, err := cached.StoragePools().ListByNode(t.Context(), "node-a")

		return err == nil && len(pools) == 1
	}, "the node's pool")

	waitFor(t, func() bool {
		replicas, err := cached.Resources().ListByDefinition(t.Context(), "pvc-idx")

		return err == nil && len(replicas) == 2
	}, "the definition's replicas")

	if n := counted.exhaustive.Load(); n != 0 {
		t.Errorf("%d whole-collection reads, want none — a scoped read fell back, "+
			"which is the exhaustive listing the index exists to avoid", n)
	}
}

// startedCachedClient brings up a manager against the envtest API server with
// the store's field indexes registered, and returns its cached client.
func startedCachedClient(t *testing.T) ctrlclient.Client {
	t.Helper()

	// k8s.NewManager, which is what the two binaries and the integration
	// harness call: registering the indexes separately is how all three came
	// to be running on the fallback at once, so the constructor is what this
	// pins.
	mgr, err := k8s.NewManager(fixture.env.Config, manager.Options{
		Scheme:                 fixture.client.Scheme(),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	if err != nil {
		t.Fatalf("build manager: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})

	go func() {
		defer close(stopped)

		_ = mgr.Start(ctx)
	}()

	t.Cleanup(func() {
		cancel()
		<-stopped
	})

	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	return mgr.GetClient()
}

// waitFor polls until the condition holds, or fails the test naming what it
// was waiting for.
func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)

	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for %s", what)
}

// The store's own contract, independent of any cache: a store built on the
// direct client answers the same scoped questions.
func TestScopedReadsOnAnUncachedClient(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	t.Cleanup(func() { wipeAll(t, fixture.client) })

	st := k8s.New(fixture.client)
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-direct"}); err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	for _, node := range []string{"node-a", "node-b"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: node, Type: "SATELLITE"}); err != nil {
			t.Fatalf("seed node %s: %v", node, err)
		}

		if err := st.Resources().Create(ctx,
			&apiv1.Resource{Name: "pvc-direct", NodeName: node}); err != nil {
			t.Fatalf("seed replica on %s: %v", node, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool-1",
			NodeName:        node,
			ProviderKind:    "LVM_THIN",
		}); err != nil {
			t.Fatalf("seed pool on %s: %v", node, err)
		}
	}

	replicas, err := st.Resources().ListByNode(ctx, "node-a")
	if err != nil || len(replicas) != 1 || replicas[0].NodeName != "node-a" {
		t.Errorf("ListByNode = %v, %v; want the one replica on node-a", replicas, err)
	}

	pools, err := st.StoragePools().ListByNode(ctx, "node-a")
	if err != nil || len(pools) != 1 || pools[0].NodeName != "node-a" {
		t.Errorf("pools ListByNode = %v, %v; want the one pool on node-a", pools, err)
	}

	byDefinition, err := st.Resources().ListByDefinition(ctx, "pvc-direct")
	if err != nil || len(byDefinition) != 2 {
		t.Errorf("ListByDefinition = %v, %v; want both replicas", byDefinition, err)
	}

	_ = store.FoldName("")
}

// A label is written by whoever created the object, and piraeus and operators
// create storage pools with `kubectl apply` and no labels. The node-scoped
// read selected on that label, so those pools were invisible to it — and this
// list is what a `node delete` is refused on and what the cascade removes, so
// an invisible pool is a node deleted with pools still registered against it.
func TestPoolsAppliedWithoutALabelAreStillOnTheNode(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	t.Cleanup(func() { wipeAll(t, fixture.client) })

	st := k8s.New(fixture.client)
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "node-hand", Type: "SATELLITE"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	// Written the way an operator writes one: the spec, and nothing else.
	applied := &crdv1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-hand.node-hand"},
		Spec: crdv1alpha1.StoragePoolSpec{
			NodeName:     "node-hand",
			PoolName:     "pool-hand",
			ProviderKind: "LVM_THIN",
		},
	}

	if err := fixture.client.Create(ctx, applied); err != nil {
		t.Fatalf("apply the pool: %v", err)
	}

	pools, err := st.StoragePools().ListByNode(ctx, "node-hand")
	if err != nil {
		t.Fatalf("ListByNode: %v", err)
	}

	if len(pools) != 1 {
		t.Fatalf("ListByNode returned %d pools, want the one applied by hand — a pool "+
			"the node-scoped read cannot see is a node deleted out from under it", len(pools))
	}
}

// A label is written by whoever created the object, and pkg/linstormigrate
// builds Snapshots adopted from a LINSTOR dump with none. This list is what
// `rd d` is refused on and what sweeps the leftovers behind it, so a snapshot
// the read cannot see is a definition deleted with snapshots still on it, and
// a mop-up that misses them too.
func TestSnapshotsAppliedWithoutALabelAreStillOnTheDefinition(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	t.Cleanup(func() { wipeAll(t, fixture.client) })

	st := k8s.New(fixture.client)
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx,
		&apiv1.ResourceDefinition{Name: "pvc-adopted"}); err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	// Written the way the migrator writes one: the spec, and nothing else.
	adopted := &crdv1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-adopted.snap-adopted"},
		Spec: crdv1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-adopted",
			SnapshotName:           "snap-adopted",
		},
	}

	if err := fixture.client.Create(ctx, adopted); err != nil {
		t.Fatalf("apply the snapshot: %v", err)
	}

	snaps, err := st.Snapshots().ListByDefinition(ctx, "pvc-adopted")
	if err != nil {
		t.Fatalf("ListByDefinition: %v", err)
	}

	if len(snaps) != 1 {
		t.Fatalf("ListByDefinition returned %d snapshots, want the one applied by hand — "+
			"a snapshot this read cannot see is a definition deleted out from under it",
			len(snaps))
	}
}

// The two untyped wordings a missing index reaches this store as, verbatim
// from controller-runtime: the manager cache's, and the fake client's.
var (
	//nolint:staticcheck // verbatim controller-runtime wording; matching it is the point
	errCacheHasNoIndex = errors.New("Index with name field:spec.nodeName does not exist")

	errFakeClientHasNoIndex = errors.New("List on GroupVersionKind /v1, Kind=Resource " +
		"specifies selector on field spec.nodeName, but no index with name spec.nodeName " +
		"has been registered for GroupVersionKind /v1, Kind=Resource")

	errRBACRefused = errors.New("nope")
)

// refusingClient answers every scoped list with one chosen error.
type refusingClient struct {
	ctrlclient.Client

	err error
}

func (c refusingClient) List(ctx context.Context, list ctrlclient.ObjectList, opts ...ctrlclient.ListOption) error {
	if len(opts) > 0 {
		return c.err
	}

	return c.Client.List(ctx, list, opts...) //nolint:wrapcheck // test decorator
}

// Falling back to the whole-cluster read is only safe for the one error that
// means "this server cannot answer that selector". A timeout, an RBAC refusal
// or a cancelled context are not statements about the selector: answering them
// with a larger read against the same exhausted budget, and returning nil,
// hides the failure and does the expensive thing at the worst moment.
func TestScopedReadsFallBackOnlyWhenTheSelectorIsRefused(t *testing.T) {
	if fixture == nil {
		t.Skip("envtest assets not installed; run `make setup-envtest` to enable")
	}

	t.Cleanup(func() { wipeAll(t, fixture.client) })

	seed := k8s.New(fixture.client)
	ctx := t.Context()

	if err := seed.Nodes().Create(ctx, &apiv1.Node{Name: "node-err", Type: "SATELLITE"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	for name, tc := range map[string]struct {
		err     error
		wantErr bool
	}{
		"the selector is refused": {
			err:     apierrors.NewBadRequest(`field label not supported: spec.nodeName`),
			wantErr: false,
		},
		"the cache has no index": {
			err:     errCacheHasNoIndex,
			wantErr: false,
		},
		// The fake client the unit suites run on words it differently, and
		// matching only the cache's wording turned every scoped read on such
		// a store into a 500 instead of the fallback.
		"the fake client has no index": {
			err:     errFakeClientHasNoIndex,
			wantErr: false,
		},
		"forbidden":       {err: apierrors.NewForbidden(schema.GroupResource{}, "x", errRBACRefused), wantErr: true},
		"server timeout":  {err: apierrors.NewTimeoutError("gateway timeout", 1), wantErr: true},
		"context expired": {err: context.DeadlineExceeded, wantErr: true},
	} {
		t.Run(name, func(t *testing.T) {
			st := k8s.New(refusingClient{Client: fixture.client, err: tc.err})

			_, err := st.Resources().ListByNode(ctx, "node-err")
			if tc.wantErr && err == nil {
				t.Error("the read failed and the store answered nil; the failure is invisible " +
					"and the whole-cluster read was issued in its place")
			}

			if !tc.wantErr && err != nil {
				t.Errorf("a refused selector must fall back, got %v", err)
			}

			_, err = st.StoragePools().ListByNode(ctx, "node-err")
			if tc.wantErr && err == nil {
				t.Error("pools: the read failed and the store answered nil")
			}

			if !tc.wantErr && err != nil {
				t.Errorf("pools: a refused selector must fall back, got %v", err)
			}
		})
	}
}
