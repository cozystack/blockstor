// SPDX-License-Identifier: Apache-2.0

package k8s_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

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

	mgr, err := manager.New(fixture.env.Config, manager.Options{
		Scheme:                 fixture.client.Scheme(),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	if err != nil {
		t.Fatalf("build manager: %v", err)
	}

	if err := k8s.RegisterFieldIndexes(t.Context(), mgr.GetFieldIndexer()); err != nil {
		t.Fatalf("register field indexes: %v", err)
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
