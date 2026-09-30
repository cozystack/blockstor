// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// cacheBlindSnapshots is a snapshot substore whose cached listing has not seen
// the row a concurrent create just wrote, while the API server has it. That is
// the moment rd d decides on: the snapshot most likely to be missing from the
// cache is exactly the one that raced the delete.
type cacheBlindSnapshots struct {
	store.SnapshotStore
}

func (cacheBlindSnapshots) ListByDefinition(context.Context, string) ([]apiv1.Snapshot, error) {
	return nil, nil
}

type cacheBlindSnapshotStore struct{ store.Store }

func (c cacheBlindSnapshotStore) Snapshots() store.SnapshotStore {
	return cacheBlindSnapshots{c.Store.Snapshots()}
}

// rd d refuses over existing snapshots and then acts, so it is a destructive
// decision; this round applied "must not read a cache" to the node reads and
// not to this gate one file over. Deciding on the cached listing dropped a
// definition the API server knew still had a snapshot.
func TestRDDeleteRefusesOnASnapshotTheCacheHasNotSeen(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd-snapgate"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := backend.Snapshots().Create(ctx, &apiv1.Snapshot{Name: "snap-raced", ResourceName: "rd-snapgate"}); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}

	base, stop := startServerWithStore(t, cacheBlindSnapshotStore{backend})
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/rd-snapgate")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409 — the definition still has a snapshot", resp.StatusCode)
	}

	if _, err := backend.ResourceDefinitions().Get(ctx, "rd-snapgate"); err != nil {
		t.Errorf("the definition was deleted over its snapshot: %v", err)
	}
}

// fullListCounter counts whole-cluster replica listings.
type fullListCounter struct {
	store.ResourceStore

	full *atomic.Int64
}

func (f fullListCounter) List(ctx context.Context) ([]apiv1.Resource, error) {
	f.full.Add(1)

	return f.ResourceStore.List(ctx) //nolint:wrapcheck // test decorator
}

type fullListCountingStore struct {
	store.Store

	full *atomic.Int64
}

func (f fullListCountingStore) Resources() store.ResourceStore {
	return fullListCounter{ResourceStore: f.Store.Resources(), full: f.full}
}

// The refusal message named the node's replicas by listing every replica in
// the cluster, after the decision itself had already moved to the node-scoped
// read. A one-node question, answered with the read this change removes.
func TestNodeLostRefusalNamesReplicasWithoutAWholeClusterListing(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()

	if err := backend.Nodes().Create(ctx, &apiv1.Node{Name: "n-scoped"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := backend.Nodes().SetConnectionStatus(ctx, "n-scoped", apiv1.NodeTypeOnline); err != nil {
		t.Fatalf("seed status: %v", err)
	}

	if err := backend.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-on-scoped", NodeName: "n-scoped"}); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	var full atomic.Int64

	base, stop := startServerWithStore(t, fullListCountingStore{Store: backend, full: &full})
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n-scoped/lost", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}

	if n := full.Load(); n != 0 {
		t.Errorf("%d whole-cluster replica listing(s) to name one node's replicas", n)
	}
}
