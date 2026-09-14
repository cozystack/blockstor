// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// lateWitness lands an unstamped replica on the target on the first listing
// after the rollback's deletes have come back empty: an auto-tiebreaker the
// controller stamped moments after placement, which the cascade's back-to-back
// passes miss and the wait is the first to see.
type lateWitness struct {
	store.ResourceStore

	target  string
	deleted *atomic.Bool
	emptied *atomic.Bool
	landed  *atomic.Bool
}

func (l lateWitness) Delete(ctx context.Context, rdName, node string) error {
	if rdName == l.target {
		l.deleted.Store(true)
	}

	return errors.Wrap(l.ResourceStore.Delete(ctx, rdName, node), "delete through the late-witness double")
}

func (l lateWitness) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	replicas, err := l.ResourceStore.ListByDefinition(ctx, rdName)
	if err != nil || rdName != l.target || !l.deleted.Load() {
		return replicas, errors.Wrap(err, "list through the late-witness double")
	}

	if len(replicas) == 0 && l.emptied.CompareAndSwap(false, true) {
		return replicas, nil
	}

	if l.emptied.Load() && l.landed.CompareAndSwap(false, true) {
		if err := l.Create(ctx, &apiv1.Resource{Name: rdName, NodeName: "node-witness"}); err != nil {
			return nil, errors.Wrap(err, "land the witness")
		}

		replicas, err = l.ResourceStore.ListByDefinition(ctx, rdName)
	}

	return replicas, errors.Wrap(err, "list through the late-witness double")
}

type lateWitnessStore struct {
	store.Store

	target  string
	deleted *atomic.Bool
	emptied *atomic.Bool
	landed  *atomic.Bool
}

func (s lateWitnessStore) Resources() store.ResourceStore {
	return lateWitness{
		ResourceStore: s.Store.Resources(), target: s.target,
		deleted: s.deleted, emptied: s.emptied, landed: s.landed,
	}
}

// The wait only re-read, so a replica that became visible after the cascade
// was watched for the whole budget and never told to go, and the rollback gave
// up over a replica one delete would have removed.
func TestRDCloneRollbackReapsAReplicaThatLandsDuringTheWait(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedGroupedCloneSource(t, backend, "src-witness", "grp-witness-gone", false)

	st := lateWitnessStore{
		Store: backend, target: "dst-witness",
		deleted: &atomic.Bool{}, emptied: &atomic.Bool{}, landed: &atomic.Bool{},
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-witness", map[string]any{"name": "dst-witness", "use_zfs_clone": true})
	_ = resp.Body.Close()

	if !st.landed.Load() {
		t.Fatal("the witness never landed, so this test proves nothing")
	}

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 — one delete removes the witness and finishes the rollback",
			resp.StatusCode)
	}

	if _, err := backend.ResourceDefinitions().Get(ctx, "dst-witness"); err == nil {
		t.Error("the definition survived a rollback that could have finished")
	}
}

// countedRetainedDeletes accepts every delete, keeps the replica listed and
// unstamped, and counts the calls: a cache that never catches up.
type countedRetainedDeletes struct {
	store.ResourceStore

	calls *atomic.Int32
}

func (c countedRetainedDeletes) Delete(context.Context, string, string) error {
	c.calls.Add(1)

	return nil
}

type countedRetainedStore struct {
	store.Store

	calls *atomic.Int32
}

func (c countedRetainedStore) Resources() store.ResourceStore {
	return countedRetainedDeletes{ResourceStore: c.Store.Resources(), calls: c.calls}
}

// The wait deletes what it sees, but a replica already deleted also lists
// unstamped while the cache trails. Deleting on every poll would send the API
// server a hundred deletes per replica over one budget.
func TestRDCloneRollbackWaitTellsEachReplicaToGoOnce(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedGroupedCloneSource(t, backend, "src-once", "grp-once-gone", false)

	calls := &atomic.Int32{}

	base, stop := startServerWithStore(t, countedRetainedStore{Store: backend, calls: calls})
	defer stop()

	resp := postClone(t, base, "src-once", map[string]any{"name": "dst-once", "use_zfs_clone": true})
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		t.Fatal("status = 404 over replicas that were never stamped")
	}

	// One replica: the delete by name, one per cascade pass, one from the wait.
	if limit := int32(1 + store.CascadeDeleteMaxPasses + 1); calls.Load() > limit {
		t.Errorf("%d deletes for one replica, want at most %d", calls.Load(), limit)
	}
}
