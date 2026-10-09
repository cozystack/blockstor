// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Every door stamps a group, so the grouped leftover is the mainline one, and
// the round-9 fixture reached only the ungrouped branch.
func TestRDCloneGroupedHalfPlacedByAFailedAttemptIsLeftForTheReplay(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedTwoNodeSource(t, backend, "src-half10")

	src, err := backend.ResourceDefinitions().Get(t.Context(), "src-half10")
	if err != nil {
		t.Fatalf("read the source: %v", err)
	}

	src.ResourceGroupName = "grp-half10"
	if err := backend.ResourceDefinitions().Update(t.Context(), &src); err != nil {
		t.Fatalf("parent the source: %v", err)
	}

	if err := backend.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{Name: "grp-half10"}); err != nil {
		t.Fatalf("seed the group: %v", err)
	}

	base, stop := startServerWithStore(t, halfPlacingStore{Store: backend, target: "dst-half10"})
	defer stop()

	first := postClone(t, base, "src-half10", map[string]any{"name": "dst-half10", "use_zfs_clone": true})
	_ = first.Body.Close()

	if first.StatusCode != http.StatusInternalServerError {
		t.Fatalf("first attempt = %d, want 500: placement failed on node-b", first.StatusCode)
	}

	leftover, err := backend.ResourceDefinitions().Get(t.Context(), "dst-half10")
	if err != nil || leftover.ResourceGroupName != "grp-half10" {
		t.Fatalf("want the grouped leftover left in place, got group %q (err=%v)", leftover.ResourceGroupName, err)
	}

	assertHalfPlacedCloneLeftForTheReplay(t, backend, base, "src-half10", "dst-half10")
}

// deadlineResources blocks a replica delete until its context ends, the way a
// call to a slow apiserver runs out the budget mid-cascade.
type deadlineResources struct{ store.ResourceStore }

func (d deadlineResources) Delete(ctx context.Context, _, _ string) error {
	<-ctx.Done()

	return ctx.Err() //nolint:wrapcheck // the context's own error is the point
}

// deadlineRDs refuses a write on a context that has already ended, which the
// Kubernetes store does and the in-memory one does not.
type deadlineRDs struct{ store.ResourceDefinitionStore }

func (d deadlineRDs) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck // the context's own error is the point
	}

	return d.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate) //nolint:wrapcheck // pass-through
}

type deadlineStore struct{ store.Store }

func (d deadlineStore) Resources() store.ResourceStore {
	return deadlineResources{d.Store.Resources()}
}

func (d deadlineStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return deadlineRDs{d.Store.ResourceDefinitions()}
}

// The mark used to be written after the rollback had failed, on the context
// the rollback had just run out. When the budget expires mid-cascade that
// write cannot land, and neither can anything after a kill, which are exactly
// the leftovers that are half torn down.
func TestAnAbandonedRollbackIsMarkedEvenWhenItsBudgetRunsOut(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, backend, "dst-budget10")

	s := &Server{Store: deadlineStore{backend}}

	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 300*time.Millisecond)
	defer cancel()

	if err := s.rollBackCompensating(rollbackCtx, "dst-budget10", []string{"node-a"}, rollbackEvenIfFinished); err == nil {
		t.Fatal("fixture: the rollback was supposed to run out of budget")
	}

	leftover, err := backend.ResourceDefinitions().Get(ctx, "dst-budget10")
	if err != nil {
		t.Fatalf("read the leftover: %v", err)
	}

	if leftover.Props[rollbackAbandonedKey] == "" {
		t.Fatal("a rollback that ran out of budget mid-cascade left no mark, so the replay would answer 201 over it")
	}

	if _, refusal := (&Server{Store: backend}).abandonedRollbackRefusal(ctx, "clone", "dst-budget10"); refusal == nil {
		t.Error("the replay gate did not refuse over the mark")
	}
}

// deadlineRecorder remembers the deadline of the first context a rollback
// hands the store.
type deadlineRecorder struct {
	store.SnapshotStore

	mu   *sync.Mutex
	seen *time.Time
}

func (d deadlineRecorder) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Snapshot, error) {
	d.mu.Lock()

	if d.seen.IsZero() {
		if deadline, ok := ctx.Deadline(); ok {
			*d.seen = deadline
		}
	}

	d.mu.Unlock()

	return d.SnapshotStore.ListByDefinition(ctx, rdName) //nolint:wrapcheck // pass-through
}

type deadlineRecorderStore struct {
	store.Store

	rec deadlineRecorder
}

func (d deadlineRecorderStore) Snapshots() store.SnapshotStore {
	d.rec.SnapshotStore = d.Store.Snapshots()

	return d.rec
}

// A door detaches once, reads the group on that context, and hands the
// rollback what is left of the same budget. Detaching again inside the
// rollback restarted it, since WithoutCancel drops the deadline, so the chain
// the termination grace was derived from was not the one the code ran.
func TestTheRollbackRunsOnWhatIsLeftOfTheDoorsBudget(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedDeployedCloneSource(t, backend, "dst-chain10")

	var seen time.Time

	s := &Server{Store: deadlineRecorderStore{Store: backend, rec: deadlineRecorder{mu: &sync.Mutex{}, seen: &seen}}}

	doorCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Second)
	defer cancel()

	doorDeadline, _ := doorCtx.Deadline()

	_ = s.rollBackCompensating(doorCtx, "dst-chain10", nil, rollbackEvenIfFinished)

	if seen.IsZero() {
		t.Fatal("fixture: the rollback's context carried no deadline at all")
	}

	if seen.After(doorDeadline) {
		t.Errorf("the rollback ran to %s past the door's deadline: it restarted the budget",
			seen.Sub(doorDeadline).Round(time.Millisecond))
	}
}
