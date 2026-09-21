// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// A cancelled request context is what both an abandoned caller and a SIGTERM
// hand a post-write door: the server gives every request the runnable's own
// context as its base, and the manager cancels that on shutdown. On that
// context the group re-read came back cancelled, which parentRGSurvived can
// only call "could not check", and the door answered success over a group
// that is gone. Every door, driven directly with a context that is already
// cancelled.
func TestPostWriteGroupCheckIsNotEndedByTheCaller(t *testing.T) {
	t.Parallel()

	doors := map[string]func(ctx context.Context, s *Server, rec *httptest.ResponseRecorder, made materialisedRD) bool{
		"clone": func(ctx context.Context, s *Server, rec *httptest.ResponseRecorder, made materialisedRD) bool {
			_, ok := s.cloneParentRGSurvived(ctx, rec,
				&apiv1.ResourceDefinition{Name: "cancel-src", ResourceGroupName: made.StampedRG}, made.Name, made)

			return ok
		},
		"restore": func(ctx context.Context, s *Server, rec *httptest.ResponseRecorder, made materialisedRD) bool {
			_, ok := s.restoreParentRGSurvived(ctx, rec, made)

			return ok
		},
		"volume-less": func(ctx context.Context, s *Server, rec *httptest.ResponseRecorder, made materialisedRD) bool {
			_, ok := s.cloneShellParentRGSurvived(ctx, rec, "cancel-src", made.Name, made.StampedRG)

			return ok
		},
	}

	for door, check := range doors {
		t.Run(door, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			seedAdoptedTarget(t, st, "cancel-dst", "grp-cancel-gone")

			made := createdRD("cancel-dst", "grp-cancel-gone")
			made.Placed = []string{"node-a"}

			ended, cancel := context.WithCancel(t.Context())
			cancel()

			rec := httptest.NewRecorder()

			if check(ended, &Server{Store: st}, rec, made) {
				t.Fatalf("the door passed over a group that is gone because its caller had ended; body %s",
					rec.Body.String())
			}

			if _, err := st.ResourceDefinitions().Get(t.Context(), "cancel-dst"); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("the definition was not rolled back: %v", err)
			}
		})
	}
}

// rgGetGate holds the post-write group re-read, the first read of the group
// once the target exists, until the test has abandoned the request. It then
// refuses on an ended context the way a real API client does; the in-memory
// store ignores the context entirely.
type rgGetGate struct {
	store.ResourceGroupStore

	backend store.Store
	target  string
	reached chan struct{}
	release chan struct{}
	once    *sync.Once
}

func (g rgGetGate) Get(ctx context.Context, name string) (apiv1.ResourceGroup, error) {
	if _, err := g.backend.ResourceDefinitions().Get(ctx, g.target); err == nil {
		g.once.Do(func() { close(g.reached) })

		<-g.release

		select {
		case <-ctx.Done():
			return apiv1.ResourceGroup{}, errors.Wrap(ctx.Err(), "get the group on an ended context")
		case <-time.After(500 * time.Millisecond):
		}
	}

	return g.ResourceGroupStore.Get(ctx, name) //nolint:wrapcheck // pass-through in a fixture
}

type rgGetGateStore struct {
	store.Store

	gate rgGetGate
}

func (s rgGetGateStore) ResourceGroups() store.ResourceGroupStore {
	gate := s.gate
	gate.ResourceGroupStore = s.Store.ResourceGroups()

	return gate
}

// Ivan's shape on the data path: the caller gives up while the post-write
// check is reading the group. The read has to finish on its own context and
// roll the clone back; on the request's context it came back cancelled, the
// clone answered 201 to nobody, and the definition stayed parented to a group
// that is gone.
func TestRDCloneGroupCheckOutlivesTheRequest(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedGroupedCloneSource(t, backend, "src-rgget", "grp-rgget-gone", false)

	gated := rgGetGateStore{Store: backend, gate: rgGetGate{
		backend: backend,
		target:  "dst-rgget",
		reached: make(chan struct{}),
		release: make(chan struct{}),
		once:    &sync.Once{},
	}}

	base, stop := startServerWithStore(t, gated)
	defer stop()

	abandonAtTheGate(t, base+"/v1/resource-definitions/src-rgget/clone",
		map[string]any{"name": "dst-rgget", "use_zfs_clone": true}, gated.gate.reached, gated.gate.release)

	waitForDefinitionGone(t, backend, "dst-rgget")
}

// deadlineRecordingRDs records what context the spawn rollback's delete ran on.
type deadlineRecordingRDs struct {
	store.ResourceDefinitionStore

	sawDeadline *time.Duration
	sawEnded    *bool
}

func (d deadlineRecordingRDs) Delete(ctx context.Context, name string) error {
	if deadline, ok := ctx.Deadline(); ok {
		*d.sawDeadline = time.Until(deadline)
	}

	*d.sawEnded = ctx.Err() != nil

	return d.ResourceDefinitionStore.Delete(ctx, name) //nolint:wrapcheck // pass-through in a fixture
}

type deadlineRecordingStore struct {
	store.Store

	rds deadlineRecordingRDs
}

func (s deadlineRecordingStore) ResourceDefinitions() store.ResourceDefinitionStore {
	rds := s.rds
	rds.ResourceDefinitionStore = s.Store.ResourceDefinitions()

	return rds
}

// rollbackSpawn detached from the caller with no bound, so the graceful
// shutdown window derived from the rollback budget did not cover it. It runs
// on the same budget now.
func TestSpawnRollbackIsBoundedLikeTheOtherCompensations(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	if err := backend.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "spawn-half"}); err != nil {
		t.Fatalf("seed the half-spawned definition: %v", err)
	}

	var (
		deadline time.Duration
		ended    bool
	)

	st := deadlineRecordingStore{Store: backend, rds: deadlineRecordingRDs{sawDeadline: &deadline, sawEnded: &ended}}

	caller, cancel := context.WithCancel(t.Context())
	cancel()

	rollbackSpawn(caller, st, "spawn-half")

	if ended {
		t.Error("the spawn rollback ran on the caller's ended context")
	}

	if deadline <= 0 || deadline > detachedRollbackBudget {
		t.Errorf("the spawn rollback ran with %s left, want a deadline within %s", deadline, detachedRollbackBudget)
	}

	if _, err := backend.ResourceDefinitions().Get(t.Context(), "spawn-half"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the half-spawned definition survived its rollback: %v", err)
	}
}

// The budget has to cover the read that decides the rollback as well as the
// rollback, now that both run on it.
func TestRollbackBudgetCoversTheGroupRecheck(t *testing.T) {
	t.Parallel()

	if detachedRollbackBudget < groupRecheckBudget+2*cacheConvergeBudget {
		t.Errorf("budget %s does not cover the group re-read %s and two convergence waits of %s",
			detachedRollbackBudget, groupRecheckBudget, cacheConvergeBudget)
	}

	if groupRecheckBudget < cacheRetryAttempts*cacheRetryDelay {
		t.Errorf("group re-read budget %s is under the cache retry it has to cover", groupRecheckBudget)
	}
}

// The volume-less door kept one hardcoded cause and "delete it by hand" after
// it gained the shared four-step rollback. With a snapshot on the shell that
// advice is a dead end, because `rd d` refuses a definition that has
// snapshots; the step-correct advice names the snapshot.
func TestRDCloneOfAVolumelessSourceAdvisesForTheStepThatFailed(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "src-shell-snap",
		ResourceGroupName: "grp-shell-snap-gone",
	}); err != nil {
		t.Fatalf("seed the volume-less source: %v", err)
	}

	// A snapshot row under the target name, which is what the rollback's
	// snapshot refusal finds on the shell.
	if err := backend.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name: "snap-on-shell", ResourceName: "dst-shell-snap",
	}); err != nil {
		t.Fatalf("seed the snapshot on the target: %v", err)
	}

	base, stop := startServerWithStore(t, backend)
	defer stop()

	resp := postClone(t, base, "src-shell-snap", map[string]any{"name": "dst-shell-snap"})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500: the rollback refuses over a snapshot", resp.StatusCode)
	}

	rc := decodeCloneMessage(t, resp)
	if !strings.Contains(rc.Correc, "snapshot") {
		t.Errorf("correction %q does not name the snapshot that stopped the rollback", rc.Correc)
	}

	if !strings.Contains(rc.Message, "or it was never there") {
		t.Errorf("message %q asserts a concurrent delete without the other reading", rc.Message)
	}
}

// Every door's failed-rollback message offers both readings of a group that
// does not exist, as the success path does: deleted while the operation ran,
// or never there. The fixture here never created the group at all.
func TestRDCloneFailedRollbackDoesNotAssertARace(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedGroupedCloneSource(t, backend, "src-noracs", "grp-never-there", false)

	base, stop := startServerWithStore(t, failingRDDeleteStore{backend})
	defer stop()

	resp := postClone(t, base, "src-noracs", map[string]any{"name": "dst-noracs", "use_zfs_clone": true})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}

	rc := decodeCloneMessage(t, resp)

	if strings.Contains(rc.Message, "deleted concurrently") {
		t.Errorf("message %q asserts a concurrent delete", rc.Message)
	}

	if !strings.Contains(rc.Message, "or it was never there") {
		t.Errorf("message %q does not offer the never-existed reading", rc.Message)
	}
}
