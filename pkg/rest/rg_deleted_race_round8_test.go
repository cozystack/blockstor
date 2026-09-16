// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// rdDeleteGate holds a rollback at the step both the bare delete and the
// shared rollback end on, the definition delete, until the test has abandoned
// the request. It then refuses on an ended context the way a real API client
// does; the in-memory store ignores the context entirely.
type rdDeleteGate struct {
	store.ResourceDefinitionStore

	target  string
	reached chan struct{}
	release chan struct{}
	once    *sync.Once
}

func (g rdDeleteGate) Delete(ctx context.Context, name string) error {
	if name != g.target {
		return g.ResourceDefinitionStore.Delete(ctx, name) //nolint:wrapcheck // pass-through in a fixture
	}

	g.once.Do(func() { close(g.reached) })

	<-g.release

	// An abandoned request reaches the handler's context asynchronously, so
	// give it the moment the round-7 gate gives it before deciding.
	select {
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "delete on an ended context")
	case <-time.After(500 * time.Millisecond):
	}

	return g.ResourceDefinitionStore.Delete(ctx, name) //nolint:wrapcheck // pass-through in a fixture
}

type rdDeleteGateStore struct {
	store.Store

	gate rdDeleteGate
}

func (s rdDeleteGateStore) ResourceDefinitions() store.ResourceDefinitionStore {
	gate := s.gate
	gate.ResourceDefinitionStore = s.Store.ResourceDefinitions()

	return gate
}

func newRDDeleteGateStore(backend store.Store, target string) rdDeleteGateStore {
	return rdDeleteGateStore{Store: backend, gate: rdDeleteGate{
		target:  target,
		reached: make(chan struct{}),
		release: make(chan struct{}),
		once:    &sync.Once{},
	}}
}

// The vol-less clone is the third post-write door. Its compensation ran on the
// request's own context, so a caller that gave up while the group check was
// still inside its NotFound budget left the shell behind, parented to a group
// that is gone. The shell has no marker and no replay gate, so every retry
// then meets AlreadyExists and 409s until someone deletes it by hand, and the
// 500 that would have said so went to a connection that was already closed.
func TestRDCloneShellRollbackOutlivesTheRequest(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()

	if err := backend.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name:              "shell-abandon-src",
		ResourceGroupName: "grp-shell-abandon-gone",
	}); err != nil {
		t.Fatalf("seed the vol-less source: %v", err)
	}

	gated := newRDDeleteGateStore(backend, "shell-abandon-dst")

	base, stop := startServerWithStore(t, gated)
	defer stop()

	abandonAtTheGate(t, base+"/v1/resource-definitions/shell-abandon-src/clone",
		map[string]any{"name": "shell-abandon-dst"}, gated.gate.reached, gated.gate.release)

	waitForDefinitionGone(t, backend, "shell-abandon-dst")
}

// The rollback runs inside the handler on a context Shutdown cannot cancel, so
// the process has to outlive it: the budget under the shutdown window, the
// window under the termination grace of every manifest that serves REST. A
// SIGTERM landing between those numbers cuts a cascade in half and kills the
// connection that would have named what was left.
func TestRollbackBudgetFitsTheShutdownWindow(t *testing.T) {
	t.Parallel()

	if detachedRollbackBudget+shutdownMargin > gracefulShutdownWindow {
		t.Errorf("rollback budget %s plus margin %s does not fit the shutdown window %s",
			detachedRollbackBudget, shutdownMargin, gracefulShutdownWindow)
	}

	// Both convergence waits the rollback can spend in full, back to back.
	if detachedRollbackBudget < 2*cacheConvergeBudget {
		t.Errorf("rollback budget %s is under its own two waits of %s; a rollback would be "+
			"cut where those waits are what keep the definition off unstamped replicas",
			detachedRollbackBudget, cacheConvergeBudget)
	}

	// Every manifest whose pod runs a process that serves REST: both server
	// binaries call rest.Server, and the satellite does not.
	for _, manifest := range []string{
		filepath.Join("..", "..", "config", "manager", "manager.yaml"),
		filepath.Join("..", "..", "stand", "blockstor-deploy.yaml"),
		filepath.Join("..", "..", "stand", "blockstor-apiserver-deploy.yaml"),
	} {
		grace := terminationGraceOf(t, manifest)

		if gracefulShutdownWindow+terminationGraceMargin > grace {
			t.Errorf("%s gives the pod %s; the shutdown window %s plus margin %s needs more",
				manifest, grace, gracefulShutdownWindow, terminationGraceMargin)
		}
	}
}

// terminationGraceOf reads terminationGracePeriodSeconds out of a manifest,
// falling back to the Kubernetes default when the field is absent.
func terminationGraceOf(t *testing.T, path string) time.Duration {
	t.Helper()

	const kubeletDefaultGrace = 30 * time.Second

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	match := regexp.MustCompile(`terminationGracePeriodSeconds:\s*(\d+)`).FindSubmatch(raw)
	if match == nil {
		return kubeletDefaultGrace
	}

	seconds, err := strconv.Atoi(string(match[1]))
	if err != nil {
		t.Fatalf("parse the grace period in %s: %v", path, err)
	}

	return time.Duration(seconds) * time.Second
}

// A materialisedRD that states no origin is not a claim of ownership. The
// producers go through the constructors; a literal that skips them, which is
// what a door adopting a leftover would add, must not authorise a cascade over
// a definition nobody said this request created.
func TestAMaterialisedRDWithNoStatedOriginIsNotThisRequestsToReap(t *testing.T) {
	t.Parallel()

	zero := materialisedRD{Name: "x", StampedRG: "g"}

	if zero.origin != rdOriginUnstated {
		t.Errorf("the zero value states origin %d; the unstated one is what a literal leaves", zero.origin)
	}

	if zero.createdHere() {
		t.Error("a materialisedRD with no stated origin authorised a rollback over its definition")
	}

	if adoptedRD("x", "g").createdHere() {
		t.Error("an adopted definition authorised a rollback over itself")
	}

	if !createdRD("x", "g").createdHere() {
		t.Error("a definition this request created was not its own to roll back")
	}
}

// And the doors read it the same way: an unstated origin is refused and left in
// place, exactly as an adopted one is.
func TestPostWriteGroupCheckLeavesADefinitionWithNoStatedOriginInPlace(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedAdoptedTarget(t, st, "unstated-dst", "grp-unstated-gone")

	rec := httptest.NewRecorder()
	made := materialisedRD{Name: "unstated-dst", StampedRG: "grp-unstated-gone", Placed: []string{"node-a"}}

	if _, ok := (&Server{Store: st}).restoreParentRGSurvived(t.Context(), rec, made); ok {
		t.Fatal("the check passed over a parent group that does not exist")
	}

	if _, err := st.ResourceDefinitions().Get(t.Context(), "unstated-dst"); err != nil {
		t.Errorf("a definition with no stated origin was rolled back: %v", err)
	}
}
