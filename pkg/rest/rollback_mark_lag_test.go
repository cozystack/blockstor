// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"errors"
	"maps"
	"strings"
	"sync/atomic"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// cacheBehindMark hands one definition patch, the lagOn-th, a view without the
// rollback mark, the way an informer cache that has not seen the mark's own
// write yet reads to the patch's closure. Every other patch sees the store.
type cacheBehindMark struct {
	store.Store

	patches *atomic.Int32
	lagOn   int32
}

type cacheBehindMarkRDs struct {
	store.ResourceDefinitionStore

	outer cacheBehindMark
}

func (c cacheBehindMark) ResourceDefinitions() store.ResourceDefinitionStore {
	return cacheBehindMarkRDs{ResourceDefinitionStore: c.Store.ResourceDefinitions(), outer: c}
}

func (r cacheBehindMarkRDs) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	if r.outer.patches.Add(1) != r.outer.lagOn {
		return r.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate) //nolint:wrapcheck // pass-through test double
	}

	// A closure deciding from a stale object refuses before anything is
	// written, so nothing reaches the store on this call.
	rd, err := r.GetUncached(ctx, name)
	if err != nil {
		return err //nolint:wrapcheck // pass-through test double
	}

	// A copy: the store hands out its own map, and the stale view must not
	// write through to it.
	rd.Props = maps.Clone(rd.Props)
	delete(rd.Props, store.RollbackAbandonedProp)

	return mutate(&rd)
}

// A cache behind the rollback's own mark refuses the step write as if the mark
// were not the rollback's. The live definition still carries it, so the
// rollback stands down, deletes nothing and takes the mark off, and a retry is
// not refused for good over a definition nobody is rolling back.
func TestARollbackWhoseCacheTrailsItsMarkLeavesNoMark(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()
	seedRolledBackDefinition(t, backend, "dst-lag")

	// The first patch writes the in-progress mark, the second enters the
	// destructive steps.
	srv := &Server{Store: cacheBehindMark{Store: backend, patches: &atomic.Int32{}, lagOn: 2}}

	err := srv.rollBackCompensating(ctx, "dst-lag", []string{"node-a"}, rollbackEvenIfFinished)
	if err == nil {
		t.Fatal("the rollback reported success over a write that refused")
	}

	left, getErr := backend.ResourceDefinitions().Get(ctx, "dst-lag")
	if getErr != nil {
		t.Fatalf("the definition was deleted: %v", getErr)
	}

	if mark, ok := left.Props[store.RollbackAbandonedProp]; ok {
		t.Errorf("the definition still carries rollback mark %q, so every retry is refused over it", mark)
	}

	if replicas, _ := backend.Resources().ListByDefinition(ctx, "dst-lag"); len(replicas) != 1 ||
		replicaAcceptedForDeletion(&replicas[0]) {
		t.Errorf("the replica was reaped: %v", replicas)
	}

	if _, refusal := (&Server{Store: backend}).abandonedRollbackRefusal(ctx, "clone", "dst-lag"); refusal != nil {
		t.Errorf("a retry is refused: %s; %s", refusal.Message, refusal.Correc)
	}
}

// A mark that really is gone stays not the rollback's: an operator who cleared
// it gets the definition left alone, mark and all.
func TestARollbackWhoseMarkWasClearedLeavesTheDefinitionAlone(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "dst-cleared", Props: map[string]string{store.RollbackAbandonedProp: "reap-replicas"},
	}); err != nil {
		t.Fatalf("seed the definition: %v", err)
	}

	gone, err := (&Server{Store: backend}).enterDestructiveRollback(ctx, "dst-cleared")
	if gone || !errors.Is(err, errRollbackMarkNotOurs) {
		t.Fatalf("enter = %v, %v; want the mark reported as not the rollback's", gone, err)
	}

	left, _ := backend.ResourceDefinitions().Get(ctx, "dst-cleared")
	if mark := left.Props[store.RollbackAbandonedProp]; mark != "reap-replicas" {
		t.Errorf("another rollback's mark = %q, want it left as %q", mark, "reap-replicas")
	}
}

// failsAfterMark lets the first definition patch, the in-progress mark,
// through and fails every one after it, and fails every live read: the steps
// that judge adoption or completion fail, and so does taking the mark off.
type failsAfterMark struct {
	handshakeBlind

	patches *atomic.Int32
}

type failsAfterMarkRDs struct {
	handshakeBlindRDs

	patches *atomic.Int32
}

func (f failsAfterMark) ResourceDefinitions() store.ResourceDefinitionStore {
	return failsAfterMarkRDs{
		handshakeBlindRDs: handshakeBlindRDs{ResourceDefinitionStore: f.Store.ResourceDefinitions(), outer: f.handshakeBlind},
		patches:           f.patches,
	}
}

func (r failsAfterMarkRDs) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	if r.patches.Add(1) > 1 {
		return errPatchRefused
	}

	return r.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate) //nolint:wrapcheck // pass-through test double
}

// A rollback that stops at reading the adoption and then cannot take its mark
// off leaves a definition every retry is refused over. Deleting it by hand
// would take a volume a retry may have answered for, so the advice is the
// command that clears the mark.
func TestAStuckMarkAfterAFailedHandshakeReadNamesTheClear(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()
	seedRolledBackDefinition(t, backend, "dst-stuckread")

	srv := &Server{Store: failsAfterMark{
		handshakeBlind: handshakeBlind{Store: backend, failLiveRead: true},
		patches:        &atomic.Int32{},
	}}

	err := srv.rollBackCompensating(ctx, "dst-stuckread", []string{"node-a"}, rollbackUnlessPlaced)

	var stuck *rollbackMarkStuckError
	if !errors.As(err, &stuck) {
		t.Fatalf("rollback = %v, want it to report the mark it could not take off", err)
	}

	if got := rollbackStepName(err); got != "read-adoption" {
		t.Errorf("step = %q, want read-adoption", got)
	}

	_, correc := rollbackFailureAdvice(err, "dst-stuckread")
	if !strings.Contains(correc, "set-property dst-stuckread "+store.RollbackAbandonedProp) || strings.Contains(correc, "by hand") {
		t.Errorf("correction %q does not name the clear, or still says to delete by hand", correc)
	}
}
