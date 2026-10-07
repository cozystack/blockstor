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

package rest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// rdPatchHook runs before the n-th spec patch of target, counting from one,
// and may answer it in place of the store.
type rdPatchHook struct {
	store.ResourceDefinitionStore

	target string
	calls  *atomic.Int32
	hook   func(n int32) (error, bool)
}

func (h rdPatchHook) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	if name == h.target {
		if err, answered := h.hook(h.calls.Add(1)); answered {
			return err
		}
	}

	return h.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate) //nolint:wrapcheck // test double
}

type rdPatchHookStore struct {
	store.Store

	target string
	calls  *atomic.Int32
	hook   func(n int32) (error, bool)
}

func (s rdPatchHookStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return rdPatchHook{ResourceDefinitionStore: s.Store.ResourceDefinitions(), target: s.target, calls: s.calls, hook: s.hook}
}

func seedDefinitionWithReplica(t *testing.T, st store.Store, name string, props map[string]string) {
	t.Helper()

	ctx := t.Context()
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: name, Props: props}); err != nil {
		t.Fatalf("seed %s: %v", name, err)
	}

	if err := st.VolumeDefinitions().Create(ctx, name, &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024}); err != nil {
		t.Fatalf("seed the volume of %s: %v", name, err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: name, NodeName: "node-a"}); err != nil {
		t.Fatalf("seed the replica of %s: %v", name, err)
	}
}

func assertDefinitionAndReplicaStand(t *testing.T, st store.Store, name string) {
	t.Helper()

	if _, err := st.ResourceDefinitions().Get(t.Context(), name); err != nil {
		t.Errorf("the definition under %s was deleted: %v", name, err)
	}

	if _, err := st.Resources().Get(t.Context(), name, "node-a"); err != nil {
		t.Errorf("the replica of %s was deleted: %v", name, err)
	}
}

// The definition a rollback was for is gone by the time it marks it, and
// another writer has a definition under the same name. The rollback has
// nothing of its own left to remove, and reports so without deleting by name.
func TestARollbackWhoseDefinitionIsGoneDeletesNothingByName(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedDefinitionWithReplica(t, backend, "dst-gone", nil)

	var calls atomic.Int32

	st := rdPatchHookStore{Store: backend, target: "dst-gone", calls: &calls, hook: func(n int32) (error, bool) {
		if n == 1 {
			return store.ErrNotFound, true
		}

		return nil, false
	}}

	err := (&Server{Store: st}).rollBackCompensating(t.Context(), "dst-gone", []string{"node-a"}, rollbackEvenIfFinished)
	if err != nil {
		t.Errorf("rollback over a definition already gone = %v, want nil", err)
	}

	assertDefinitionAndReplicaStand(t, backend, "dst-gone")
}

// The definition under the name stops carrying the rollback's own in-progress
// mark between the handshake and the first delete: it was recreated, or an
// operator cleared the mark. Nothing is deleted by name, and the mark is not
// rewritten onto a definition that is not this rollback's.
func TestARollbackDeletesNothingOnceItsMarkIsGone(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedDefinitionWithReplica(t, backend, "dst-swapped", nil)

	var calls atomic.Int32

	st := rdPatchHookStore{Store: backend, target: "dst-swapped", calls: &calls, hook: func(n int32) (error, bool) {
		if n != 2 {
			return nil, false
		}

		// Between the handshake and the first destructive step, the
		// definition under the name loses the mark.
		_ = backend.ResourceDefinitions().PatchResourceDefinitionSpec(context.Background(), "dst-swapped",
			func(rd *apiv1.ResourceDefinition) error {
				delete(rd.Props, store.RollbackAbandonedProp)

				return nil
			})

		return nil, false
	}}

	err := (&Server{Store: st}).rollBackCompensating(t.Context(), "dst-swapped", []string{"node-a"}, rollbackEvenIfFinished)
	if !errors.Is(err, errRollbackMarkNotOurs) {
		t.Errorf("rollback = %v, want it to stop over a mark that is no longer its own", err)
	}

	assertDefinitionAndReplicaStand(t, backend, "dst-swapped")

	rd, err := backend.ResourceDefinitions().Get(t.Context(), "dst-swapped")
	if err == nil && rd.Props[store.RollbackAbandonedProp] != "" {
		t.Errorf("the rollback wrote %q onto a definition that is not its own", rd.Props[store.RollbackAbandonedProp])
	}
}

// A rollback whose first destructive step fails, and which then has no budget
// left to say where it stopped, still leaves a mark that names a step: it went
// on before the step ran. An operator cannot clear it, since the definition
// under it may already be part torn, and an in-progress mark is kept for
// definitions left whole.
func TestARollbackThatDiesTakingTheDefinitionApartLeavesAStepMark(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedDefinitionWithReplica(t, backend, "dst-torn", nil)

	var calls atomic.Int32

	var deleting atomic.Bool

	// Every write after the first delete was tried fails, the way an
	// exhausted budget fails it.
	st := rdPatchHookStore{Store: backend, target: "dst-torn", calls: &calls, hook: func(int32) (error, bool) {
		if deleting.Load() {
			return context.DeadlineExceeded, true
		}

		return nil, false
	}}

	srv := &Server{Store: replicaDeleteRefused{Store: st, tried: &deleting}}

	err := srv.rollBackCompensating(t.Context(), "dst-torn", []string{"node-a"}, rollbackEvenIfFinished)
	if err == nil {
		t.Fatal("a rollback whose replica delete failed reported success")
	}

	rd, err := backend.ResourceDefinitions().Get(t.Context(), "dst-torn")
	if err != nil {
		t.Fatalf("read the definition: %v", err)
	}

	mark := rd.Props[store.RollbackAbandonedProp]
	if mark != store.RollbackStepReapReplicas {
		t.Errorf("rollback mark = %q, want %q", mark, store.RollbackStepReapReplicas)
	}

	cleared := map[string]string{}
	if !errors.Is(store.RollbackMarkClearRefusal(rd.Props, cleared), store.ErrRollbackStepMarkKept) {
		t.Errorf("an operator may clear %q over a definition the rollback started taking apart", mark)
	}
}

// replicaDeleteRefused fails every replica delete and records that one was
// tried.
type replicaDeleteRefused struct {
	store.Store

	tried *atomic.Bool
}

func (r replicaDeleteRefused) Resources() store.ResourceStore {
	return replicaDeleteRefusedResources{ResourceStore: r.Store.Resources(), tried: r.tried}
}

type replicaDeleteRefusedResources struct {
	store.ResourceStore

	tried *atomic.Bool
}

func (r replicaDeleteRefusedResources) Delete(context.Context, string, string) error {
	r.tried.Store(true)

	return context.DeadlineExceeded
}

// A rollback whose mark was replaced does not tell the operator it failed to
// record that it started: it did, and somebody else's definition is there now.
func TestARollbackWhoseMarkWasReplacedSaysSo(t *testing.T) {
	t.Parallel()

	cause, _ := rollbackFailureAdvice(newRollbackError(rollbackStepMark,
		fmt.Errorf("%q: %w", "dst-replaced", errRollbackMarkNotOurs)), "dst-replaced")
	if strings.Contains(cause, "could not record that it started") {
		t.Errorf("advice for a replaced mark = %q, want it to say the mark was cleared or replaced", cause)
	}

	if !strings.Contains(cause, "cleared or replaced") {
		t.Errorf("advice for a replaced mark = %q", cause)
	}
}

// A definition gone by the time the rollback enters its destructive steps is
// left gone: a definition recreated under the name since is somebody else's,
// and the cascade goes by name.
func TestARollbackWhoseDefinitionGoesBeforeItsFirstDeleteDeletesNothing(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedDefinitionWithReplica(t, backend, "dst-regone", nil)

	var calls atomic.Int32

	st := rdPatchHookStore{Store: backend, target: "dst-regone", calls: &calls, hook: func(n int32) (error, bool) {
		if n != 2 {
			return nil, false
		}

		// The step-mark write finds no definition: it went, and another
		// writer has already put a new one with a replica under the name.
		return store.ErrNotFound, true
	}}

	err := (&Server{Store: st}).rollBackCompensating(t.Context(), "dst-regone", []string{"node-a"}, rollbackEvenIfFinished)
	if err != nil {
		t.Errorf("rollback over a definition that went = %v, want nil", err)
	}

	assertDefinitionAndReplicaStand(t, backend, "dst-regone")
}
