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

package store_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// claimPatchHook runs before on every spec patch, then lets it through, and
// fails it after it landed when landThenFail is set.
type claimPatchHook struct {
	store.ResourceDefinitionStore

	before       func()
	landThenFail bool
	failNoLand   error
	patches      *atomic.Int32
	// cancel ends the caller's context with a landed write that fails, the
	// way a write at the request's deadline does.
	cancel context.CancelFunc
}

// GetUncached refuses a dead context, as a real client does.
func (r claimPatchHook) GetUncached(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	if err := ctx.Err(); err != nil {
		return apiv1.ResourceDefinition{}, err //nolint:wrapcheck // test double
	}

	return r.ResourceDefinitionStore.GetUncached(ctx, name) //nolint:wrapcheck // test double
}

func (r claimPatchHook) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	r.patches.Add(1)

	if r.before != nil {
		r.before()
	}

	if r.failNoLand != nil {
		return r.failNoLand
	}

	err := r.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate)
	if err == nil && r.landThenFail {
		if r.cancel != nil {
			r.cancel()
		}

		return context.DeadlineExceeded
	}

	return err //nolint:wrapcheck // test double
}

type claimPatchHookStore struct {
	store.Store

	hook claimPatchHook
}

func (s claimPatchHookStore) ResourceDefinitions() store.ResourceDefinitionStore {
	h := s.hook
	h.ResourceDefinitionStore = s.Store.ResourceDefinitions()

	if h.patches == nil {
		h.patches = new(atomic.Int32)
	}

	return h
}

func seedClaimTarget(t *testing.T, st store.Store, name string, props map[string]string) {
	t.Helper()

	if err := st.ResourceDefinitions().Create(t.Context(),
		&apiv1.ResourceDefinition{Name: name, Props: props}); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

func claimTargetProps(t *testing.T, st store.Store, name string) map[string]string {
	t.Helper()

	rd, err := st.ResourceDefinitions().Get(t.Context(), name)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}

	return rd.Props
}

func claimSnap() *apiv1.Snapshot {
	return &apiv1.Snapshot{VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1024}}}
}

// A claim over a definition the creator is rolling back writes nothing: the
// refusal is decided inside the one patch, before anything is written, so
// there is no mark and no volume record to take back off. One rolling back
// when the patch runs is refused the same as one already marked.
func TestAClaimOverARollbackWritesNothing(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		seeded string
		late   bool
	}{
		{name: "rollback in progress", seeded: store.RollbackInProgress},
		{name: "rollback taking it apart", seeded: store.RollbackStepReapReplicas},
		{name: "rollback lands just before the claim", late: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()

			props := map[string]string{store.RestoreVolumesProp: "0=2048"}
			if tc.seeded != "" {
				props[store.RollbackAbandonedProp] = tc.seeded
			}

			seedClaimTarget(t, backend, "pvc-rb", props)

			var st store.Store = backend
			if tc.late {
				st = claimPatchHookStore{Store: backend, hook: claimPatchHook{before: func() {
					_ = backend.ResourceDefinitions().PatchResourceDefinitionSpec(context.Background(), "pvc-rb",
						func(rd *apiv1.ResourceDefinition) error {
							rd.Props[store.RollbackAbandonedProp] = store.RollbackInProgress

							return nil
						})
				}}}
			}

			err := store.ClaimAdoptedLeftover(t.Context(), st, "pvc-rb", claimSnap())
			if !errors.Is(err, store.ErrAdoptedLeftoverRollingBack) {
				t.Fatalf("claim over a rollback = %v, want ErrAdoptedLeftoverRollingBack", err)
			}

			got := claimTargetProps(t, backend, "pvc-rb")
			if mark, ok := got[store.RestoreAdoptedProp]; ok {
				t.Errorf("a refused claim wrote adoption mark %q", mark)
			}

			if got[store.RestoreVolumesProp] != "0=2048" {
				t.Errorf("a refused claim changed the volume record to %q", got[store.RestoreVolumesProp])
			}
		})
	}
}

// An accepted claim writes the mark and the volumes snap holds; without a
// snapshot it records nothing.
func TestAnAcceptedClaimMarksAndRecords(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedClaimTarget(t, backend, "pvc-snap", nil)
	seedClaimTarget(t, backend, "pvc-nosnap", map[string]string{store.RestoreVolumesProp: "0=2048"})

	if err := store.ClaimAdoptedLeftover(t.Context(), backend, "pvc-snap", claimSnap()); err != nil {
		t.Fatalf("claim: %v", err)
	}

	if err := store.ClaimAdoptedLeftover(t.Context(), backend, "pvc-nosnap", nil); err != nil {
		t.Fatalf("claim without a snapshot: %v", err)
	}

	withSnap := claimTargetProps(t, backend, "pvc-snap")
	if withSnap[store.RestoreAdoptedProp] == "" {
		t.Error("an accepted claim left no adoption mark")
	}

	if withSnap[store.RestoreVolumesProp] != "0=1024" {
		t.Errorf("volume record = %q, want the claim's snapshot", withSnap[store.RestoreVolumesProp])
	}

	if got := claimTargetProps(t, backend, "pvc-nosnap")[store.RestoreVolumesProp]; got != "0=2048" {
		t.Errorf("a claim without a snapshot changed the volume record to %q", got)
	}
}

// Claims that keep coming for one definition all answer, and the mark is one
// token, the last claim's.
func TestRepeatedClaimsAllAnswer(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedClaimTarget(t, backend, "pvc-many", nil)

	var last string

	for range 3 {
		if err := store.ClaimAdoptedLeftover(t.Context(), backend, "pvc-many", claimSnap()); err != nil {
			t.Fatalf("claim: %v", err)
		}

		mark := claimTargetProps(t, backend, "pvc-many")[store.RestoreAdoptedProp]
		if mark == "" || mark == last {
			t.Fatalf("mark after a claim = %q, want a fresh token (previous %q)", mark, last)
		}

		last = mark
	}
}

// A claim whose write landed and still answered an error is accepted: its own
// token is there to read back.
func TestAClaimWhoseWriteLandsAndErrsIsAccepted(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedClaimTarget(t, backend, "pvc-landed", nil)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	st := claimPatchHookStore{Store: backend, hook: claimPatchHook{landThenFail: true, cancel: cancel}}

	if err := store.ClaimAdoptedLeftover(ctx, st, "pvc-landed", claimSnap()); err != nil {
		t.Errorf("a claim whose write landed = %v, want it accepted", err)
	}
}

var errRefused = errors.New("apiserver refused the patch")

// A claim whose write did not land answers the error and leaves no mark.
func TestAClaimWhoseWriteFailsAnswersTheError(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedClaimTarget(t, backend, "pvc-failed", nil)

	st := claimPatchHookStore{Store: backend, hook: claimPatchHook{failNoLand: errRefused}}

	if err := store.ClaimAdoptedLeftover(t.Context(), st, "pvc-failed", claimSnap()); !errors.Is(err, errRefused) {
		t.Errorf("a claim whose write failed = %v, want the write's error", err)
	}

	if mark, ok := claimTargetProps(t, backend, "pvc-failed")[store.RestoreAdoptedProp]; ok {
		t.Errorf("a failed claim left adoption mark %q", mark)
	}
}

// A definition recreated under the name before the claim's patch is marked
// like any other: an adoption mark only ever makes a rollback leave a
// definition in place.
func TestAClaimMarksADefinitionRecreatedUnderTheName(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedClaimTarget(t, backend, "pvc-recreated", nil)

	st := claimPatchHookStore{Store: backend, hook: claimPatchHook{before: func() {
		_ = backend.ResourceDefinitions().Delete(context.Background(), "pvc-recreated")
		_ = backend.ResourceDefinitions().Create(context.Background(), &apiv1.ResourceDefinition{Name: "pvc-recreated"})
	}}}

	if err := store.ClaimAdoptedLeftover(t.Context(), st, "pvc-recreated", nil); err != nil {
		t.Fatalf("claim: %v", err)
	}

	if claimTargetProps(t, backend, "pvc-recreated")[store.RestoreAdoptedProp] == "" {
		t.Error("the recreated definition carries no adoption mark")
	}
}

// The CLI door's refusal over a rollback still in progress does not warn of a
// clear handing the definition to a retry the rollback then deletes: the
// rollback checks its own mark before its first delete, so a clear stops it.
func TestTheInProgressRefusalSaysAClearStopsTheRollback(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-inprog",
		Props: map[string]string{
			store.RestoreFromSnapshotProp: "pvc-src:snap-1",
			store.RollbackAbandonedProp:   store.RollbackInProgress,
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	snap := &apiv1.Snapshot{
		ResourceName: "pvc-src", Name: "snap-1",
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1024}},
	}

	_, _, err := store.JudgeRestoreLeftover(ctx, backend, "pvc-inprog", snap)
	if err == nil {
		t.Fatal("a leftover under a rollback in progress was not refused")
	}

	if !strings.Contains(err.Error(), "stops that rollback before it deletes anything") ||
		strings.Contains(err.Error(), "rollback then deletes") {
		t.Errorf("refusal = %q, want it to say a clear stops the rollback", err)
	}
}
