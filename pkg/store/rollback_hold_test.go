// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"errors"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

var errPatchRefused = errors.New("apiserver refused the patch")

// patchRefusingRDs fails every spec patch, the way an API server that cannot be
// reached does.
type patchRefusingRDs struct{ store.ResourceDefinitionStore }

func (patchRefusingRDs) PatchResourceDefinitionSpec(
	context.Context, string, func(*apiv1.ResourceDefinition) error,
) error {
	return errPatchRefused
}

type patchRefusingStore struct{ store.Store }

func (p patchRefusingStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return patchRefusingRDs{p.Store.ResourceDefinitions()}
}

var errReadRefused = errors.New("apiserver refused the read")

// readRefusingRDs lets the mark through and fails the authoritative read that
// follows it, the read the rollback decides on.
type readRefusingRDs struct{ store.ResourceDefinitionStore }

func (readRefusingRDs) GetUncached(context.Context, string) (apiv1.ResourceDefinition, error) {
	return apiv1.ResourceDefinition{}, errReadRefused
}

type readRefusingStore struct{ store.Store }

func (r readRefusingStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return readRefusingRDs{r.Store.ResourceDefinitions()}
}

// The rollback half of the handshake: a definition no request adopted may be
// deleted, one a request adopted is left to it, and one whose mark could not be
// written is not deleted at all.
func TestHoldRollback(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		adopted    bool
		finished   bool
		refuse     bool
		refuseRead bool
		want       error
		mayGo      bool
	}{
		{name: "nobody adopted it", mayGo: true},
		{name: "a request adopted it", adopted: true, want: store.ErrRollbackYielded},
		{name: "it already reads as finished", finished: true, want: store.ErrRollbackAnswered},
		{name: "the mark cannot be written", refuse: true, want: errPatchRefused},
		{name: "the adoption cannot be read", refuseRead: true, want: errReadRefused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			backend := store.NewInMemory()

			props := map[string]string{}
			if tc.adopted {
				props[store.RestoreAdoptedProp] = "2026-10-07T00:00:00Z"
			}

			if err := backend.ResourceDefinitions().Create(ctx,
				&apiv1.ResourceDefinition{Name: "pvc-new", Props: props}); err != nil {
				t.Fatalf("seed: %v", err)
			}

			if tc.finished {
				seedFinishedLeftover(t, backend, "pvc-new")
			}

			var st store.Store = backend
			if tc.refuse {
				st = patchRefusingStore{backend}
			}

			if tc.refuseRead {
				st = readRefusingStore{backend}
			}

			err := store.HoldRollback(ctx, st, "pvc-new", true)
			if tc.mayGo {
				if err != nil {
					t.Fatalf("HoldRollback = %v, want nil", err)
				}

				return
			}

			if !errors.Is(err, tc.want) {
				t.Fatalf("HoldRollback = %v, want %v", err, tc.want)
			}

			if !errors.Is(tc.want, store.ErrRollbackAnswered) && errors.Is(err, store.ErrRollbackAnswered) {
				t.Fatalf("HoldRollback = %v: a failure reported as a finished leftover", err)
			}

			if !errors.Is(tc.want, store.ErrRollbackYielded) && errors.Is(err, store.ErrRollbackYielded) {
				t.Fatalf("HoldRollback = %v: a failure reported as a yield", err)
			}

			rd, getErr := backend.ResourceDefinitions().Get(ctx, "pvc-new")
			if getErr != nil {
				t.Fatalf("read back: %v", getErr)
			}

			if mark, ok := rd.Props[store.RollbackAbandonedProp]; ok {
				t.Errorf("a rollback that may not delete left mark %q", mark)
			}
		})
	}
}

// seedFinishedLeftover gives a definition a volume and a live replica, which
// is what a replay and the clone status poll answer as finished.
func seedFinishedLeftover(t *testing.T, st store.Store, rdName string) {
	t.Helper()

	ctx := t.Context()

	if err := st.VolumeDefinitions().Create(ctx, rdName,
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024}); err != nil {
		t.Fatalf("seed the volume: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: rdName, NodeName: "node-a"}); err != nil {
		t.Fatalf("seed the replica: %v", err)
	}
}

// A definition being deleted is not finished, to the replay, the poll or the
// rollback: it holds every volume and a replica and is still not spared.
func TestReadsAsFinishedIsFalseForADefinitionBeingDeleted(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		deleting bool
		want     bool
	}{
		{name: "whole", want: true},
		{name: "being-deleted", deleting: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			backend := store.NewInMemory()

			rd := &apiv1.ResourceDefinition{Name: "pvc-flagged"}
			if tc.deleting {
				rd.Flags = []string{apiv1.ResourceFlagDelete}
			}

			if err := backend.ResourceDefinitions().Create(ctx, rd); err != nil {
				t.Fatalf("seed: %v", err)
			}

			seedFinishedLeftover(t, backend, "pvc-flagged")

			got, err := store.ReadsAsFinished(ctx, backend, "pvc-flagged", true)
			if err != nil || got != tc.want {
				t.Errorf("ReadsAsFinished = %v, %v; want %v", got, err, tc.want)
			}

			err = store.HoldRollback(ctx, backend, "pvc-flagged", true)
			if tc.want != errors.Is(err, store.ErrRollbackAnswered) {
				t.Errorf("HoldRollback = %v, want answered=%v", err, tc.want)
			}
		})
	}
}

// The rule an operator's edit of a definition is held to over the rollback
// mark: the mark over a definition left whole may be deleted, one naming a
// step past that may not, and an edit that leaves the mark alone goes through
// whatever it says.
func TestRollbackMarkClearRefusal(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		before, after map[string]string
		refused       bool
	}{
		{name: "no-mark", before: map[string]string{"Aux/x": "1"}, after: map[string]string{}},
		{
			name:   "in-progress-deleted",
			before: map[string]string{store.RollbackAbandonedProp: store.RollbackInProgress}, after: map[string]string{},
		},
		{
			name:   "snapshots-deleted",
			before: map[string]string{store.RollbackAbandonedProp: store.RollbackStepSnapshots}, after: map[string]string{},
		},
		{
			name:   "read-snapshots-deleted",
			before: map[string]string{store.RollbackAbandonedProp: store.RollbackStepReadSnapshots}, after: map[string]string{},
		},
		{
			name:   "step-deleted",
			before: map[string]string{store.RollbackAbandonedProp: "reap-replicas"}, after: map[string]string{},
			refused: true,
		},
		{
			name:   "step-kept-other-set",
			before: map[string]string{store.RollbackAbandonedProp: "reap-replicas"},
			after:  map[string]string{store.RollbackAbandonedProp: "reap-replicas", "Aux/x": "1"},
		},
		{
			name:   "step-kept-other-deleted",
			before: map[string]string{store.RollbackAbandonedProp: "reap-replicas", "Aux/x": "1"},
			after:  map[string]string{store.RollbackAbandonedProp: "reap-replicas"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := store.RollbackMarkClearRefusal(tc.before, tc.after)
			if tc.refused != errors.Is(err, store.ErrRollbackStepMarkKept) {
				t.Errorf("RollbackMarkClearRefusal = %v, want refused=%v", err, tc.refused)
			}
		})
	}
}

// secondPatchRefusingRDs lets the in-progress mark through and refuses the
// patch that would take it back off.
type secondPatchRefusingRDs struct {
	store.ResourceDefinitionStore

	patches *int
}

func (r secondPatchRefusingRDs) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	*r.patches++
	if *r.patches > 1 {
		return errPatchRefused
	}

	return r.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate) //nolint:wrapcheck // test double
}

type secondPatchRefusingStore struct {
	store.Store

	patches *int
}

func (s secondPatchRefusingStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return secondPatchRefusingRDs{ResourceDefinitionStore: s.Store.ResourceDefinitions(), patches: s.patches}
}

// A rollback that yields and cannot take its mark back off leaves a mark every
// retry is refused over, and says so rather than dropping the failure.
func TestHoldRollbackReportsAMarkItCouldNotTakeOff(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	if err := backend.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name: "pvc-stuck", Props: map[string]string{store.RestoreAdoptedProp: "2026-01-01T00:00:00Z"},
	}); err != nil {
		t.Fatalf("seed the adopted definition: %v", err)
	}

	patches := 0

	err := store.HoldRollback(t.Context(), secondPatchRefusingStore{Store: backend, patches: &patches}, "pvc-stuck", true)
	if !errors.Is(err, store.ErrRollbackYielded) {
		t.Fatalf("hold over an adopted definition = %v, want ErrRollbackYielded", err)
	}

	if !errors.Is(err, errPatchRefused) {
		t.Errorf("hold whose mark could not be taken off = %v, want the refused patch said too", err)
	}
}
