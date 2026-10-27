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

package store

import (
	"context"
	"errors"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

func seedRestoreSource(t *testing.T, st Store, srcRD, snapName string) {
	t.Helper()

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: srcRD}); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	if err := st.Snapshots().Create(t.Context(), &apiv1.Snapshot{Name: snapName, ResourceName: srcRD}); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}
}

func seedRestoredFrom(t *testing.T, st Store, rdName, marker string, flags ...string) {
	t.Helper()

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name: rdName, Flags: flags, Props: map[string]string{RestoreFromSnapshotProp: marker},
	}); err != nil {
		t.Fatalf("seed restored definition: %v", err)
	}
}

// A delete of a snapshot that is not there holds nothing: a definition whose
// marker still names it, with a restore that can never finish, used to turn
// the idempotent answer into a refusal every retry met.
func TestDeleteSnapshotOfAMissingSnapshotRefusesNothing(t *testing.T) {
	t.Parallel()

	st := NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "src-gone"}); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	seedRestoredFrom(t, st, "dst-gone", "src-gone:snap-gone")

	err := DeleteSnapshot(t.Context(), st, "src-gone", "snap-gone")
	if !errors.Is(err, ErrNotFound) || errors.Is(err, ErrRestoreSourceInUse) {
		t.Fatalf("delete of a missing snapshot = %v, want ErrNotFound alone", err)
	}
}

// A definition being deleted is not waited for.
func TestDeleteSnapshotSkipsADefinitionBeingDeleted(t *testing.T) {
	t.Parallel()

	st := NewInMemory()
	seedRestoreSource(t, st, "src-del", "snap-del")
	seedRestoredFrom(t, st, "dst-del", "src-del:snap-del", apiv1.ResourceFlagDelete)

	if err := DeleteSnapshot(t.Context(), st, "src-del", "snap-del"); err != nil {
		t.Fatalf("delete under a definition being deleted = %v, want nil", err)
	}

	if _, err := st.Snapshots().Get(t.Context(), "src-del", "snap-del"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the snapshot is still there: %v", err)
	}
}

// A delete the restore refuses takes its mark back off, so restores from the
// kept snapshot are not refused for the mark's lifetime.
func TestDeleteSnapshotTakesItsMarkOffASnapshotItKeeps(t *testing.T) {
	t.Parallel()

	st := NewInMemory()
	seedRestoreSource(t, st, "src-keep", "snap-keep")
	seedRestoredFrom(t, st, "dst-keep", "src-keep:snap-keep")

	err := DeleteSnapshot(t.Context(), st, "src-keep", "snap-keep")
	if !errors.Is(err, ErrRestoreSourceInUse) {
		t.Fatalf("delete under an unfinished restore = %v, want ErrRestoreSourceInUse", err)
	}

	snap, err := st.Snapshots().Get(t.Context(), "src-keep", "snap-keep")
	if err != nil {
		t.Fatalf("the refused delete took the snapshot: %v", err)
	}

	if mark, ok := snap.Props[SnapshotDeletingProp]; ok {
		t.Errorf("the kept snapshot still carries the deleting mark %q", mark)
	}

	if err := RestoreSourceWithdrawn(t.Context(), st, "src-keep", "snap-keep"); err != nil {
		t.Errorf("a restore from the kept snapshot is withdrawn: %v", err)
	}
}

// raceOnList runs before every uncached definition list: what a restore does
// between the delete's mark and its list.
type raceOnList struct {
	Store

	before func(ctx context.Context)
}

type raceOnListRDs struct {
	ResourceDefinitionStore

	before func(ctx context.Context)
}

func (r raceOnList) ResourceDefinitions() ResourceDefinitionStore {
	return raceOnListRDs{ResourceDefinitionStore: r.Store.ResourceDefinitions(), before: r.before}
}

func (r raceOnListRDs) ListUncached(ctx context.Context) ([]apiv1.ResourceDefinition, error) {
	r.before(ctx)

	return r.ResourceDefinitionStore.ListUncached(ctx) //nolint:wrapcheck // pass-through test double
}

// A restore whose definition lands after the delete's mark and before its
// list is seen by both halves: the restore reads the mark back and withdraws,
// and the delete lists the definition and keeps the snapshot. Neither ends
// with a definition naming a snapshot that is gone.
func TestDeleteSnapshotAndARacingRestoreSeeEachOther(t *testing.T) {
	t.Parallel()

	backend := NewInMemory()
	seedRestoreSource(t, backend, "src-race", "snap-race")

	var withdrawn error

	st := raceOnList{Store: backend, before: func(ctx context.Context) {
		seedRestoredFrom(t, backend, "dst-race", "src-race:snap-race") //nolint:contextcheck // the helper seeds on t's own context
		withdrawn = RestoreSourceWithdrawn(ctx, backend, "src-race", "snap-race")
	}}

	err := DeleteSnapshot(t.Context(), st, "src-race", "snap-race")
	if !errors.Is(err, ErrRestoreSourceInUse) {
		t.Errorf("the delete did not see the restore that raced it: %v", err)
	}

	if !errors.Is(withdrawn, ErrRestoreSourceWithdrawn) || !errors.Is(withdrawn, ErrRestoreSourceDeleting) {
		t.Errorf("the restore did not see the delete that raced it: %v", withdrawn)
	}
}

func TestRestoreSourceWithdrawn(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	for _, tc := range []struct {
		name     string
		props    map[string]string
		missing  bool
		wantErrs []error
	}{
		{name: "plain"},
		{name: "gone", missing: true, wantErrs: []error{ErrRestoreSourceWithdrawn, ErrNotFound}},
		{
			name:     "live-mark",
			props:    map[string]string{SnapshotDeletingProp: "tok@" + now.Format(time.RFC3339)},
			wantErrs: []error{ErrRestoreSourceWithdrawn, ErrRestoreSourceDeleting},
		},
		{
			name:  "expired-mark",
			props: map[string]string{SnapshotDeletingProp: "tok@" + now.Add(-time.Hour).Format(time.RFC3339)},
		},
		{name: "unreadable-mark", props: map[string]string{SnapshotDeletingProp: "tok"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := NewInMemory()
			if !tc.missing {
				if err := st.Snapshots().Create(t.Context(), &apiv1.Snapshot{
					Name: "snap", ResourceName: "src", Props: tc.props,
				}); err != nil {
					t.Fatalf("seed: %v", err)
				}
			}

			err := RestoreSourceWithdrawn(t.Context(), st, "src", "snap")
			if len(tc.wantErrs) == 0 && err != nil {
				t.Fatalf("withdrawn = %v, want nil", err)
			}

			for _, want := range tc.wantErrs {
				if !errors.Is(err, want) {
					t.Errorf("withdrawn = %v, want it to wrap %v", err, want)
				}
			}
		})
	}
}

// The mark describes the snapshot it is on: a definition restored from the
// snapshot while it was marked does not carry it.
func TestTravellingPropsLeaveTheDeletingMarkBehind(t *testing.T) {
	t.Parallel()

	out := TravellingProps(map[string]string{SnapshotDeletingProp: "tok@now", "Aux/keep": "v"})
	if _, ok := out[SnapshotDeletingProp]; ok || out["Aux/keep"] != "v" {
		t.Fatalf("TravellingProps = %v", out)
	}
}

// cacheBlindToDefinitions answers the cached definition list from a cache
// that has not seen any definition yet.
type cacheBlindToDefinitions struct {
	Store
}

type cacheBlindRDs struct {
	ResourceDefinitionStore
}

func (c cacheBlindToDefinitions) ResourceDefinitions() ResourceDefinitionStore {
	return cacheBlindRDs{ResourceDefinitionStore: c.Store.ResourceDefinitions()}
}

func (cacheBlindRDs) List(context.Context) ([]apiv1.ResourceDefinition, error) {
	return nil, nil
}

// The dependent most likely to be missing from a cache is the one created a
// moment ago, so the delete lists past it.
func TestDeleteSnapshotListsDependentsPastTheCache(t *testing.T) {
	t.Parallel()

	backend := NewInMemory()
	seedRestoreSource(t, backend, "src-cache", "snap-cache")
	seedRestoredFrom(t, backend, "dst-cache", "src-cache:snap-cache")

	err := DeleteSnapshot(t.Context(), cacheBlindToDefinitions{Store: backend}, "src-cache", "snap-cache")
	if !errors.Is(err, ErrRestoreSourceInUse) {
		t.Fatalf("delete with a cache blind to the dependent = %v, want ErrRestoreSourceInUse", err)
	}
}
