// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

var errGroupUnreadable = errors.New("probe: the group could not be read")

type unreadableGroups struct {
	store.ResourceGroupStore
}

func (unreadableGroups) Get(context.Context, string) (apiv1.ResourceGroup, error) {
	return apiv1.ResourceGroup{}, errGroupUnreadable
}

type unreadableGroupStore struct {
	store.Store
}

func (s unreadableGroupStore) ResourceGroups() store.ResourceGroupStore {
	return unreadableGroups{s.Store.ResourceGroups()}
}

func seedGroups(t *testing.T, st store.Store) {
	t.Helper()

	for _, rg := range []apiv1.ResourceGroup{
		{Name: "grp-luks", SelectFilter: apiv1.AutoSelectFilter{LayerStack: []string{"DRBD", "LUKS", "STORAGE"}}},
		{Name: "grp-plain"},
	} {
		if err := st.ResourceGroups().Create(t.Context(), &rg); err != nil {
			t.Fatalf("seed group %q: %v", rg.Name, err)
		}
	}
}

// The stack a definition's volumes are laid down under: its own; the default
// for an empty one with nothing above it that sets one; and undecided for an
// empty one under a group that sets one, since the controller decides that by
// whether data is already there.
func TestEffectiveLayerStack(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedGroups(t, st)

	for _, tc := range []struct {
		name  string
		rd    apiv1.ResourceDefinition
		want  []string
		undec bool
	}{
		{"own stack", apiv1.ResourceDefinition{Name: "a", ResourceGroupName: "grp-luks", LayerStack: []string{"DRBD", "STORAGE"}}, []string{"DRBD", "STORAGE"}, false},
		{"no group", apiv1.ResourceDefinition{Name: "b"}, apiv1.DefaultLayerStack(), false},
		{"group gone", apiv1.ResourceDefinition{Name: "c", ResourceGroupName: "grp-gone"}, apiv1.DefaultLayerStack(), false},
		{"group sets none", apiv1.ResourceDefinition{Name: "d", ResourceGroupName: "grp-plain"}, apiv1.DefaultLayerStack(), false},
		{"group sets one", apiv1.ResourceDefinition{Name: "e", ResourceGroupName: "grp-luks"}, nil, true},
	} {
		got, err := store.EffectiveLayerStack(t.Context(), st, &tc.rd)
		if tc.undec {
			if !errors.Is(err, store.ErrLayerStackUndecided) {
				t.Errorf("%s: err = %v, want ErrLayerStackUndecided", tc.name, err)
			}

			continue
		}

		if err != nil || !slices.Equal(got, tc.want) {
			t.Errorf("%s: = %v, %v; want %v", tc.name, got, err, tc.want)
		}
	}
}

// A group that cannot be read is an error, never the default and never
// undecided: the caller cannot tell, so it retries.
func TestEffectiveLayerStackDoesNotGuessOverAnUnreadableGroup(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedGroups(t, backend)

	rd := apiv1.ResourceDefinition{Name: "f", ResourceGroupName: "grp-plain"}

	_, err := store.EffectiveLayerStack(t.Context(), unreadableGroupStore{backend}, &rd)
	if !errors.Is(err, errGroupUnreadable) || errors.Is(err, store.ErrLayerStackUndecided) {
		t.Errorf("err = %v, want the read error", err)
	}
}

// A copy is flagged only when its group asks for layers it does not have.
func TestUnappliedGroupStack(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedGroups(t, st)

	for _, tc := range []struct {
		group string
		stack []string
		want  string
	}{
		{"grp-luks", []string{"DRBD", "STORAGE"}, "DRBD,LUKS,STORAGE"},
		{"grp-luks", []string{"storage", "luks", "drbd"}, ""},
		{"grp-plain", []string{"DRBD", "STORAGE"}, ""},
		{"", []string{"DRBD", "STORAGE"}, ""},
	} {
		got, err := store.UnappliedGroupStack(t.Context(), st, tc.group, tc.stack)
		if err != nil || got != tc.want {
			t.Errorf("UnappliedGroupStack(%q, %v) = %q, %v; want %q", tc.group, tc.stack, got, err, tc.want)
		}
	}
}

func seedGroupedPreparedTarget(t *testing.T, st store.Store, group string, stack []string) *apiv1.Snapshot {
	t.Helper()

	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-src", LayerStack: apiv1.DefaultLayerStack()}); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	snap := &apiv1.Snapshot{
		Name: "snap-1", ResourceName: "pvc-src",
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1024}},
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-dst", ResourceGroupName: group, LayerStack: stack,
	}); err != nil {
		t.Fatalf("seed target: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-dst", &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024}); err != nil {
		t.Fatalf("seed target volume: %v", err)
	}

	return snap
}

// A prepared target whose stack its group has yet to decide is not judged
// prepared and nothing is written to it; a decided one of the source's stack is.
func TestPreparedRestoreTargetWaitsForAnUndecidedStack(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedGroups(t, st)
	snap := seedGroupedPreparedTarget(t, st, "grp-luks", nil)

	target, _ := st.ResourceDefinitions().Get(t.Context(), "pvc-dst")

	if _, err := store.PreparedRestoreTarget(t.Context(), st, &target, snap); !errors.Is(err, store.ErrLayerStackUndecided) {
		t.Errorf("an undecided target = %v, want ErrLayerStackUndecided", err)
	}

	if _, err := store.AdoptPreparedRestoreTarget(t.Context(), st, "pvc-dst", snap); !errors.Is(err, store.ErrLayerStackUndecided) {
		t.Errorf("marking an undecided target = %v, want ErrLayerStackUndecided", err)
	}

	target, _ = st.ResourceDefinitions().Get(t.Context(), "pvc-dst")
	if target.Props[store.RestoreFromSnapshotProp] != "" || len(target.LayerStack) > 0 {
		t.Errorf("the refused mark wrote %v / %v", target.Props, target.LayerStack)
	}
}

// The target is marked with the stack its group resolves to, read before the
// patch; a target moved to another group in between is answered as undecided
// rather than stamped with the old group's answer.
func TestRestoreMarkerResolvesTheStackItIsWrittenWith(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedGroups(t, st)
	snap := seedGroupedPreparedTarget(t, st, "grp-plain", nil)

	moved := claimPatchHookStore{Store: st, hook: claimPatchHook{before: func() {
		rd, _ := st.ResourceDefinitions().Get(context.Background(), "pvc-dst")
		rd.ResourceGroupName = "grp-luks"
		_ = st.ResourceDefinitions().Update(context.Background(), &rd)
	}}}

	if _, err := store.AdoptPreparedRestoreTarget(t.Context(), moved, "pvc-dst", snap); !errors.Is(err, store.ErrLayerStackUndecided) {
		t.Errorf("a target moved under the patch = %v, want ErrLayerStackUndecided", err)
	}

	rd, _ := st.ResourceDefinitions().Get(t.Context(), "pvc-dst")
	rd.ResourceGroupName = "grp-plain"
	_ = st.ResourceDefinitions().Update(t.Context(), &rd)

	if _, err := store.AdoptPreparedRestoreTarget(t.Context(), st, "pvc-dst", snap); err != nil {
		t.Fatalf("marking a target under a group that sets no stack: %v", err)
	}

	rd, _ = st.ResourceDefinitions().Get(t.Context(), "pvc-dst")
	if !slices.Equal(rd.LayerStack, apiv1.DefaultLayerStack()) {
		t.Errorf("marked target stack = %v, want %v", rd.LayerStack, apiv1.DefaultLayerStack())
	}
}
