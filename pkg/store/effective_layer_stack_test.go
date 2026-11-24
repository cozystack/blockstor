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
		{Name: "grp-drbd", SelectFilter: apiv1.AutoSelectFilter{LayerStack: []string{"DRBD", "STORAGE"}}},
		{Name: "grp-local", SelectFilter: apiv1.AutoSelectFilter{LayerStack: []string{"STORAGE"}}},
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
		// More layers than the group asks for is not short of it.
		{"grp-drbd", []string{"DRBD", "LUKS", "STORAGE"}, ""},
		{"", []string{"DRBD", "STORAGE"}, ""},
	} {
		got, err := store.UnappliedGroupStack(t.Context(), st, tc.group, tc.stack)
		if err != nil || got != tc.want {
			t.Errorf("UnappliedGroupStack(%q, %v) = %q, %v; want %q", tc.group, tc.stack, got, err, tc.want)
		}
	}
}

// A prepared target is restored under the source's stack, whatever stack it
// was created with, the way LINSTOR builds a restored resource from the
// snapshot's layer data: Velero's data mover restores a DRBD volume's snapshot
// into a storage-only class, a LUKS one into a plaintext class, and the other
// way round. Nothing is on the target yet, so no layer it named has written
// anything, and the copy is read back through the layers it was written under.
func TestRestoreIntoAPreparedTargetTakesTheSourceStack(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		group  string
		target []string
		source []string
	}{
		{"storage-only target of a DRBD source", "grp-local", []string{"STORAGE"}, []string{"DRBD", "STORAGE"}},
		{"plaintext target of a LUKS source", "grp-drbd", []string{"DRBD", "STORAGE"}, []string{"DRBD", "LUKS", "STORAGE"}},
		{"LUKS target of a plaintext source", "grp-luks", []string{"DRBD", "LUKS", "STORAGE"}, []string{"DRBD", "STORAGE"}},
		{"DRBD target of a storage-only source", "grp-drbd", []string{"DRBD", "STORAGE"}, []string{"STORAGE"}},
		{"undecided target", "grp-luks", nil, []string{"DRBD", "STORAGE"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			seedGroups(t, st)
			snap := seedPreparedTargetOf(t, st, tc.source, tc.group, tc.target)

			target, _ := st.ResourceDefinitions().Get(t.Context(), "pvc-dst")

			prepared, err := store.PreparedRestoreTarget(t.Context(), st, &target, snap)
			if err != nil || !prepared {
				t.Fatalf("PreparedRestoreTarget = %v, %v; want a prepared target", prepared, err)
			}

			adopted, err := store.AdoptPreparedRestoreTarget(t.Context(), st, "pvc-dst", snap)
			if err != nil || !adopted {
				t.Fatalf("AdoptPreparedRestoreTarget = %v, %v; want adopted", adopted, err)
			}

			got, _ := st.ResourceDefinitions().Get(t.Context(), "pvc-dst")
			if !slices.Equal(got.LayerStack, tc.source) {
				t.Errorf("restored target stack = %v, want the source's %v", got.LayerStack, tc.source)
			}

			if got.Props[store.RestoreFromSnapshotProp] == "" {
				t.Errorf("the adopted target carries no restore marker: %v", got.Props)
			}
		})
	}
}

// A source whose stack its group has yet to decide gives the restore no stack
// to inherit: the target is not taken and nothing is written to it until the
// controller decides it.
func TestRestoreWaitsForAnUndecidedSourceStack(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedGroups(t, st)
	snap := seedPreparedTargetOf(t, st, nil, "grp-local", []string{"STORAGE"})

	src, _ := st.ResourceDefinitions().Get(t.Context(), "pvc-src")
	src.ResourceGroupName = "grp-luks"
	_ = st.ResourceDefinitions().Update(t.Context(), &src)

	target, _ := st.ResourceDefinitions().Get(t.Context(), "pvc-dst")

	if _, err := store.PreparedRestoreTarget(t.Context(), st, &target, snap); !errors.Is(err, store.ErrLayerStackUndecided) {
		t.Errorf("a target of an undecided source = %v, want ErrLayerStackUndecided", err)
	}

	if _, err := store.AdoptPreparedRestoreTarget(t.Context(), st, "pvc-dst", snap); !errors.Is(err, store.ErrLayerStackUndecided) {
		t.Errorf("marking a target of an undecided source = %v, want ErrLayerStackUndecided", err)
	}

	got, _ := st.ResourceDefinitions().Get(t.Context(), "pvc-dst")
	if got.Props[store.RestoreFromSnapshotProp] != "" || !slices.Equal(got.LayerStack, []string{"STORAGE"}) {
		t.Errorf("the refused mark wrote %v / %v", got.Props, got.LayerStack)
	}
}

// The target's stack can be rewritten between the judgement and the stamp;
// the stamp writes the source's stack over whatever it finds, in the same
// patch as the marker, so the marker never lands over another stack.
func TestRestoreMarkerWritesTheSourceStackOverARewrite(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedGroups(t, st)
	snap := seedPreparedTargetOf(t, st, apiv1.DefaultLayerStack(), "grp-plain", apiv1.DefaultLayerStack())

	rewritten := claimPatchHookStore{Store: st, hook: claimPatchHook{before: func() {
		rd, _ := st.ResourceDefinitions().Get(context.Background(), "pvc-dst")
		rd.LayerStack = []string{"DRBD", "LUKS", "STORAGE"}
		rd.ResourceGroupName = "grp-luks"
		_ = st.ResourceDefinitions().Update(context.Background(), &rd)
	}}}

	if _, err := store.AdoptPreparedRestoreTarget(t.Context(), rewritten, "pvc-dst", snap); err != nil {
		t.Fatalf("marking a target rewritten under the patch: %v", err)
	}

	rd, _ := st.ResourceDefinitions().Get(t.Context(), "pvc-dst")
	if !slices.Equal(rd.LayerStack, apiv1.DefaultLayerStack()) || rd.Props[store.RestoreFromSnapshotProp] == "" {
		t.Errorf("marked target = %v / %v, want the source's stack %v under the marker",
			rd.LayerStack, rd.Props, apiv1.DefaultLayerStack())
	}
}

func seedPreparedTargetOf(t *testing.T, st store.Store, source []string, group string, stack []string) *apiv1.Snapshot {
	t.Helper()

	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-src", LayerStack: source}); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-dst", ResourceGroupName: group, LayerStack: stack,
	}); err != nil {
		t.Fatalf("seed target: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-dst", &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024}); err != nil {
		t.Fatalf("seed target volume: %v", err)
	}

	return &apiv1.Snapshot{
		Name: "snap-1", ResourceName: "pvc-src",
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1024}},
	}
}
