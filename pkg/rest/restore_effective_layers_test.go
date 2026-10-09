// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"net/http"
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

func seedLayeredGroup(t *testing.T, st store.Store, name string, stack []string) {
	t.Helper()

	if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{
		Name: name, SelectFilter: apiv1.AutoSelectFilter{LayerStack: stack},
	}); err != nil {
		t.Fatalf("seed group %q: %v", name, err)
	}
}

func assertDataPlaneStack(t *testing.T, st store.Store, rdName string) {
	t.Helper()

	rd, err := st.ResourceDefinitions().Get(t.Context(), rdName)
	if err != nil {
		t.Fatalf("read %q: %v", rdName, err)
	}

	if !slices.Equal(rd.LayerStack, apiv1.DefaultLayerStack()) {
		t.Errorf("%q stack = %v, want the source's %v stamped: left empty, the control plane "+
			"would judge it by its group's stack while the satellite brings it up as the source's",
			rdName, rd.LayerStack, apiv1.DefaultLayerStack())
	}
}

// A clone named into a group whose stack differs keeps the stack the source's
// bytes were written under, stamped, so the group's stack cannot reach it.
func TestRDCloneIntoAGroupOfAnotherStackKeepsTheSourcesStack(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-grp")
	seedLayeredGroup(t, st, "grp-luks", []string{"DRBD", "LUKS", "STORAGE"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-grp", "dst-grp", map[string]any{"resource_group": "grp-luks"}); code != http.StatusCreated {
		t.Fatalf("clone into a group of another stack = %d, want 201", code)
	}

	assertDataPlaneStack(t, st, "dst-grp")
}

// A volume-less source is cloned as a shell, and the shell is stamped with the
// source's data-plane stack the same way the data path stamps its target: left
// empty under a group of another stack, volumes added to it later would be
// brought up as the default while the control plane judged them by the group.
func TestRDCloneShellIntoAGroupOfAnotherStackKeepsTheSourcesStack(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedLayeredGroup(t, st, "grp-shell-luks", []string{"DRBD", "LUKS", "STORAGE"})

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "src-shell"}); err != nil {
		t.Fatalf("seed the volume-less source: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-shell", "dst-shell",
		map[string]any{"resource_group": "grp-shell-luks"}); code != http.StatusCreated {
		t.Fatalf("shell clone into a group of another stack = %d, want 201", code)
	}

	assertDataPlaneStack(t, st, "dst-shell")
}

// The shell clone stores the stack the caller named in canonical case too.
func TestRDCloneShellStoresALowercaseLayerListCanonically(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "src-case"}); err != nil {
		t.Fatalf("seed the volume-less source: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-case", "dst-case",
		map[string]any{"layer_list": []string{"drbd", "storage"}}); code != http.StatusCreated {
		t.Fatalf("shell clone with a lowercase layer_list = %d, want 201", code)
	}

	dst, err := st.ResourceDefinitions().Get(t.Context(), "dst-case")
	if err != nil {
		t.Fatalf("read the clone: %v", err)
	}

	if !slices.Equal(dst.LayerStack, []string{"DRBD", "STORAGE"}) {
		t.Errorf("layer stack = %v, want [DRBD STORAGE]", dst.LayerStack)
	}
}

// A restore inherits the source's group, whose stack may since differ from
// the one the source was created with.
func TestSnapshotRestoreStampsTheSourcesDataPlaneStack(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-rst")
	seedLayeredGroup(t, st, "grp-changed", []string{"DRBD", "LUKS", "STORAGE"})

	src, err := st.ResourceDefinitions().Get(ctx, "src-rst")
	if err != nil {
		t.Fatalf("read the source: %v", err)
	}

	src.ResourceGroupName = "grp-changed"
	if err := st.ResourceDefinitions().Update(ctx, &src); err != nil {
		t.Fatalf("move the source under the changed group: %v", err)
	}

	seedRestoreSnapshot(t, st, "src-rst", "snap-rst", []string{"node-a"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := restoreOnce(t, base, "src-rst", "snap-rst", map[string]any{"to_resource": "dst-rst"}); code != http.StatusCreated {
		t.Fatalf("restore = %d, want 201", code)
	}

	assertDataPlaneStack(t, st, "dst-rst")
}

// A prepared target stored without a stack is brought up with the default,
// whatever its group says, and is judged and stamped by that.
func TestSnapshotRestoreIntoAPreparedTargetStampsItsDataPlaneStack(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-prep")
	seedRestoreSnapshot(t, st, "src-prep", "snap-prep", []string{"node-a"})
	seedLayeredGroup(t, st, "grp-sc", []string{"DRBD", "LUKS", "STORAGE"})

	snap, err := st.Snapshots().Get(ctx, "src-prep", "snap-prep")
	if err != nil {
		t.Fatalf("read the snapshot: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-prep", ResourceGroupName: "grp-sc",
	}); err != nil {
		t.Fatalf("seed the prepared target: %v", err)
	}

	for _, svd := range snap.VolumeDefinitions {
		if err := st.VolumeDefinitions().Create(ctx, "pvc-prep",
			&apiv1.VolumeDefinition{VolumeNumber: svd.VolumeNumber, SizeKib: svd.SizeKib}); err != nil {
			t.Fatalf("seed the prepared volume: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := restoreOnce(t, base, "src-prep", "snap-prep", map[string]any{
		"to_resource": "pvc-prep", "nodes": []string{"node-a"},
	}); code != http.StatusCreated {
		t.Fatalf("restore into the prepared target = %d, want 201", code)
	}

	assertDataPlaneStack(t, st, "pvc-prep")
}
