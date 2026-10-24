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

func luksStack() []string { return []string{"DRBD", "LUKS", "STORAGE"} }

func readRD(t *testing.T, st store.Store, name string) apiv1.ResourceDefinition {
	t.Helper()

	rd, err := st.ResourceDefinitions().Get(t.Context(), name)
	if err != nil {
		t.Fatalf("read %q: %v", name, err)
	}

	return rd
}

// reshapeRD sets a definition's group, stack and annotations the way the
// controller pin or an operator edit would.
func reshapeRD(t *testing.T, st store.Store, name, group string, stack []string, annotations map[string]string) {
	t.Helper()

	// Patched rather than updated: Update keeps a decided stack, and an
	// empty one here stands for a definition an older controller stored
	// without one.
	err := st.ResourceDefinitions().PatchResourceDefinitionSpec(t.Context(), name, func(rd *apiv1.ResourceDefinition) error {
		rd.ResourceGroupName = group
		rd.LayerStack = stack
		rd.Annotations = annotations

		return nil
	})
	if err != nil {
		t.Fatalf("reshape %q: %v", name, err)
	}
}

func assertRDAbsent(t *testing.T, st store.Store, name string) {
	t.Helper()

	if _, err := st.ResourceDefinitions().Get(t.Context(), name); err == nil {
		t.Errorf("%q exists after a refusal that should have written nothing", name)
	}
}

func assertStack(t *testing.T, st store.Store, name string, want []string) {
	t.Helper()

	if got := readRD(t, st, name).LayerStack; !slices.Equal(got, want) {
		t.Errorf("%q stack = %v, want %v", name, got, want)
	}
}

func assertUnapplied(t *testing.T, st store.Store, name, want string) {
	t.Helper()

	if got := readRD(t, st, name).Annotations[apiv1.UnappliedGroupLayerStackAnnotation]; got != want {
		t.Errorf("%q unapplied-group annotation = %q, want %q", name, got, want)
	}
}

// A clone named into a group whose stack differs keeps the stack the source's
// bytes were written under, stamped, so the group's stack cannot reach it, and
// it is flagged as short of what its group asks for.
func TestRDCloneIntoAGroupOfAnotherStackKeepsTheSourcesStack(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-grp")
	seedLayeredGroup(t, st, "grp-luks", luksStack())

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-grp", "dst-grp", map[string]any{"resource_group": "grp-luks"}); code != http.StatusCreated {
		t.Fatalf("clone into a group of another stack = %d, want 201", code)
	}

	assertStack(t, st, "dst-grp", apiv1.DefaultLayerStack())
	assertUnapplied(t, st, "dst-grp", "DRBD,LUKS,STORAGE")
}

// A clone of a source whose stack its group has yet to decide waits for the
// decision: 503, nothing written. Once the controller pins the source (here
// over data, so to the default stack), the same request clones it with that
// stack.
func TestRDCloneOfAnUndecidedSourceWaitsForTheStack(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-und")
	seedLayeredGroup(t, st, "grp-und", luksStack())
	reshapeRD(t, st, "src-und", "grp-und", nil, nil)

	base, stop := startServerWithStore(t, st)
	defer stop()

	for _, extra := range []map[string]any{nil, {"layer_list": []string{"DRBD", "STORAGE"}}} {
		if code := cloneOnce(t, base, "src-und", "dst-und", extra); code != http.StatusServiceUnavailable {
			t.Errorf("clone of an undecided source (%v) = %d, want 503", extra, code)
		}
	}

	assertRDAbsent(t, st, "dst-und")

	if _, err := st.Snapshots().Get(ctx, "src-und", cloneSnapshotName("dst-und")); err == nil {
		t.Error("the refused clone took its internal snapshot of the source")
	}

	reshapeRD(t, st, "src-und", "grp-und", apiv1.DefaultLayerStack(),
		map[string]string{apiv1.UnappliedGroupLayerStackAnnotation: "DRBD,LUKS,STORAGE"})

	if code := cloneOnce(t, base, "src-und", "dst-und", nil); code != http.StatusCreated {
		t.Fatalf("clone once the source is pinned = %d, want 201", code)
	}

	assertStack(t, st, "dst-und", apiv1.DefaultLayerStack())
	assertUnapplied(t, st, "dst-und", "DRBD,LUKS,STORAGE")
}

// A finished clone is answered before the source is read, so a source that
// became undecided since (moved under a group with a stack) does not fail its
// replay.
func TestRDCloneReplayDoesNotReadTheSourcesStack(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-rep")
	seedLayeredGroup(t, st, "grp-rep", luksStack())

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-rep", "dst-rep", nil); code != http.StatusCreated {
		t.Fatalf("first clone = %d, want 201", code)
	}

	reshapeRD(t, st, "src-rep", "grp-rep", nil, nil)

	if code := cloneOnce(t, base, "src-rep", "dst-rep", nil); code != http.StatusCreated {
		t.Errorf("replay over a source that became undecided = %d, want 201", code)
	}

	if code := cloneOnce(t, base, "src-rep", "dst-rep",
		map[string]any{"layer_list": []string{"DRBD", "STORAGE"}}); code != http.StatusCreated {
		t.Errorf("replay naming the stack the clone has = %d, want 201", code)
	}
}

// A finished clone left with an empty stack, as an older controller wrote
// them, is compared against a named stack once its group decides it: 503
// until the controller pins it, the usual answer after.
func TestRDCloneReplayOfAnUndecidedLeftoverWaitsForTheStack(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-old")
	seedLayeredGroup(t, st, "grp-old", luksStack())

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-old", "dst-old", nil); code != http.StatusCreated {
		t.Fatalf("first clone = %d, want 201", code)
	}

	dst := readRD(t, st, "dst-old")
	reshapeRD(t, st, "dst-old", "grp-old", nil, dst.Annotations)

	named := map[string]any{"layer_list": []string{"DRBD", "STORAGE"}}
	if code := cloneOnce(t, base, "src-old", "dst-old", named); code != http.StatusServiceUnavailable {
		t.Errorf("replay over an undecided leftover = %d, want 503", code)
	}

	reshapeRD(t, st, "dst-old", "grp-old", apiv1.DefaultLayerStack(), dst.Annotations)

	if code := cloneOnce(t, base, "src-old", "dst-old", named); code != http.StatusCreated {
		t.Errorf("replay once the leftover is pinned = %d, want 201", code)
	}
}

// A volume-less source that never said what stack it has passes that on: the
// shell holds no bytes, so its own group decides through the controller pin,
// as for any definition created empty.
func TestRDCloneShellOfASourceThatNeverSaidLeavesTheStackToItsGroup(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedLayeredGroup(t, st, "grp-shell-luks", luksStack())

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name: "src-shell", ResourceGroupName: "grp-shell-luks",
	}); err != nil {
		t.Fatalf("seed the volume-less source: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-shell", "dst-shell", nil); code != http.StatusCreated {
		t.Fatalf("shell clone = %d, want 201", code)
	}

	assertStack(t, st, "dst-shell", nil)
	assertUnapplied(t, st, "dst-shell", "")
}

// A stack the controller pinned over data that was already there is the data's,
// not a choice: a shell, which holds none, does not take it or its flag.
func TestRDCloneShellDoesNotInheritAStackPinnedOverData(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedLayeredGroup(t, st, "grp-pinned", luksStack())

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name: "src-pinned", ResourceGroupName: "grp-pinned", LayerStack: apiv1.DefaultLayerStack(),
		Annotations: map[string]string{apiv1.UnappliedGroupLayerStackAnnotation: "DRBD,LUKS,STORAGE"},
	}); err != nil {
		t.Fatalf("seed the volume-less source: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-pinned", "dst-pinned", nil); code != http.StatusCreated {
		t.Fatalf("shell clone = %d, want 201", code)
	}

	assertStack(t, st, "dst-pinned", nil)
	assertUnapplied(t, st, "dst-pinned", "")
	assertUnapplied(t, st, "src-pinned", "DRBD,LUKS,STORAGE")
}

// A stack the source chose is copied onto its shell.
func TestRDCloneShellKeepsAStackTheSourceChose(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedLayeredGroup(t, st, "grp-chose", luksStack())

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name: "src-chose", ResourceGroupName: "grp-chose", LayerStack: apiv1.DefaultLayerStack(),
	}); err != nil {
		t.Fatalf("seed the volume-less source: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-chose", "dst-chose", nil); code != http.StatusCreated {
		t.Fatalf("shell clone = %d, want 201", code)
	}

	assertStack(t, st, "dst-chose", apiv1.DefaultLayerStack())
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

	assertStack(t, st, "dst-case", []string{"DRBD", "STORAGE"})
}

// A restore of a source whose stack its group has yet to decide waits for the
// decision: 503, nothing written. Once the controller pins the source over its
// data, the restore stamps that stack on the new definition and flags it as
// short of its group's.
func TestSnapshotRestoreWaitsForTheSourcesStackToBeDecided(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-rst")
	seedLayeredGroup(t, st, "grp-changed", luksStack())
	reshapeRD(t, st, "src-rst", "grp-changed", nil, nil)
	seedRestoreSnapshot(t, st, "src-rst", "snap-rst", []string{"node-a"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := restoreOnce(t, base, "src-rst", "snap-rst", map[string]any{"to_resource": "dst-rst"}); code != http.StatusServiceUnavailable {
		t.Fatalf("restore of an undecided source = %d, want 503", code)
	}

	assertRDAbsent(t, st, "dst-rst")

	reshapeRD(t, st, "src-rst", "grp-changed", apiv1.DefaultLayerStack(), nil)

	if code := restoreOnce(t, base, "src-rst", "snap-rst", map[string]any{"to_resource": "dst-rst"}); code != http.StatusCreated {
		t.Fatalf("restore once the source is pinned = %d, want 201", code)
	}

	assertStack(t, st, "dst-rst", apiv1.DefaultLayerStack())
	assertUnapplied(t, st, "dst-rst", "DRBD,LUKS,STORAGE")
}

// A group that cannot be read leaves the source's stack unknown, and the
// restore refuses with a retryable answer before it writes anything.
func TestSnapshotRestoreRefusesWhenTheSourcesGroupCannotBeRead(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedDeployedCloneSource(t, backend, "src-unread")
	seedLayeredGroup(t, backend, "grp-unread", luksStack())
	reshapeRD(t, backend, "src-unread", "grp-unread", nil, nil)
	seedRestoreSnapshot(t, backend, "src-unread", "snap-unread", []string{"node-a"})

	base, stop := startServerWithStore(t, failingRGReadStore{backend})
	defer stop()

	code := restoreOnce(t, base, "src-unread", "snap-unread", map[string]any{"to_resource": "dst-unread"})
	if code != http.StatusInternalServerError {
		t.Errorf("restore over an unreadable group = %d, want 500", code)
	}

	assertRDAbsent(t, backend, "dst-unread")
}

func seedPreparedTarget(t *testing.T, st store.Store, srcName, snapName, rdName, group string, stack []string) {
	t.Helper()

	ctx := t.Context()

	snap, err := st.Snapshots().Get(ctx, srcName, snapName)
	if err != nil {
		t.Fatalf("read the snapshot: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: rdName, ResourceGroupName: group, LayerStack: stack,
	}); err != nil {
		t.Fatalf("seed the prepared target: %v", err)
	}

	for _, svd := range snap.VolumeDefinitions {
		if err := st.VolumeDefinitions().Create(ctx, rdName,
			&apiv1.VolumeDefinition{VolumeNumber: svd.VolumeNumber, SizeKib: svd.SizeKib}); err != nil {
			t.Fatalf("seed the prepared volume: %v", err)
		}
	}
}

// A prepared target stored without a stack under a group that sets one waits
// for the controller to decide it: 503, and no marker goes on. Pinned to the
// group's LUKS stack, as a target with nothing on it is, it is refused against
// a plaintext source; one the operator gave the source's stack is restored
// into and keeps it.
func TestSnapshotRestoreIntoAnUndecidedPreparedTargetWaits(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-prep")
	seedRestoreSnapshot(t, st, "src-prep", "snap-prep", []string{"node-a"})
	seedLayeredGroup(t, st, "grp-sc", luksStack())
	seedPreparedTarget(t, st, "src-prep", "snap-prep", "pvc-prep", "grp-sc", nil)

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := map[string]any{"to_resource": "pvc-prep", "nodes": []string{"node-a"}}

	if code := restoreOnce(t, base, "src-prep", "snap-prep", body); code != http.StatusServiceUnavailable {
		t.Fatalf("restore into an undecided prepared target = %d, want 503", code)
	}

	if marker := readRD(t, st, "pvc-prep").Props[store.RestoreFromSnapshotProp]; marker != "" {
		t.Errorf("the refused restore left marker %q on the target", marker)
	}

	assertStack(t, st, "pvc-prep", nil)

	reshapeRD(t, st, "pvc-prep", "grp-sc", luksStack(), nil)

	if code := restoreOnce(t, base, "src-prep", "snap-prep", body); code != http.StatusConflict {
		t.Errorf("restore of a plaintext source into a LUKS target = %d, want 409", code)
	}

	reshapeRD(t, st, "pvc-prep", "grp-sc", apiv1.DefaultLayerStack(), nil)

	if code := restoreOnce(t, base, "src-prep", "snap-prep", body); code != http.StatusCreated {
		t.Fatalf("restore into a target of the source's stack = %d, want 201", code)
	}

	assertStack(t, st, "pvc-prep", apiv1.DefaultLayerStack())
}
