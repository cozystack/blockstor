package cli

import (
	"errors"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// The CLI deletes snapshots through the store directly, so it runs the same
// release the REST door does: a snapshot an unfinished restore reads from
// stays, with the reason.
func TestSnapshotDeleteRefusesWhileARestoreReadsFromIt(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	st := store.NewInMemory()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "src-cli"}); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{Name: "snap-cli", ResourceName: "src-cli"}); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "dst-cli",
		Props: map[string]string{store.RestoreFromSnapshotProp: "src-cli:snap-cli"},
	}); err != nil {
		t.Fatalf("seed restored definition: %v", err)
	}

	run := &runContext{
		Store: st,
		Flags: &flagSet{Values: map[string]string{}, Positionals: []string{"src-cli", "snap-cli"}},
	}

	err := snapshotDelete(ctx, run)
	if !errors.Is(err, store.ErrRestoreSourceInUse) {
		t.Fatalf("snapshot delete under an unfinished restore = %v, want ErrRestoreSourceInUse", err)
	}

	if _, err := st.Snapshots().Get(ctx, "src-cli", "snap-cli"); err != nil {
		t.Fatalf("the refused delete took the snapshot: %v", err)
	}
}
