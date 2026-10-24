package rest

import (
	"errors"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Replicas under a name with no definition over them are not this request's
// work. Created over them, the restore adopted them, and its rollback after a
// failed placement cascaded over them too.
func TestCloneRefusesATargetNameWithReplicasAndNoDefinition(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-orph")

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "dst-orph", NodeName: "node-x", Flags: []string{apiv1.ResourceFlagDiskless},
	}); err != nil {
		t.Fatalf("seed the replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-orph", "dst-orph", nil); code != http.StatusConflict {
		t.Fatalf("clone over replicas with no definition = %d, want 409", code)
	}

	if _, err := st.Resources().Get(ctx, "dst-orph", "node-x"); err != nil {
		t.Fatalf("the refusal took the replica that was there: %v", err)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "dst-orph"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the refusal left a definition behind: err=%v", err)
	}
}
