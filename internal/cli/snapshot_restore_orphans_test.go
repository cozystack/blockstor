// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"errors"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// `s resource restore` into a name with a diskless replica and no
// definition over it is refused before anything is written, the way the
// REST door refuses it, and the replica stays.
func TestSnapshotRestoreRefusesATargetWithOrphanReplicas(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)
		_ = backend.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-orph", NodeName: "node-2", Flags: []string{apiv1.ResourceFlagDiskless},
		})
	})

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-orph")); got == 0 {
		t.Fatal("restore over an orphan diskless replica succeeded")
	}

	st := appStore(t, app)

	if _, err := st.Resources().Get(t.Context(), "pvc-orph", "node-2"); err != nil {
		t.Fatalf("the refusal took the orphan: %v", err)
	}

	if _, err := st.ResourceDefinitions().Get(t.Context(), "pvc-orph"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the refusal left a definition behind: err=%v", err)
	}
}
