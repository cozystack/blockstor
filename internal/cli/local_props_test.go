// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

func markPVCX(ctx context.Context, backend store.Store) {
	def, err := backend.ResourceDefinitions().Get(ctx, "pvc-x")
	if err != nil {
		return
	}

	if def.Props == nil {
		def.Props = map[string]string{}
	}

	def.Props[store.RollbackAbandonedProp] = "snapshots"
	_ = backend.ResourceDefinitions().Update(ctx, &def)
}

// The CLI copies a definition's props onward on the same two paths the REST
// door does, and the abandoned-rollback mark is about the definition it sits
// on, never about a copy of it.
func TestCLIDoesNotCarryTheAbandonedRollbackMarkOnward(t *testing.T) {
	t.Parallel()

	t.Run("snapshot-create", func(t *testing.T) {
		t.Parallel()

		app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
			seedSnapshotSource(ctx, backend)
			markPVCX(ctx, backend)
		})

		if got := app.Run(t.Context(), []string{"s", "c", "pvc-x", "snap-m"}); got != 0 {
			t.Fatalf("create exit = %d (stderr: %s)", got, errBuf.String())
		}

		snap, err := appStore(t, app).Snapshots().Get(t.Context(), "pvc-x", "snap-m")
		if err != nil {
			t.Fatalf("get snapshot: %v", err)
		}

		if step, ok := snap.Props[store.RollbackAbandonedProp]; ok {
			t.Errorf("the snapshot carries the source's mark %q", step)
		}
	})

	for _, tc := range []struct {
		name string
		seed func(context.Context, store.Store)
	}{
		{name: "restore-from-a-snapshot-carrying-it", seed: func(ctx context.Context, backend store.Store) {
			seedSnapshotSource(ctx, backend)
			_ = backend.Snapshots().Create(ctx, &apiv1.Snapshot{
				Name: "snap-m", ResourceName: "pvc-x", Nodes: []string{"node-1", "node-2"},
				Props:             map[string]string{store.RollbackAbandonedProp: "snapshots"},
				VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1 << 20}},
			})
		}},
		{name: "restore-falling-back-to-the-source-props", seed: func(ctx context.Context, backend store.Store) {
			seedSnapshotSource(ctx, backend)
			markPVCX(ctx, backend)
			_ = backend.Snapshots().Create(ctx, &apiv1.Snapshot{
				Name: "snap-m", ResourceName: "pvc-x", Nodes: []string{"node-1", "node-2"},
				VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1 << 20}},
			})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			app, _, errBuf := newApp(t, tc.seed)

			argv := []string{
				"s", "resource", "restore",
				"--from-resource", "pvc-x", "--from-snapshot", "snap-m", "--to-resource", "pvc-m",
			}

			if got := app.Run(t.Context(), argv); got != 0 {
				t.Fatalf("restore exit = %d (stderr: %s)", got, errBuf.String())
			}

			def, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-m")
			if err != nil {
				t.Fatalf("get restored definition: %v", err)
			}

			if step, ok := def.Props[store.RollbackAbandonedProp]; ok {
				t.Errorf("the restored definition carries the mark %q", step)
			}
		})
	}
}
