// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// A re-run over a finished restore needs neither the snapshot nor the source:
// both can be cleaned up once the restore is done.
func TestSnapshotRestoreRerunSurvivesTheSnapshotAndTheSource(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		remove func(ctx context.Context, backend store.Store) error
	}{
		{name: "snapshot-deleted", remove: func(ctx context.Context, backend store.Store) error {
			return backend.Snapshots().Delete(ctx, "pvc-x", "snap-1")
		}},
		{name: "source-deleted", remove: func(ctx context.Context, backend store.Store) error {
			return backend.ResourceDefinitions().Delete(ctx, "pvc-x")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			app, outBuf, errBuf := newApp(t, seedSnapshotSource)

			if got := app.Run(t.Context(), restoreResourceArgv("pvc-done", "--nodes", "node-1")); got != 0 {
				t.Fatalf("first restore exit = %d (stderr: %s)", got, errBuf.String())
			}

			backend := appStore(t, app)
			if err := tc.remove(t.Context(), backend); err != nil {
				t.Fatalf("fixture: %v", err)
			}

			before, _ := backend.Resources().ListByDefinition(t.Context(), "pvc-done")

			if got := app.Run(t.Context(), restoreResourceArgv("pvc-done", "--nodes", "node-1")); got != 0 {
				t.Fatalf("re-run exit = %d (stderr: %s), want the finished restore left alone", got, errBuf.String())
			}

			if !strings.Contains(outBuf.String(), "pvc-done is already restored") {
				t.Errorf("stdout %q does not say the restore is already done", outBuf.String())
			}

			after, _ := backend.Resources().ListByDefinition(t.Context(), "pvc-done")
			if len(after) != len(before) {
				t.Errorf("the re-run placed replicas: %d before, %d after", len(before), len(after))
			}
		})
	}
}

// An unfinished restore cannot be finished from a snapshot that is gone, and
// the refusal names the way out.
func TestSnapshotRestoreRefusesAnUnfinishedTargetWhoseSnapshotIsGone(t *testing.T) {
	t.Parallel()

	snap := apiv1.Snapshot{
		Name: "snap-1", ResourceName: "pvc-x",
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1 << 20}},
	}

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)
		_ = backend.Snapshots().Delete(ctx, "pvc-x", "snap-1")
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name: "pvc-half", Props: store.WithRestoreMarker(nil, &snap),
		})
	})

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-half", "--nodes", "node-1")); got == 0 {
		t.Fatal("the restore over an unfinished target with its snapshot gone succeeded")
	}

	if !strings.Contains(errBuf.String(), "restore it from another snapshot") {
		t.Errorf("stderr %q does not name the way out", errBuf.String())
	}

	if vds, _ := appStore(t, app).VolumeDefinitions().List(t.Context(), "pvc-half"); len(vds) != 0 {
		t.Errorf("the refused restore hydrated %d volume(s)", len(vds))
	}
}

// With no target to judge, a restore needs its source and its snapshot, and a
// missing one is the error, before anything is written.
func TestSnapshotRestoreNeedsItsSourceWhenThereIsNoTarget(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		remove func(ctx context.Context, backend store.Store)
		stderr string
	}{
		{name: "source-gone", stderr: "get resource definition pvc-x", remove: func(ctx context.Context, backend store.Store) {
			_ = backend.ResourceDefinitions().Delete(ctx, "pvc-x")
		}},
		{name: "snapshot-gone", stderr: "get snapshot snap-1 of pvc-x", remove: func(ctx context.Context, backend store.Store) {
			_ = backend.Snapshots().Delete(ctx, "pvc-x", "snap-1")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
				seedSnapshotSource(ctx, backend)
				tc.remove(ctx, backend)
			})

			if got := app.Run(t.Context(), restoreResourceArgv("pvc-new", "--nodes", "node-1")); got == 0 {
				t.Fatal("the restore succeeded without its source")
			}

			if !strings.Contains(errBuf.String(), tc.stderr) {
				t.Errorf("stderr %q does not name %q", errBuf.String(), tc.stderr)
			}

			if _, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-new"); err == nil {
				t.Error("the failed restore created its target")
			}
		})
	}
}

// A target restored before the volumes were recorded has nothing to be judged
// against once its snapshot is gone, and keeps the error it got.
func TestSnapshotRestoreKeepsALegacyTargetsErrorWhenTheSnapshotIsGone(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)
		_ = backend.Snapshots().Delete(ctx, "pvc-x", "snap-1")
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name:  "pvc-legacy",
			Props: map[string]string{store.RestoreFromSnapshotProp: "pvc-x:snap-1"},
		})
		_ = backend.VolumeDefinitions().Create(ctx, "pvc-legacy", &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1 << 20})
		_ = backend.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-legacy", NodeName: "node-1"})
	})

	if got := app.Run(t.Context(), restoreResourceArgv("pvc-legacy", "--nodes", "node-1")); got == 0 {
		t.Fatal("a legacy target with its snapshot gone was reported restored")
	}

	if !strings.Contains(errBuf.String(), "get snapshot snap-1 of pvc-x") {
		t.Errorf("stderr %q does not name the missing snapshot", errBuf.String())
	}
}
