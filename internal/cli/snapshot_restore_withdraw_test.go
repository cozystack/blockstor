// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// The CLI restore reads the snapshot back after it creates the definition,
// the same way the REST door does, and withdraws from one a reap has marked.
func TestSnapshotRestoreWithdrawsFromASnapshotBeingReaped(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)
		_ = backend.Snapshots().Create(ctx, &apiv1.Snapshot{
			Name: "clone-pvc-old", ResourceName: "pvc-x",
			Nodes:             []string{"node-1", "node-2"},
			VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1 << 20}},
			Props: map[string]string{
				store.CloneSnapshotOwnerProp:   "pvc-old",
				store.CloneSnapshotReapingProp: "pvc-old@" + time.Now().UTC().Format(time.RFC3339),
			},
		})
	})

	if got := app.Run(t.Context(), []string{
		"s", "resource", "restore", "--from-resource", "pvc-x",
		"--from-snapshot", "clone-pvc-old", "--to-resource", "pvc-third",
	}); got == 0 {
		t.Fatal("restore from a snapshot being reaped succeeded")
	}

	if !strings.Contains(errBuf.String(), "retry once it finishes") {
		t.Errorf("refusal %q does not say to retry once the reap finishes", errBuf.String())
	}

	if _, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-third"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the definition restored from a snapshot being reaped was left behind: %v", err)
	}
}

// The CLI restore drops the snapshot's reaping mark the way the REST door does.
func TestSnapshotRestoreDropsAnExpiredReapingMark(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)
		_ = backend.Snapshots().Create(ctx, &apiv1.Snapshot{
			Name: "snap-stale", ResourceName: "pvc-x",
			Nodes:             []string{"node-1", "node-2"},
			VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1 << 20}},
			Props: map[string]string{
				store.CloneSnapshotReapingProp: "gone@" + time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
			},
		})
	})

	if got := app.Run(t.Context(), []string{
		"s", "resource", "restore", "--from-resource", "pvc-x",
		"--from-snapshot", "snap-stale", "--to-resource", "pvc-stale",
	}); got != 0 {
		t.Fatalf("restore over an expired mark exit = %d (stderr: %s)", got, errBuf.String())
	}

	rd, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-stale")
	if err != nil {
		t.Fatalf("read the restored definition: %v", err)
	}

	if mark, ok := rd.Props[store.CloneSnapshotReapingProp]; ok {
		t.Errorf("the restored definition carries the snapshot's reaping mark %q", mark)
	}
}

// markSnapReaping puts a live reaping mark on pvc-x/snap-1, as a reap of it
// that started after the restore read it.
func markSnapReaping(t *testing.T, st store.Store) {
	t.Helper()

	snap, err := st.Snapshots().Get(t.Context(), "pvc-x", "snap-1")
	if err != nil {
		t.Fatalf("read the snapshot: %v", err)
	}

	snap.Props = map[string]string{store.CloneSnapshotReapingProp: "other@" + time.Now().UTC().Format(time.RFC3339)}

	if err := st.Snapshots().Update(t.Context(), &snap); err != nil {
		t.Fatalf("mark the snapshot: %v", err)
	}
}

// assertNothingPlaced fails when the restore placed a replica of target.
func assertNothingPlaced(t *testing.T, st store.Store, target string) {
	t.Helper()

	replicas, err := st.Resources().ListByDefinition(t.Context(), target)
	if err != nil || len(replicas) != 0 {
		t.Errorf("replicas of %s after the refusal = %d (%v), want none", target, len(replicas), err)
	}
}

// A prepared target the restore marked reads the snapshot back before it
// places anything, and a reap that started on it refuses the placement.
func TestSnapshotRestoreIntoAPreparedTargetWithdrawsFromASnapshotBeingReaped(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedSnapshotSource)

	for _, argv := range [][]string{
		{"rd", "c", "pvc-prep"},
		{"s", "vd", "restore", "--from-resource", "pvc-x", "--from-snapshot", "snap-1", "--to-resource", "pvc-prep"},
	} {
		if got := app.Run(t.Context(), argv); got != 0 {
			t.Fatalf("%v exit = %d (stderr: %s)", argv, got, errBuf.String())
		}
	}

	markSnapReaping(t, appStore(t, app))

	argv := []string{"s", "resource", "restore", "--from-resource", "pvc-x", "--from-snapshot", "snap-1", "--to-resource", "pvc-prep"}
	if got := app.Run(t.Context(), argv); got == 0 {
		t.Fatal("restore into a prepared target from a snapshot being reaped exited 0")
	}

	assertNothingPlaced(t, appStore(t, app), "pvc-prep")
}

// The same read-back guards the resume of this restore's own leftover.
func TestSnapshotRestoreOfItsLeftoverWithdrawsFromASnapshotBeingReaped(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedSnapshotSource)

	for _, argv := range [][]string{
		{"rd", "c", "pvc-left"},
		{"s", "vd", "restore", "--from-resource", "pvc-x", "--from-snapshot", "snap-1", "--to-resource", "pvc-left"},
	} {
		if got := app.Run(t.Context(), argv); got != 0 {
			t.Fatalf("%v exit = %d (stderr: %s)", argv, got, errBuf.String())
		}
	}

	st := appStore(t, app)

	err := st.ResourceDefinitions().PatchResourceDefinitionSpec(t.Context(), "pvc-left", func(rd *apiv1.ResourceDefinition) error {
		rd.Props = map[string]string{store.RestoreFromSnapshotProp: "pvc-x:snap-1"}

		return nil
	})
	if err != nil {
		t.Fatalf("leave the leftover: %v", err)
	}

	markSnapReaping(t, st)

	argv := []string{"s", "resource", "restore", "--from-resource", "pvc-x", "--from-snapshot", "snap-1", "--to-resource", "pvc-left"}
	if got := app.Run(t.Context(), argv); got == 0 {
		t.Fatal("resume of a leftover from a snapshot being reaped exited 0")
	}

	if !strings.Contains(errBuf.String(), "retry once it finishes") {
		t.Errorf("stderr %q is not the withdrawal", errBuf.String())
	}

	assertNothingPlaced(t, st, "pvc-left")
}
