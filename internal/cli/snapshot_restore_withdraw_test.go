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

	if !strings.Contains(errBuf.String(), "being deleted") {
		t.Errorf("refusal %q does not say the snapshot is going", errBuf.String())
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
