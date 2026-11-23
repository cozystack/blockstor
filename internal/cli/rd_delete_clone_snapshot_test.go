// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// `rd clone` takes the same internal snapshot the REST door takes, and `rd d`
// of the clone reaped nothing, so the source could never be deleted through
// the CLI either.
func TestResourceDefinitionDeleteOfACloneReapsItsSnapshot(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedSnapshotSource)

	if got := app.Run(t.Context(), []string{"rd", "clone", "pvc-x", "pvc-reap"}); got != 0 {
		t.Fatalf("clone exit = %d (stderr: %s)", got, errBuf.String())
	}

	// The owner prop is the snapshot's. Carried onto the clone, it would be
	// copied into every snapshot later taken of the clone as a claim of
	// ownership nobody made.
	clone, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-reap")
	if err != nil {
		t.Fatalf("get the clone: %v", err)
	}

	if owner, ok := clone.Props[store.CloneSnapshotOwnerProp]; ok {
		t.Errorf("the clone carries the snapshot's owner prop %q", owner)
	}

	if got := app.Run(t.Context(), []string{"rd", "d", "pvc-reap"}); got != 0 {
		t.Fatalf("delete of the clone exit = %d (stderr: %s)", got, errBuf.String())
	}

	backend := appStore(t, app)

	if _, err := backend.Snapshots().Get(t.Context(), "pvc-x", "clone-pvc-reap"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the internal snapshot outlived the clone it backed: %v", err)
	}

	// seedSnapshotSource also leaves an operator snapshot on pvc-x, which the
	// delete has to keep refusing over. The clone's own must not be among what
	// it names.
	app.Run(t.Context(), []string{"rd", "d", "pvc-x"})

	if strings.Contains(errBuf.String(), "clone-pvc-reap") {
		t.Errorf("the source delete still meets the clone's snapshot: %s", errBuf.String())
	}
}

// An operator can name a snapshot anything, `clone-<target>` included, and
// restore it under that target. Only the prop the clone path writes makes a
// snapshot the clone's to reap.
func TestResourceDefinitionDeleteKeepsAnOperatorSnapshotNamedLikeAClone(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)
		_ = backend.Snapshots().Create(ctx, &apiv1.Snapshot{
			Name: "clone-pvc-op", ResourceName: "pvc-x",
			Nodes:             []string{"node-1", "node-2"},
			VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1 << 20}},
		})
	})

	argv := []string{
		"s", "resource", "restore",
		"--from-resource", "pvc-x", "--from-snapshot", "clone-pvc-op", "--to-resource", "pvc-op",
	}

	if got := app.Run(t.Context(), argv); got != 0 {
		t.Fatalf("restore exit = %d (stderr: %s)", got, errBuf.String())
	}

	if got := app.Run(t.Context(), []string{"rd", "d", "pvc-op"}); got != 0 {
		t.Fatalf("delete exit = %d (stderr: %s)", got, errBuf.String())
	}

	if _, err := appStore(t, app).Snapshots().Get(t.Context(), "pvc-x", "clone-pvc-op"); err != nil {
		t.Errorf("the operator's snapshot was reaped because of its name: %v", err)
	}
}

// The CLI refusal over a source still carrying a snapshot its clone left had no
// case that reached it: every fixture's list came back empty.
func TestResourceDefinitionDeleteNamesACloneSnapshotLeftBehind(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-left"})
		_ = backend.Snapshots().Create(ctx, &apiv1.Snapshot{
			Name: "clone-pvc-gone", ResourceName: "pvc-left",
			Props: map[string]string{store.CloneSnapshotOwnerProp: "pvc-gone"},
		})
	})

	if got := app.Run(t.Context(), []string{"rd", "d", "pvc-left"}); got == 0 {
		t.Fatal("delete of a source still carrying a snapshot succeeded")
	}

	if !strings.Contains(errBuf.String(), "clone-pvc-gone") {
		t.Errorf("refusal %q does not name the snapshot the clone left", errBuf.String())
	}
}
