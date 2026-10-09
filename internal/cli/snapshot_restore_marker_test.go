// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/internal/cli"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

type foldingSnapshots struct{ store.SnapshotStore }

func (f foldingSnapshots) Get(ctx context.Context, rdName, snapName string) (apiv1.Snapshot, error) {
	return f.SnapshotStore.Get(ctx, strings.ToLower(rdName), strings.ToLower(snapName)) //nolint:wrapcheck // pass-through
}

type foldingSnapshotStore struct{ store.Store }

func (f foldingSnapshotStore) Snapshots() store.SnapshotStore {
	return foldingSnapshots{f.Store.Snapshots()}
}

// The marker is a store key for the placer and for the REST resume, so both
// halves are the stored spelling, not what the operator typed. The store folds
// snapshot names the way the Kubernetes store does, which is the only way the
// two spellings can differ and both be read.
func TestSnapshotResourceRestoreMarkerTakesTheStoredSpelling(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedSnapshotSource(t.Context(), backend)

	folding := foldingSnapshotStore{backend}

	var out, errBuf strings.Builder

	app := &cli.App{
		Out: &out,
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			return folding, nil
		},
	}

	argv := []string{
		"s", "resource", "restore",
		"--from-resource", "pvc-x", "--from-snapshot", "SNAP-1", "--to-resource", "pvc-cased",
	}

	if got := app.Run(t.Context(), argv); got != 0 {
		t.Fatalf("restore exit = %d (stderr: %s)", got, errBuf.String())
	}

	def, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-cased")
	if err != nil {
		t.Fatalf("get restored definition: %v", err)
	}

	if got := def.Props["BlockstorRestoreFromSnapshot"]; got != "pvc-x:snap-1" {
		t.Errorf("restore marker = %q, want pvc-x:snap-1, the spelling the snapshot is stored under", got)
	}
}
