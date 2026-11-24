// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"bytes"
	"context"
	"strconv"
	"sync/atomic"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"

	"github.com/cozystack/blockstor/internal/cli"
)

// countingVDs records whether the command reads the volume definitions. Embedding the
// real store keeps every other method behaviourally identical, so the only
// difference between this and the plain in-memory store is the counters.
type countingVDs struct {
	store.VolumeDefinitionStore

	perDefinition atomic.Int64
	wholeCluster  atomic.Int64
}

func (c *countingVDs) List(ctx context.Context, rdName string) ([]apiv1.VolumeDefinition, error) {
	c.perDefinition.Add(1)

	return c.VolumeDefinitionStore.List(ctx, rdName) //nolint:wrapcheck // test helper
}

func (c *countingVDs) ListAll(ctx context.Context) (map[string][]apiv1.VolumeDefinition, error) {
	c.wholeCluster.Add(1)

	return c.VolumeDefinitionStore.ListAll(ctx) //nolint:wrapcheck // test helper
}

type countingStore struct {
	store.Store

	vds *countingVDs
}

func (c *countingStore) VolumeDefinitions() store.VolumeDefinitionStore { return c.vds }

// seedCountedCluster builds a cluster of `definitions` single-volume
// definitions, each with one replica, behind counters on the volume reads.
func seedCountedCluster(t *testing.T, definitions int) *countingStore {
	t.Helper()

	backend := store.NewInMemory()
	ctx := t.Context()

	for i := range definitions {
		name := "pvc-" + strconv.Itoa(i)

		if err := backend.ResourceDefinitions().Create(ctx,
			&apiv1.ResourceDefinition{Name: name}); err != nil {
			t.Fatalf("seed definition: %v", err)
		}

		if err := backend.VolumeDefinitions().Create(ctx, name,
			&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1 << 20}); err != nil {
			t.Fatalf("seed volume: %v", err)
		}

		if err := backend.Resources().Create(ctx,
			&apiv1.Resource{Name: name, NodeName: "node-1"}); err != nil {
			t.Fatalf("seed replica: %v", err)
		}
	}

	return &countingStore{
		Store: backend,
		vds:   &countingVDs{VolumeDefinitionStore: backend.VolumeDefinitions()},
	}
}

// runCountedList runs the command against the counted store and fails on a
// non-zero exit.
func runCountedList(t *testing.T, counted *countingStore, args ...string) {
	t.Helper()

	var out, errBuf bytes.Buffer

	app := &cli.App{
		Out: &out,
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			return counted, nil
		},
	}

	if got := app.Run(t.Context(), args); got != 0 {
		t.Fatalf("%v: exit = %d (stderr: %s)", args, got, errBuf.String())
	}
}

// `resource list` used to read the volume sizes to derive a sync percentage,
// one definition at a time or the whole cluster at once. The State column now
// shows DRBD's own per-peer `done`, which arrives with the replicas, so the
// command reads no volume definition at all, wide or narrowed: on the
// Kubernetes store each such read is a round trip against an uncached client,
// in the command an operator runs while watching a resync.
func TestResourceListReadsNoVolumeDefinitions(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{
		{"resource", "list"},
		{"resource", "list", "-r", "pvc-7"},
		{"resource", "list", "--faulty"},
	} {
		counted := seedCountedCluster(t, 40)

		runCountedList(t, counted, args...)

		if n := counted.vds.perDefinition.Load() + counted.vds.wholeCluster.Load(); n != 0 {
			t.Errorf("%v: %d volume-definition reads, want none", args, n)
		}
	}
}

// A narrowed listing returns every row the filter matches. The filter compares
// case-insensitively, as LINSTOR does, while a replica's spec keeps the case its
// writer used: an adoption run or linstor-csi can store `NODE-1` for a node the
// operator types as `node-1`. Answering `-n` and `-r` with the scoped reads,
// which compare the stored spelling, printed an empty table for those replicas
// and exited zero, during the incident the narrowing is typed for.
//
// Both directions are held: stored in upper case and typed in lower, and the
// mirror.
func TestResourceListNarrowedFindsEveryStoredSpelling(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()

	for _, rep := range []apiv1.Resource{
		{Name: "PVC-ADOPTED", NodeName: "NODE-1"},
		{Name: "pvc-written", NodeName: "node-2"},
	} {
		if err := backend.ResourceDefinitions().Create(ctx,
			&apiv1.ResourceDefinition{Name: rep.Name}); err != nil {
			t.Fatalf("seed definition: %v", err)
		}

		if err := backend.Resources().Create(ctx, &rep); err != nil {
			t.Fatalf("seed replica: %v", err)
		}
	}

	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"node stored upper, typed lower", []string{"-n", "node-1"}, "PVC-ADOPTED"},
		{"definition stored upper, typed lower", []string{"-r", "pvc-adopted"}, "PVC-ADOPTED"},
		{"node stored lower, typed upper", []string{"-n", "NODE-2"}, "pvc-written"},
		{"definition stored lower, typed upper", []string{"-r", "PVC-WRITTEN"}, "pvc-written"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var out, errBuf bytes.Buffer

			app := &cli.App{
				Out: &out,
				Err: &errBuf,
				StoreFor: func(context.Context) (store.Store, error) {
					return backend, nil
				},
			}

			args := append([]string{"resource", "list", "-m"}, tc.args...)
			if got := app.Run(t.Context(), args); got != 0 {
				t.Fatalf("%v: exit = %d (stderr: %s)", args, got, errBuf.String())
			}

			if !bytes.Contains(out.Bytes(), []byte(tc.want)) {
				t.Errorf("%v lost the replica of %s; output = %s", args, tc.want, out.String())
			}
		})
	}
}
