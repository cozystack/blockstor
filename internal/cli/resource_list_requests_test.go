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

// countingVDs records how the command reaches the volume sizes. Embedding the
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

// `resource list` fills its sync-percentage column from the volume sizes, and
// it used to read them one definition at a time. On the Kubernetes store each
// of those is a GET of one ResourceDefinition against an uncached client, so
// the command an operator runs while watching a resync cost one LIST plus one
// round trip per definition.
//
// The acceptance is a request count that does not grow with the number of
// definitions, so the test asserts it across two cluster sizes rather than
// against a fixed number: a per-definition read would make the count track
// the seed size, and no single expected value could catch that.
func TestResourceListDoesNotReadPerDefinition(t *testing.T) {
	t.Parallel()

	for _, definitions := range []int{3, 40} {
		backend := store.NewInMemory()
		ctx := context.Background()

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

		counted := &countingStore{Store: backend, vds: &countingVDs{VolumeDefinitionStore: backend.VolumeDefinitions()}}

		var out, errBuf bytes.Buffer

		app := &cli.App{
			Out: &out,
			Err: &errBuf,
			StoreFor: func(context.Context) (store.Store, error) {
				return counted, nil
			},
		}

		if got := app.Run(ctx, []string{"resource", "list"}); got != 0 {
			t.Fatalf("%d definitions: exit = %d (stderr: %s)", definitions, got, errBuf.String())
		}

		if n := counted.vds.perDefinition.Load(); n != 0 {
			t.Errorf("%d definitions: %d per-definition reads, want none — the count would "+
				"track the cluster size", definitions, n)
		}

		if n := counted.vds.wholeCluster.Load(); n != 1 {
			t.Errorf("%d definitions: %d whole-cluster reads, want exactly 1", definitions, n)
		}
	}
}
