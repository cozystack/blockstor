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

// countingResources separates the node-scoped read from the whole-cluster one.
type countingResources struct {
	store.ResourceStore

	wholeCluster atomic.Int64
	byNode       atomic.Int64
}

func (c *countingResources) List(ctx context.Context) ([]apiv1.Resource, error) {
	c.wholeCluster.Add(1)

	return c.ResourceStore.List(ctx) //nolint:wrapcheck // test decorator
}

func (c *countingResources) ListByNode(ctx context.Context, node string) ([]apiv1.Resource, error) {
	c.byNode.Add(1)

	return c.ResourceStore.ListByNode(ctx, node) //nolint:wrapcheck // test decorator
}

type countingResourceStore struct {
	store.Store

	resources *countingResources
}

func (c *countingResourceStore) Resources() store.ResourceStore { return c.resources }

// `node lost` and `node evacuate` both ask about one node — which replicas it
// holds, and whether any of them is in use — and both answered by listing
// every replica in the cluster and filtering here. That is the read
// ListByNode was added to replace, and on the two commands an operator runs
// during a node failure it was still being taken.
func TestNodeCommandsReadOnlyTheirNode(t *testing.T) {
	t.Parallel()

	for name, argv := range map[string][]string{
		"node lost":     {"node", "lost", "node-1"},
		"node evacuate": {"node", "evacuate", "node-1"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			counted := seedTwoNodeCluster(t)

			var out, errBuf bytes.Buffer

			app := &cli.App{
				Out: &out,
				Err: &errBuf,
				StoreFor: func(context.Context) (store.Store, error) {
					return counted, nil
				},
			}

			if got := app.Run(t.Context(), argv); got != 0 {
				t.Fatalf("exit = %d (stderr: %s)", got, errBuf.String())
			}

			if n := counted.resources.wholeCluster.Load(); n != 0 {
				t.Errorf("%d whole-cluster reads, want none — the question is about one node", n)
			}

			if n := counted.resources.byNode.Load(); n == 0 {
				t.Error("no node-scoped reads at all; the command answered from somewhere else")
			}
		})
	}
}

// seedTwoNodeCluster puts replicas and a pool on each of two nodes, behind
// counters on the replica reads.
func seedTwoNodeCluster(t *testing.T) *countingResourceStore {
	t.Helper()

	backend := store.NewInMemory()
	ctx := t.Context()

	for _, node := range []string{"node-1", "node-2"} {
		if err := backend.Nodes().Create(ctx,
			&apiv1.Node{Name: node, Type: "SATELLITE"}); err != nil {
			t.Fatalf("seed node %s: %v", node, err)
		}

		if err := backend.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool-1",
			NodeName:        node,
			ProviderKind:    "LVM_THIN",
		}); err != nil {
			t.Fatalf("seed pool on %s: %v", node, err)
		}

		for i := range 3 {
			rd := "pvc-" + node + "-" + strconv.Itoa(i)

			if err := backend.ResourceDefinitions().Create(ctx,
				&apiv1.ResourceDefinition{Name: rd}); err != nil {
				t.Fatalf("seed definition: %v", err)
			}

			if err := backend.Resources().Create(ctx,
				&apiv1.Resource{Name: rd, NodeName: node}); err != nil {
				t.Fatalf("seed replica: %v", err)
			}
		}
	}

	return &countingResourceStore{
		Store:     backend,
		resources: &countingResources{ResourceStore: backend.Resources()},
	}
}
