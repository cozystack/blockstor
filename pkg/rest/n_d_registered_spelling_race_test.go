// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"net/http"
	"testing"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// foldingNodes resolves a node name the way the Kubernetes store does, through
// its folded form, and lands a replica under the registered spelling the moment
// the node row goes: the Bug 174 window, with a writer that spells the node
// the way it was registered.
type foldingNodes struct {
	store.NodeStore

	onDelete func(ctx context.Context, registered string) error
}

func (f foldingNodes) registered(ctx context.Context, name string) (string, error) {
	all, err := f.List(ctx)
	if err != nil {
		return "", errors.Wrap(err, "list nodes")
	}

	for i := range all {
		if store.FoldName(all[i].Name) == store.FoldName(name) {
			return all[i].Name, nil
		}
	}

	return name, nil
}

func (f foldingNodes) Get(ctx context.Context, name string) (apiv1.Node, error) {
	registered, err := f.registered(ctx, name)
	if err != nil {
		return apiv1.Node{}, err
	}

	return f.NodeStore.Get(ctx, registered) //nolint:wrapcheck // test double
}

func (f foldingNodes) Delete(ctx context.Context, name string) error {
	registered, err := f.registered(ctx, name)
	if err != nil {
		return err
	}

	err = f.NodeStore.Delete(ctx, registered)
	if err != nil {
		return err //nolint:wrapcheck // test double
	}

	return f.onDelete(ctx, registered)
}

type foldingNodeStore struct {
	store.Store

	nodes foldingNodes
}

func (f foldingNodeStore) Nodes() store.NodeStore { return f.nodes }

// The Bug 174 re-walk runs after the node row is deleted, and the spelling a
// node is registered under is only known from that row. Re-deriving the
// spellings there asked the caller's spelling and its fold alone, so a replica
// that raced in under the registered spelling was invisible to the re-walk, the
// rollback never fired, and the node stayed deleted under a live replica.
func TestNodeDeleteRollsBackARaceUnderTheRegisteredSpelling(t *testing.T) {
	t.Parallel()

	inner := store.NewInMemory()
	ctx := t.Context()

	if err := inner.Nodes().Create(ctx, &apiv1.Node{Name: "NODE-X", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := inner.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd-race"}); err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	st := foldingNodeStore{
		Store: inner,
		nodes: foldingNodes{
			NodeStore: inner.Nodes(),
			onDelete: func(ctx context.Context, registered string) error {
				return inner.Resources().Create(ctx, &apiv1.Resource{Name: "rd-race", NodeName: registered}) //nolint:wrapcheck // test double
			},
		},
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/node-x")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("node delete over a replica that raced in under NODE-X = %d, want 409 from the rollback",
			resp.StatusCode)
	}

	if _, err := inner.Nodes().Get(ctx, "NODE-X"); err != nil {
		t.Errorf("the node is gone with a replica still on it: %v", err)
	}
}
