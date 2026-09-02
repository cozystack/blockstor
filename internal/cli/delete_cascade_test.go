// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"errors"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Dropping a definition without reaping its replicas does not fail — the
// replicas simply stay, with no parent left to stamp a deletion on. The
// satellite finalizer never runs, `drbdadm down` never happens, and the DRBD
// minor, port and peer entries stay live on every node, so the next create
// with the same name collides with them.
func TestResourceDefinitionDeleteReapsItsReplicas(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})

		for _, node := range []string{"node-1", "node-2"} {
			_ = backend.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-x", NodeName: node})
		}
	})

	if got := app.Run(t.Context(), []string{"rd", "d", "pvc-x"}); got != 0 {
		t.Fatalf("delete exit = %d (stderr: %s)", got, errBuf.String())
	}

	left, err := appStore(t, app).Resources().ListByDefinition(t.Context(), "pvc-x")
	if err != nil && !isNotFoundForTest(err) {
		t.Fatalf("list replicas: %v", err)
	}

	if len(left) != 0 {
		t.Errorf("the delete left %d replica(s) with no definition above them", len(left))
	}
}

// A definition with snapshots is refused, the way the REST door refuses it,
// and refused BEFORE anything is torn down: a refusal after the cascade would
// leave the cluster half-dismantled, with the replicas gone, the definition
// kept and the snapshots pointing at neither.
func TestResourceDefinitionDeleteRefusesWhileSnapshotsRemain(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})
		_ = backend.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-x", NodeName: "node-1"})
		_ = backend.Snapshots().Create(ctx, &apiv1.Snapshot{
			Name: "snap-1", ResourceName: "pvc-x", Nodes: []string{"node-1"},
		})
	})

	if got := app.Run(t.Context(), []string{"rd", "d", "pvc-x"}); got == 0 {
		t.Fatal("a definition with snapshots was deleted")
	}

	backend := appStore(t, app)

	if _, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-x"); err != nil {
		t.Errorf("the refused delete removed the definition anyway: %v", err)
	}

	left, err := backend.Resources().ListByDefinition(t.Context(), "pvc-x")
	if err != nil && !isNotFoundForTest(err) {
		t.Fatalf("list replicas: %v", err)
	}

	if len(left) != 1 {
		t.Errorf("the refused delete tore down %d replica(s) on its way out", 1-len(left))
	}
}

// A node still carrying replicas or pools is refused rather than silently
// emptied, because deleting it leaves those objects naming a node that is
// gone and nothing reaps them afterwards.
func TestNodeDeleteRefusesWhileStillReferenced(t *testing.T) {
	t.Parallel()

	seed := func(ctx context.Context, backend store.Store) {
		_ = backend.Nodes().Create(ctx, &apiv1.Node{Name: "node-1"})
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})
		_ = backend.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-x", NodeName: "node-1"})
		_ = backend.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName: "node-1", StoragePoolName: "data", ProviderKind: apiv1.StoragePoolKindLVM,
		})
	}

	app, _, _ := newApp(t, seed)

	if got := app.Run(t.Context(), []string{"n", "d", "node-1"}); got == 0 {
		t.Fatal("a node still carrying a replica and a pool was deleted")
	}

	if _, err := appStore(t, app).Nodes().Get(t.Context(), "node-1"); err != nil {
		t.Errorf("the refused delete removed the node anyway: %v", err)
	}

	// --force is the explicit "this node is gone" decision, and it takes
	// the referencing objects with it rather than stranding them.
	forced, _, forcedErr := newApp(t, seed)
	if got := forced.Run(t.Context(), []string{"n", "d", "node-1", "--force"}); got != 0 {
		t.Fatalf("forced delete exit = %d (stderr: %s)", got, forcedErr.String())
	}

	backend := appStore(t, forced)

	pools, err := backend.StoragePools().ListByNode(t.Context(), "node-1")
	if err != nil && !isNotFoundForTest(err) {
		t.Fatalf("list pools: %v", err)
	}

	if len(pools) != 0 {
		t.Errorf("the forced delete left %d pool(s) on a node that is gone", len(pools))
	}

	left, err := backend.Resources().ListByDefinition(t.Context(), "pvc-x")
	if err != nil && !isNotFoundForTest(err) {
		t.Fatalf("list replicas: %v", err)
	}

	if len(left) != 0 {
		t.Errorf("the forced delete left %d replica(s) on a node that is gone", len(left))
	}
}

// isNotFoundForTest treats an empty listing and a NotFound alike: which one a
// listing returns for a parent that is gone depends on the backend.
func isNotFoundForTest(err error) bool {
	return errors.Is(err, store.ErrNotFound)
}

// REST creates the default diskless pool on every `node create`, so a node
// registered by piraeus, linstor-csi or the upstream client always carries
// one. Counting it as a reference makes an idle node undeletable — the
// refusal names a pool the operator has no way to remove, while `linstor n d`
// on the same cluster succeeds.
func TestNodeDeleteIgnoresTheDefaultDisklessPool(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.Nodes().Create(ctx, &apiv1.Node{Name: "node-1"})
		_ = backend.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName:        "node-1",
			StoragePoolName: apiv1.DfltDisklessStorPoolName,
			ProviderKind:    apiv1.StoragePoolKindDiskless,
		})
	})

	if got := app.Run(t.Context(), []string{"n", "d", "node-1"}); got != 0 {
		t.Fatalf("an idle node carrying only the default diskless pool was refused, "+
			"exit = %d (stderr: %s)", got, errBuf.String())
	}
}
