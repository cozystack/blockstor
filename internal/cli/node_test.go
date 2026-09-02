// SPDX-License-Identifier: Apache-2.0

/*
Copyright 2026 Cozystack contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cli_test

import (
	"context"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

func boolPtr(b bool) *bool { return &b }

// Draining a node latches EVICTED, which is what keeps the autoplacer
// off it; restore clears the latch when the node comes back before the
// migration finished.
func TestNodeEvacuateAndRestore(t *testing.T) {
	t.Parallel()

	for _, verb := range []string{"evacuate", "evict"} {
		app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
			_ = backend.Nodes().Create(ctx, &apiv1.Node{Name: "node-1"})
		})

		if got := app.Run(t.Context(), []string{"n", verb, "node-1"}); got != 0 {
			t.Fatalf("%s exit = %d (stderr: %s)", verb, got, errBuf.String())
		}

		node, err := appStore(t, app).Nodes().Get(t.Context(), "node-1")
		if err != nil {
			t.Fatalf("get node: %v", err)
		}

		if !containsFlag(node.Flags, "EVICTED") {
			t.Errorf("%s did not latch EVICTED: %v", verb, node.Flags)
		}

		if got := app.Run(t.Context(), []string{"n", "restore", "node-1"}); got != 0 {
			t.Fatalf("restore exit = %d (stderr: %s)", got, errBuf.String())
		}

		node, err = appStore(t, app).Nodes().Get(t.Context(), "node-1")
		if err != nil {
			t.Fatalf("get node: %v", err)
		}

		if containsFlag(node.Flags, "EVICTED") {
			t.Errorf("restore did not clear EVICTED: %v", node.Flags)
		}
	}
}

// Draining a node whose volumes are still mounted would let the
// autoplacer strand a live volume, so it is refused — and --force is
// the operator's conscious override.
func TestNodeEvacuateRefusesInUse(t *testing.T) {
	t.Parallel()

	seed := func(ctx context.Context, backend store.Store) {
		_ = backend.Nodes().Create(ctx, &apiv1.Node{Name: "node-1"})
		_ = backend.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-x", NodeName: "node-1",
			State: apiv1.ResourceState{InUse: boolPtr(true)},
		})
	}

	app, _, errBuf := newApp(t, seed)

	if got := app.Run(t.Context(), []string{"n", "evacuate", "node-1"}); got == 0 {
		t.Fatal("evacuating a node with an in-use resource succeeded")
	}

	if !strings.Contains(errBuf.String(), "pvc-x") {
		t.Errorf("the refusal does not name the blocking resource:\n%s", errBuf.String())
	}

	forced, _, forcedErr := newApp(t, seed)
	if got := forced.Run(t.Context(), []string{"n", "evacuate", "node-1", "--force"}); got != 0 {
		t.Errorf("--force did not override the refusal: exit %d (stderr: %s)", got, forcedErr.String())
	}
}

// A replica whose satellite has not reported yet has in_use unset.
// That is "unknown", not "in use" — refusing there would block an
// operator draining a node that never came up.
func TestNodeEvacuateAllowsUnobservedResource(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.Nodes().Create(ctx, &apiv1.Node{Name: "node-1"})
		_ = backend.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-x", NodeName: "node-1"})
	})

	if got := app.Run(t.Context(), []string{"n", "evacuate", "node-1"}); got != 0 {
		t.Errorf("evacuate exit = %d, want 0 (stderr: %s)", got, errBuf.String())
	}
}

// `node lost` cascade-deletes the replicas and pools of a dead
// satellite. The satellite that would otherwise run the cleanup is
// gone with the node, so leaving it to a finalizer would hang every
// orphan forever and brick the next definition that recycles the name.
func TestNodeLostCascades(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.Nodes().Create(ctx, &apiv1.Node{Name: "node-1", ConnectionStatus: "OFFLINE"})
		_ = backend.Nodes().Create(ctx, &apiv1.Node{Name: "node-2", ConnectionStatus: "OFFLINE"})
		_ = backend.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-x", NodeName: "node-1"})
		_ = backend.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-x", NodeName: "node-2"})
		_ = backend.StoragePools().Create(ctx, &apiv1.StoragePool{NodeName: "node-1", StoragePoolName: "data"})
	})

	if got := app.Run(t.Context(), []string{"n", "lost", "node-1"}); got != 0 {
		t.Fatalf("lost exit = %d (stderr: %s)", got, errBuf.String())
	}

	backend := appStore(t, app)

	if _, err := backend.Resources().Get(t.Context(), "pvc-x", "node-1"); err == nil {
		t.Error("the replica on the lost node survived")
	}

	pools, err := backend.StoragePools().ListByNode(t.Context(), "node-1")
	if err != nil {
		t.Fatalf("list pools: %v", err)
	}

	if len(pools) != 0 {
		t.Errorf("the pool on the lost node survived: %+v", pools)
	}

	// The surviving peer is left alone so the tiebreaker reconciler
	// can stamp a fresh witness.
	if _, err := backend.Resources().Get(t.Context(), "pvc-x", "node-2"); err != nil {
		t.Errorf("the peer replica was collateral damage: %v", err)
	}

	// A re-run of a teardown script must not fail on cleaned state.
	if got := app.Run(t.Context(), []string{"n", "lost", "node-1"}); got != 0 {
		t.Errorf("second lost exit = %d, want 0 (idempotent)", got)
	}
}

// Losing a node whose satellite is still ONLINE orphans its resources
// and leaves the DRBD state on the host, so it is refused.
func TestNodeLostRefusesOnlineSatellite(t *testing.T) {
	t.Parallel()

	seed := func(ctx context.Context, backend store.Store) {
		_ = backend.Nodes().Create(ctx, &apiv1.Node{Name: "node-1", ConnectionStatus: "ONLINE"})
	}

	app, _, _ := newApp(t, seed)
	if got := app.Run(t.Context(), []string{"n", "lost", "node-1"}); got == 0 {
		t.Error("losing an ONLINE node succeeded")
	}

	forced, _, errBuf := newApp(t, seed)
	if got := forced.Run(t.Context(), []string{"n", "lost", "node-1", "--force"}); got != 0 {
		t.Errorf("--force did not override: exit %d (stderr: %s)", got, errBuf.String())
	}
}

// `node info` answers "why didn't autoplace pick this node?" — so it
// has to name the provider kinds, including for a node whose satellite
// has never reported its capabilities.
func TestNodeInfo(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.Nodes().Create(ctx, &apiv1.Node{Name: "node-1"})
	})

	if got := app.Run(t.Context(), []string{"n", "info"}); got != 0 {
		t.Fatalf("info exit = %d (stderr: %s)", got, errBuf.String())
	}

	for _, want := range []string{"node-1", "ZFS", "DRBD"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("node info is missing %q:\n%s", want, out.String())
		}
	}
}
