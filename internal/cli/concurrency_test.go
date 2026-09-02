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
	"bytes"
	"context"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"

	"github.com/cozystack/blockstor/internal/cli"
)

// racingStore lets a test land a competing write in the window a CLI
// verb used to leave open: after it has decided what to write, before
// its write lands.
//
// The verbs this exercises talk to the store's Patch* entry points,
// which fetch current state inside the call. Writing the peer's change
// just before delegating therefore puts it exactly where a real
// concurrent writer would be — the reconciler stamping a flag while an
// operator sets a property on the same object.
type racingStore struct {
	store.Store

	peer func(context.Context)
}

func (s *racingStore) Resources() store.ResourceStore {
	return &racingResources{ResourceStore: s.Store.Resources(), peer: s.peer}
}

func (s *racingStore) VolumeDefinitions() store.VolumeDefinitionStore {
	return &racingVolumeDefinitions{VolumeDefinitionStore: s.Store.VolumeDefinitions(), peer: s.peer}
}

type racingResources struct {
	store.ResourceStore

	peer func(context.Context)
}

func (r *racingResources) PatchResourceSpec(
	ctx context.Context, rdName, node string, mutate func(*apiv1.Resource) error,
) error {
	r.peer(ctx)

	//nolint:wrapcheck // a pass-through wrapper; wrapping would obscure what the test asserts on
	return r.ResourceStore.PatchResourceSpec(ctx, rdName, node, mutate)
}

type racingVolumeDefinitions struct {
	store.VolumeDefinitionStore

	peer func(context.Context)
}

func (r *racingVolumeDefinitions) PatchVolumeDefinitionSpec(
	ctx context.Context, rdName string, volumeNumber int32, mutate func(*apiv1.VolumeDefinition) error,
) error {
	r.peer(ctx)

	//nolint:wrapcheck // a pass-through wrapper; wrapping would obscure what the test asserts on
	return r.VolumeDefinitionStore.PatchVolumeDefinitionSpec(ctx, rdName, volumeNumber, mutate)
}

// racingApp wires an App onto `backend` with `peer` racing every patch.
func racingApp(t *testing.T, backend store.Store, peer func(context.Context)) (*cli.App, *bytes.Buffer) {
	t.Helper()

	var out, errBuf bytes.Buffer

	racing := &racingStore{Store: backend, peer: peer}

	app := &cli.App{
		Out: &out,
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			return racing, nil
		},
	}

	return app, &errBuf
}

// TestSetPropertyKeepsAConcurrentPeersKey: `set-property` edits one key.
// Reading the whole bag, changing that key locally and writing the bag
// back reverts every key another writer added in between — silently, and
// with exit 0. The satellite and the migration reconciler both stamp
// per-replica properties, so this is the reconciler's work being undone
// by an operator command that reported success.
func TestSetPropertyKeepsAConcurrentPeersKey(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()

	err := backend.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-x",
		NodeName: "node-a",
		Props:    map[string]string{"Existing": "1"},
	})
	if err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	// The reconciler stamps its own key while our command is in flight.
	peer := func(ctx context.Context) {
		patchErr := backend.Resources().PatchResourceSpec(ctx, "pvc-x", "node-a",
			func(res *apiv1.Resource) error {
				if res.Props == nil {
					res.Props = map[string]string{}
				}

				res.Props["StampedByReconciler"] = "yes"

				return nil
			})
		if patchErr != nil {
			t.Errorf("peer write: %v", patchErr)
		}
	}

	app, errBuf := racingApp(t, backend, peer)

	got := app.Run(t.Context(), []string{"r", "set-property", "node-a", "pvc-x", "Ours", "1"})
	if got != 0 {
		t.Fatalf("set-property exit = %d (stderr: %s)", got, errBuf.String())
	}

	res, err := backend.Resources().Get(t.Context(), "pvc-x", "node-a")
	if err != nil {
		t.Fatalf("get resource: %v", err)
	}

	if res.Props["Ours"] != "1" {
		t.Errorf("our own key did not land: %v", res.Props)
	}

	if res.Props["StampedByReconciler"] != "yes" {
		t.Errorf("the concurrent peer's key was reverted: %v", res.Props)
	}

	if res.Props["Existing"] != "1" {
		t.Errorf("a pre-existing key was dropped: %v", res.Props)
	}
}

// TestSetSizeRefusesAShrinkAgainstAConcurrentGrow: the shrink guard has
// to compare against the size the write lands on. Comparing against a
// size read beforehand leaves a window in which the CSI grows a live
// volume; the guard then passes against the smaller pre-grow size and
// the absolute write truncates a volume that is now larger and in use.
func TestSetSizeRefusesAShrinkAgainstAConcurrentGrow(t *testing.T) {
	t.Parallel()

	const (
		// The command asks for 4 GiB: a grow against the seeded size,
		// a shrink against the size the peer leaves behind.
		startKib = 1 * 1024 * 1024 // 1 GiB
		grownKib = 8 * 1024 * 1024 // 8 GiB, courtesy of a concurrent resize
	)

	backend := store.NewInMemory()

	err := backend.VolumeDefinitions().Create(t.Context(), "pvc-x", &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      startKib,
	})
	if err != nil {
		t.Fatalf("seed volume definition: %v", err)
	}

	grown := false

	peer := func(ctx context.Context) {
		if grown {
			return
		}

		grown = true

		patchErr := backend.VolumeDefinitions().PatchVolumeDefinitionSpec(ctx, "pvc-x", 0,
			func(vd *apiv1.VolumeDefinition) error {
				vd.SizeKib = grownKib

				return nil
			})
		if patchErr != nil {
			t.Errorf("peer grow: %v", patchErr)
		}
	}

	app, _ := racingApp(t, backend, peer)

	if got := app.Run(t.Context(), []string{"vd", "set-size", "pvc-x", "0", "4G"}); got == 0 {
		t.Error("set-size truncated a volume that grew underneath it; want a refusal")
	}

	vd, err := backend.VolumeDefinitions().Get(t.Context(), "pvc-x", 0)
	if err != nil {
		t.Fatalf("get volume definition: %v", err)
	}

	if vd.SizeKib != grownKib {
		t.Errorf("size = %d KiB, want the concurrent grow (%d KiB) left intact", vd.SizeKib, grownKib)
	}
}
