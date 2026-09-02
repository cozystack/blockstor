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
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// The layer stack decides whether a volume is replicated and whether it is
// encrypted, and the satellite decides that by asking whether the stack
// contains the layer. An unrecognised token is therefore not a loud failure:
// it is a silently absent layer. `DRDB,STORAGE` is a single transposed
// character away from `DRBD,STORAGE` and brings the volume up as one local
// copy; `DRBD,LUSK,STORAGE` writes plaintext. Both exited 0 and were stored
// verbatim before these tests.
//
// The stacks below are the five the REST path has always refused.
func TestResourceDefinitionCreateRefusesBadLayerStacks(t *testing.T) {
	t.Parallel()

	for _, stack := range []string{
		"DRDB,STORAGE",      // typo: replication silently off
		"DRBD,LUSK,STORAGE", // typo: encryption silently off
		"LUKS,DRBD,STORAGE", // LUKS above DRBD replicates plaintext
		"STORAGE,DRBD",      // STORAGE must be terminal
		"CACHE,STORAGE",     // layer blockstor does not support
	} {
		app, _, errBuf := newApp(t, nil)

		argv := []string{"rd", "c", "pvc-x", "--layer-list", stack}
		if got := app.Run(t.Context(), argv); got == 0 {
			t.Errorf("%q was accepted; want a refusal (stderr: %s)", stack, errBuf.String())
		}
	}
}

// The stacks blockstor does support must keep working, or the check has
// become a blanket refusal.
func TestResourceDefinitionCreateAcceptsValidLayerStacks(t *testing.T) {
	t.Parallel()

	for i, stack := range []string{
		"STORAGE",
		"LUKS,STORAGE",
		"DRBD,STORAGE",
		"DRBD,LUKS,STORAGE",
		"drbd,storage", // upstream LINSTOR accepts mixed case
	} {
		app, _, errBuf := newApp(t, nil)

		name := "pvc-ok-" + string(rune('a'+i))
		argv := []string{"rd", "c", name, "--layer-list", stack}
		if got := app.Run(t.Context(), argv); got != 0 {
			t.Errorf("%q was refused: exit %d (stderr: %s)", stack, got, errBuf.String())
		}
	}
}

// `rd modify --layer-list` overwrites a live definition's stack, so it needs
// the same gate: a typo there turns replication off on a resource that has it.
func TestResourceDefinitionModifyRefusesBadLayerStack(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name:       "pvc-x",
			LayerStack: []string{"DRBD", "STORAGE"},
		})
	})

	if got := app.Run(t.Context(), []string{"rd", "m", "pvc-x", "--layer-list", "DRDB,STORAGE"}); got == 0 {
		t.Fatal("a typo'd layer stack overwrote a live definition; want a refusal")
	}

	def, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-x")
	if err != nil {
		t.Fatalf("get definition: %v", err)
	}

	if len(def.LayerStack) != 2 || def.LayerStack[0] != "DRBD" {
		t.Errorf("the refused modify still changed the stack: %v", def.LayerStack)
	}
}

// The resource-group select filter feeds every definition spawned from the
// group, so an unchecked stack there is the same defect one level up.
func TestResourceGroupRefusesBadLayerStack(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, nil)

	argv := []string{"rg", "c", "rg-x", "--layer-list", "DRDB,STORAGE"}
	if got := app.Run(t.Context(), argv); got == 0 {
		t.Fatal("a typo'd layer stack was accepted on a resource group; want a refusal")
	}
}
