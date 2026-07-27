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

// Every noun that carries properties uses the same grammar: the
// object's identifying positionals, then the key, then an optional
// value. The identifier arity differs per noun and getting it wrong
// would write the key onto the wrong object, so each noun is pinned
// here with the argument order this repository's harnesses use.
func TestPropertyGrammarPerNoun(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		set  []string
		list []string
		seed func(context.Context, store.Store)
	}{
		{
			name: "resource-definition",
			set:  []string{"rd", "sp", "pvc-x", "Aux/probe", "v"},
			list: []string{"rd", "lp", "pvc-x"},
			seed: seedDefinition,
		},
		{
			// node set-property <node> <key> <value>
			name: "node",
			set:  []string{"n", "sp", "node-1", "Aux/probe", "v"},
			list: []string{"n", "lp", "node-1"},
			seed: func(ctx context.Context, backend store.Store) {
				_ = backend.Nodes().Create(ctx, &apiv1.Node{Name: "node-1"})
			},
		},
		{
			// volume-definition set-property <rd> <volume-number> <key> <value>
			name: "volume-definition",
			set:  []string{"vd", "sp", "pvc-x", "0", "Aux/probe", "v"},
			list: []string{"vd", "lp", "pvc-x", "0"},
			seed: func(ctx context.Context, backend store.Store) {
				seedDefinition(ctx, backend)
				_ = backend.VolumeDefinitions().Create(ctx, "pvc-x", &apiv1.VolumeDefinition{SizeKib: 1024})
			},
		},
		{
			// resource set-property <node> <rd> <key> <value>
			name: "resource",
			set:  []string{"r", "sp", "node-1", "pvc-x", "Aux/probe", "v"},
			list: []string{"r", "lp", "node-1", "pvc-x"},
			seed: seedResource,
		},
		{
			// storage-pool set-property <node> <pool> <key> <value>
			name: "storage-pool",
			set:  []string{"sp", "set-property", "node-1", "data", "Aux/probe", "v"},
			list: []string{"sp", "list-properties", "node-1", "data"},
			seed: func(ctx context.Context, backend store.Store) {
				_ = backend.StoragePools().Create(ctx, &apiv1.StoragePool{
					NodeName: "node-1", StoragePoolName: "data",
				})
			},
		},
		{
			// resource-group set-property <rg> <key> <value>
			name: "resource-group",
			set:  []string{"rg", "sp", "grp", "Aux/probe", "v"},
			list: []string{"rg", "lp", "grp"},
			seed: func(ctx context.Context, backend store.Store) {
				_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "grp"})
			},
		},
		{
			// volume-group set-property <rg> <volume-number> <key> <value>
			name: "volume-group",
			set:  []string{"vg", "sp", "grp", "0", "Aux/probe", "v"},
			list: []string{"vg", "lp", "grp", "0"},
			seed: func(ctx context.Context, backend store.Store) {
				_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
					Name:         "grp",
					VolumeGroups: []apiv1.VolumeGroup{{VolumeNumber: 0}},
				})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			app, out, errBuf := newApp(t, tc.seed)

			if got := app.Run(t.Context(), tc.set); got != 0 {
				t.Fatalf("set-property exit = %d (stderr: %s)", got, errBuf.String())
			}

			out.Reset()

			if got := app.Run(t.Context(), tc.list); got != 0 {
				t.Fatalf("list-properties exit = %d (stderr: %s)", got, errBuf.String())
			}

			if !strings.Contains(out.String(), "Aux/probe") {
				t.Fatalf("list-properties lost the key:\n%s", out.String())
			}

			// delete-property removes it again, on every noun.
			del := make([]string, len(tc.set)-1)
			copy(del, tc.set)
			del[1] = "delete-property"

			if got := app.Run(t.Context(), del); got != 0 {
				t.Fatalf("delete-property exit = %d (stderr: %s)", got, errBuf.String())
			}

			out.Reset()

			if got := app.Run(t.Context(), tc.list); got != 0 {
				t.Fatalf("list-properties after delete exit = %d", got)
			}

			if strings.Contains(out.String(), "Aux/probe") {
				t.Errorf("delete-property did not remove the key:\n%s", out.String())
			}
		})
	}
}

// The controller's bag has no identifying positional, so
// delete-property takes the key alone.
func TestControllerDeleteProperty(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, nil)

	if got := app.Run(t.Context(), []string{"c", "sp", "Aux/probe", "v"}); got != 0 {
		t.Fatalf("set exit = %d (stderr: %s)", got, errBuf.String())
	}

	if got := app.Run(t.Context(), []string{"c", "dp", "Aux/probe"}); got != 0 {
		t.Fatalf("delete-property exit = %d (stderr: %s)", got, errBuf.String())
	}

	out.Reset()

	if got := app.Run(t.Context(), []string{"c", "lp"}); got != 0 {
		t.Fatalf("list exit = %d", got)
	}

	if strings.Contains(out.String(), "Aux/probe") {
		t.Errorf("controller delete-property did not remove the key:\n%s", out.String())
	}
}

// Property writes on an object that does not exist are an API-level
// rejection (10), not a client-side one (2) and not a silent success:
// a runbook that typos a node name must see it fail.
func TestSetPropertyOnMissingObject(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, nil)

	if got := app.Run(t.Context(), []string{"n", "sp", "no-such-node", "Aux/probe", "v"}); got != 10 {
		t.Errorf("set-property on a missing node exit = %d, want 10 (stderr: %s)", got, errBuf.String())
	}
}
