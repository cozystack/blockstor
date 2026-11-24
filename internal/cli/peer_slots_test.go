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

// The CLI writes definitions itself, so it resolves their peer slots the
// way the REST door does: `--peer-slots`, else the group's, else
// PeerSlotsNewResource, else LINSTOR's 7.
func TestResourceDefinitionCreateResolvesPeerSlots(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		argv []string
		want int32
	}{
		"default":      {argv: []string{"rd", "c", "pvc-x"}, want: 7},
		"--peer-slots": {argv: []string{"rd", "c", "pvc-x", "--peer-slots", "5"}, want: 5},
		"group":        {argv: []string{"rd", "c", "pvc-x", "--resource-group", "rg"}, want: 9},
		"spawn":        {argv: []string{"rg", "spawn-resources", "rg", "pvc-x"}, want: 9},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			app, _, errBuf := newApp(t, func(ctx context.Context, st store.Store) {
				_ = st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "rg", PeerSlots: 9})
			})

			if got := app.Run(t.Context(), tc.argv); got != 0 {
				t.Fatalf("exit %d: %s", got, errBuf.String())
			}

			rd, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-x")
			if err != nil || rd.PeerSlots != tc.want {
				t.Errorf("peer slots: %d, %v; want %d", rd.PeerSlots, err, tc.want)
			}
		})
	}
}

func TestResourceDefinitionCreateRefusesTooManyPeerSlots(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, nil)

	if got := app.Run(t.Context(), []string{"rd", "c", "pvc-x", "--peer-slots", "32"}); got == 0 {
		t.Errorf("32 peer slots were accepted")
	}

	if _, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-x"); err == nil {
		t.Errorf("a refused definition was created")
	}
}
