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
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/store"
)

// The CLI writes definitions itself, so it gives them the peer
// authentication the REST doors do: a generated secret of their own and
// the sha1 digest, a restored one included, never its source's.
func TestDefinitionCreateSeedsPeerAuthentication(t *testing.T) {
	t.Parallel()

	const sourceSecret = "fixture-source-secret"

	for name, argv := range map[string][]string{
		"rd create": {"rd", "c", "pvc-new"},
		"spawn":     {"rg", "spawn-resources", "rg", "pvc-new"},
		"restore": {
			"s", "resource", "restore", "--from-resource", "pvc-x",
			"--from-snapshot", "snap-auth", "--to-resource", "pvc-new",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
				_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "rg"})

				seedSnapshotSource(ctx, backend)

				// Not snap-1: seedSnapshotSource made that one, without a
				// secret, and a second create of it would be refused.
				err := backend.Snapshots().Create(ctx, &apiv1.Snapshot{
					Name: "snap-auth", ResourceName: "pvc-x",
					Nodes:             []string{"node-1", "node-2"},
					VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1 << 20}},
					Props:             map[string]string{drbd.SharedSecretProp: sourceSecret},
				})
				if err != nil {
					t.Errorf("seed the snapshot: %v", err)
				}
			})

			if got := app.Run(t.Context(), argv); got != 0 {
				t.Fatalf("exit %d: %s", got, errBuf.String())
			}

			rd, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-new")
			if err != nil {
				t.Fatalf("get: %v", err)
			}

			secret := rd.Props[drbd.SharedSecretProp]
			if len(secret) != 20 || secret == sourceSecret {
				t.Errorf("shared secret of length %d (source's: %t), want a generated one of its own",
					len(secret), secret == sourceSecret)
			}

			if got := rd.Props[drbd.CramHMACAlgProp]; got != drbd.DefaultCramHMACAlg {
				t.Errorf("cram-hmac-alg %q, want %q", got, drbd.DefaultCramHMACAlg)
			}
		})
	}
}
