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

// `storage-pool create <provider> <node> <pool> <backing>` has to land
// the backing device under the provider-specific `StorDriver/*` key the
// satellite's provider factory reads. Writing it under the wrong key
// leaves the pool registered but unusable: every reconcile fails with
// "attach requires ...", and free/total capacity is never probed.
func TestStoragePoolCreateProviderKeys(t *testing.T) {
	t.Parallel()

	cases := []struct {
		provider string
		backing  string
		wantKind string
		wantKeys map[string]string
	}{
		{"lvm", "vg0", apiv1.StoragePoolKindLVM, map[string]string{"StorDriver/LvmVg": "vg0"}},
		{
			"lvmthin", "vg0/thin", apiv1.StoragePoolKindLVMThin,
			map[string]string{"StorDriver/LvmVg": "vg0", "StorDriver/ThinPool": "thin"},
		},
		{"zfs", "tank", apiv1.StoragePoolKindZFS, map[string]string{"StorDriver/ZPool": "tank"}},
		{"zfsthin", "tank", apiv1.StoragePoolKindZFSThin, map[string]string{"StorDriver/ZPoolThin": "tank"}},
		{"file", "/var/lib/bs", apiv1.StoragePoolKindFile, map[string]string{"StorDriver/FileDir": "/var/lib/bs"}},
		{"diskless", "", apiv1.StoragePoolKindDiskless, map[string]string{}},
	}

	for _, tc := range cases {
		t.Run(tc.provider, func(t *testing.T) {
			t.Parallel()

			app, _, errBuf := newApp(t, nil)

			argv := []string{"sp", "c", tc.provider, "node-1", "pool-1"}
			if tc.backing != "" {
				argv = append(argv, tc.backing)
			}

			if got := app.Run(t.Context(), argv); got != 0 {
				t.Fatalf("create exit = %d (stderr: %s)", got, errBuf.String())
			}

			pool, err := appStore(t, app).StoragePools().Get(t.Context(), "node-1", "pool-1")
			if err != nil {
				t.Fatalf("get pool: %v", err)
			}

			if pool.ProviderKind != tc.wantKind {
				t.Errorf("provider kind = %q, want %q", pool.ProviderKind, tc.wantKind)
			}

			for key, want := range tc.wantKeys {
				if pool.Props[key] != want {
					t.Errorf("props[%q] = %q, want %q (all props: %v)", key, pool.Props[key], want, pool.Props)
				}
			}
		})
	}
}

// A thin LVM pool is named `<vg>/<thinpool>`; anything else is a
// client-side rejection rather than a pool registered with a guessed
// thin-pool name that does not exist on the node.
func TestStoragePoolCreateLvmThinNeedsBothNames(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, nil)

	if got := app.Run(t.Context(), []string{"sp", "c", "lvmthin", "node-1", "pool-1", "vg0"}); got != 2 {
		t.Errorf("lvmthin with no thin-pool name exit = %d, want 2", got)
	}
}

// An unknown provider is a client-side rejection: registering the pool
// with an empty kind would leave it permanently un-reconcilable.
func TestStoragePoolCreateUnknownProvider(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, nil)

	if got := app.Run(t.Context(), []string{"sp", "c", "exos", "node-1", "pool-1", "x"}); got != 2 {
		t.Errorf("unknown provider exit = %d, want 2", got)
	}
}

// Deleting a pool is idempotent, like every other delete here.
func TestStoragePoolDelete(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.StoragePools().Create(ctx, &apiv1.StoragePool{NodeName: "node-1", StoragePoolName: "data"})
	})

	if got := app.Run(t.Context(), []string{"sp", "d", "node-1", "data"}); got != 0 {
		t.Fatalf("delete exit = %d (stderr: %s)", got, errBuf.String())
	}

	if got := app.Run(t.Context(), []string{"sp", "d", "node-1", "data"}); got != 0 {
		t.Errorf("second delete exit = %d, want 0 (idempotent)", got)
	}
}

// A volume group is a per-volume template inside a resource group.
// Creating one and listing it back is how an operator sets defaults
// for every resource the group will later spawn.
func TestVolumeGroupCreateAndList(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "grp"})
	})

	if got := app.Run(t.Context(), []string{"vg", "c", "grp"}); got != 0 {
		t.Fatalf("create exit = %d (stderr: %s)", got, errBuf.String())
	}

	if got := app.Run(t.Context(), []string{"vg", "l", "--resource-group", "grp"}); got != 0 {
		t.Fatalf("list exit = %d (stderr: %s)", got, errBuf.String())
	}

	if !strings.Contains(out.String(), "grp") {
		t.Errorf("volume-group listing is missing the group:\n%s", out.String())
	}

	// A second create allocates the next volume number rather than
	// clobbering the first template.
	if got := app.Run(t.Context(), []string{"vg", "c", "grp"}); got != 0 {
		t.Fatalf("second create exit = %d (stderr: %s)", got, errBuf.String())
	}

	group, err := appStore(t, app).ResourceGroups().Get(t.Context(), "grp")
	if err != nil {
		t.Fatalf("get group: %v", err)
	}

	if len(group.VolumeGroups) != 2 {
		t.Fatalf("got %d volume groups, want 2: %+v", len(group.VolumeGroups), group.VolumeGroups)
	}

	if group.VolumeGroups[0].VolumeNumber == group.VolumeGroups[1].VolumeNumber {
		t.Errorf("second create reused volume number %d", group.VolumeGroups[0].VolumeNumber)
	}
}
