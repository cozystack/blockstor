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

func seedDefinition(ctx context.Context, backend store.Store) {
	_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})
}

func seedVolumeDefinition(ctx context.Context, backend store.Store) {
	seedDefinition(ctx, backend)
	_ = backend.VolumeDefinitions().Create(ctx, "pvc-x", &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024})
}

// Volume sizes are written the way operators write them — `10G`,
// `100M`, `2T` — and the scripts in this repo use exactly those
// spellings. Storing the wrong magnitude here would silently
// provision a volume a thousand times too small.
func TestVolumeDefinitionCreateParsesSizes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		size    string
		wantKib int64
	}{
		{"100M", 102400},
		{"1G", 1 << 20},
		{"2T", 2 << 30},
		{"32M", 32768},
		{"1024K", 1024},
	}

	for _, tc := range cases {
		app, _, errBuf := newApp(t, seedDefinition)

		if got := app.Run(t.Context(), []string{"vd", "c", "pvc-x", tc.size}); got != 0 {
			t.Fatalf("create %s exit = %d (stderr: %s)", tc.size, got, errBuf.String())
		}

		backend := appStore(t, app)

		vds, err := backend.VolumeDefinitions().List(t.Context(), "pvc-x")
		if err != nil {
			t.Fatalf("list: %v", err)
		}

		if len(vds) != 1 {
			t.Fatalf("%s: got %d volume definitions, want 1", tc.size, len(vds))
		}

		if vds[0].SizeKib != tc.wantKib {
			t.Errorf("size %q stored as %d KiB, want %d", tc.size, vds[0].SizeKib, tc.wantKib)
		}
	}
}

// `volume-definition delete` takes the volume number after a `--`
// separator in this repo's scripts, so a number that looks like a flag
// still reaches the handler.
func TestVolumeDefinitionDeleteAfterSeparator(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedDefinition(ctx, backend)
		_ = backend.VolumeDefinitions().Create(ctx, "pvc-x", &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024})
	})

	if got := app.Run(t.Context(), []string{"vd", "d", "pvc-x", "--", "0"}); got != 0 {
		t.Fatalf("delete exit = %d (stderr: %s)", got, errBuf.String())
	}

	vds, err := appStore(t, app).VolumeDefinitions().List(t.Context(), "pvc-x")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(vds) != 0 {
		t.Errorf("volume definition survived the delete: %+v", vds)
	}
}

// `resource create <node> <rd>` places one replica; the node is a
// leading positional, as in every script that calls it.
func TestResourceCreateAndDelete(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedDefinition)

	if got := app.Run(t.Context(), []string{"r", "c", "node-1", "pvc-x"}); got != 0 {
		t.Fatalf("create exit = %d (stderr: %s)", got, errBuf.String())
	}

	resources, err := appStore(t, app).Resources().List(t.Context())
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(resources) != 1 || resources[0].NodeName != "node-1" || resources[0].Name != "pvc-x" {
		t.Fatalf("created replica = %+v, want pvc-x on node-1", resources)
	}

	if got := app.Run(t.Context(), []string{"r", "d", "node-1", "pvc-x"}); got != 0 {
		t.Fatalf("delete exit = %d (stderr: %s)", got, errBuf.String())
	}

	resources, err = appStore(t, app).Resources().List(t.Context())
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(resources) != 0 {
		t.Errorf("replica survived the delete: %+v", resources)
	}
}

// `resource create --diskless` marks the replica so it never carves
// local storage.
func TestResourceCreateDiskless(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedDefinition)

	if got := app.Run(t.Context(), []string{"r", "c", "--diskless", "node-2", "pvc-x"}); got != 0 {
		t.Fatalf("create exit = %d (stderr: %s)", got, errBuf.String())
	}

	resources, err := appStore(t, app).Resources().List(t.Context())
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(resources) != 1 {
		t.Fatalf("got %d replicas, want 1", len(resources))
	}

	if !containsFlag(resources[0].Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("replica flags = %v, want DISKLESS", resources[0].Flags)
	}
}

// A node is created with its address and type, the shape the install
// scripts use.
func TestNodeCreateAndDelete(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, nil)

	if got := app.Run(t.Context(), []string{"n", "c", "node-1", "10.0.0.1", "--node-type", "Satellite"}); got != 0 {
		t.Fatalf("create exit = %d (stderr: %s)", got, errBuf.String())
	}

	if got := app.Run(t.Context(), []string{"n", "l"}); got != 0 {
		t.Fatalf("list exit = %d", got)
	}

	for _, want := range []string{"node-1", "10.0.0.1"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("node listing is missing %q:\n%s", want, out.String())
		}
	}

	if got := app.Run(t.Context(), []string{"n", "d", "node-1"}); got != 0 {
		t.Fatalf("delete exit = %d (stderr: %s)", got, errBuf.String())
	}
}

// Snapshots are created with the definition and snapshot names, with
// the target nodes as optional LEADING positionals.
func TestSnapshotCreateAndDelete(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, seedDefinition)

	if got := app.Run(t.Context(), []string{"s", "c", "node-1", "pvc-x", "snap-1"}); got != 0 {
		t.Fatalf("create exit = %d (stderr: %s)", got, errBuf.String())
	}

	if got := app.Run(t.Context(), []string{"s", "l"}); got != 0 {
		t.Fatalf("list exit = %d", got)
	}

	if !strings.Contains(out.String(), "snap-1") {
		t.Errorf("snapshot listing is missing the snapshot:\n%s", out.String())
	}

	if got := app.Run(t.Context(), []string{"s", "d", "pvc-x", "snap-1"}); got != 0 {
		t.Fatalf("delete exit = %d (stderr: %s)", got, errBuf.String())
	}

	// Deleting an absent snapshot is a success, matching the upstream
	// idempotence this repo pins.
	if got := app.Run(t.Context(), []string{"s", "d", "pvc-x", "snap-1"}); got != 0 {
		t.Errorf("second delete exit = %d, want 0 (idempotent)", got)
	}
}

// A resource group is created with its placement policy and shows up
// in the listing.
func TestResourceGroupCreate(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, nil)

	if got := app.Run(t.Context(), []string{"rg", "c", "sc-1", "--place-count", "3", "--storage-pool", "data"}); got != 0 {
		t.Fatalf("create exit = %d (stderr: %s)", got, errBuf.String())
	}

	if got := app.Run(t.Context(), []string{"rg", "l"}); got != 0 {
		t.Fatalf("list exit = %d", got)
	}

	for _, want := range []string{"sc-1", "PlaceCount: 3", "data"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("group listing is missing %q:\n%s", want, out.String())
		}
	}
}

func containsFlag(flags []string, want string) bool {
	for _, flag := range flags {
		if flag == want {
			return true
		}
	}

	return false
}

// A shrink is refused: nothing here shrinks the filesystem first, so
// handing the block device a smaller size under a live filesystem
// truncates it. --force is the operator stating they already shrank
// the filesystem themselves.
func TestVolumeDefinitionSetSizeRefusesShrink(t *testing.T) {
	t.Parallel()

	seed := func(ctx context.Context, backend store.Store) {
		seedDefinition(ctx, backend)
		_ = backend.VolumeDefinitions().Create(ctx, "pvc-x", &apiv1.VolumeDefinition{
			VolumeNumber: 0, SizeKib: 2 << 20, // 2 GiB
		})
	}

	app, _, errBuf := newApp(t, seed)

	if got := app.Run(t.Context(), []string{"vd", "s", "pvc-x", "0", "1G"}); got == 0 {
		t.Fatal("a shrink was accepted")
	}

	if !strings.Contains(errBuf.String(), "--force") {
		t.Errorf("the refusal does not say how to proceed deliberately:\n%s", errBuf.String())
	}

	vds, err := appStore(t, app).VolumeDefinitions().List(t.Context(), "pvc-x")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if vds[0].SizeKib != 2<<20 {
		t.Errorf("the refused shrink still wrote %d KiB", vds[0].SizeKib)
	}

	forced, _, forcedErr := newApp(t, seed)
	if got := forced.Run(t.Context(), []string{"vd", "s", "pvc-x", "0", "1G", "--force"}); got != 0 {
		t.Errorf("--force did not allow the shrink: exit %d (%s)", got, forcedErr.String())
	}
}

// The size bounds hold even under --force. Below DRBD's per-device
// minimum the satellite loops on `drbdadm create-md` forever rather
// than failing, so a zero or 1 KiB size must never reach the spec.
func TestVolumeDefinitionSetSizeBounds(t *testing.T) {
	t.Parallel()

	for _, size := range []string{"1024", "1M", "17179869184M"} {
		app, _, _ := newApp(t, func(ctx context.Context, backend store.Store) {
			seedDefinition(ctx, backend)
			_ = backend.VolumeDefinitions().Create(ctx, "pvc-x", &apiv1.VolumeDefinition{
				VolumeNumber: 0, SizeKib: 1 << 20,
			})
		})

		if got := app.Run(t.Context(), []string{"vd", "s", "pvc-x", "0", size, "--force"}); got == 0 {
			t.Errorf("--force let %s past the size bounds", size)
		}
	}
}
