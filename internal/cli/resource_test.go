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

	"github.com/cozystack/blockstor/internal/cli"
)

func seedDiskful(ctx context.Context, backend store.Store) {
	_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})
	_ = backend.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-x", NodeName: "node-1"})
}

func getResource(t *testing.T, app *cli.App, node string) apiv1.Resource {
	t.Helper()

	res, err := appStore(t, app).Resources().Get(t.Context(), "pvc-x", node)
	if err != nil {
		t.Fatalf("get resource on %s: %v", node, err)
	}

	return res
}

// `resource toggle-disk --diskless <node> <rd>` demotes a replica to a
// storage-free witness, and the pool-bearing form promotes it back.
// The DISKLESS flag is what the satellite reconciles on, so the exact
// flag edit matters more than any message.
func TestResourceToggleDisk(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedDiskful)

	if got := app.Run(t.Context(), []string{"r", "td", "--diskless", "node-1", "pvc-x"}); got != 0 {
		t.Fatalf("toggle to diskless exit = %d (stderr: %s)", got, errBuf.String())
	}

	if !containsFlag(getResource(t, app, "node-1").Flags, apiv1.ResourceFlagDiskless) {
		t.Fatal("--diskless did not set the DISKLESS flag")
	}

	// Forcing diskless again is a no-op, not a flip back.
	if got := app.Run(t.Context(), []string{"r", "td", "--diskless", "node-1", "pvc-x"}); got != 0 {
		t.Fatalf("second toggle exit = %d", got)
	}

	if !containsFlag(getResource(t, app, "node-1").Flags, apiv1.ResourceFlagDiskless) {
		t.Error("--diskless is not idempotent: the flag was cleared")
	}

	if got := app.Run(t.Context(), []string{"r", "td", "--storage-pool", "data", "node-1", "pvc-x"}); got != 0 {
		t.Fatalf("promote exit = %d (stderr: %s)", got, errBuf.String())
	}

	promoted := getResource(t, app, "node-1")
	if containsFlag(promoted.Flags, apiv1.ResourceFlagDiskless) {
		t.Error("promotion left the DISKLESS flag set")
	}

	if promoted.Props["StorPoolName"] != "data" {
		t.Errorf("StorPoolName = %q, want data", promoted.Props["StorPoolName"])
	}
}

// Promoting a witness must clear TIE_BREAKER as well as DISKLESS. A
// diskful replica still carrying TIE_BREAKER is counted as a witness
// by the tiebreaker reconciler, which then double-counts the slot.
func TestResourceToggleDiskClearsTieBreaker(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})
		_ = backend.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-x", NodeName: "node-1",
			Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
		})
	})

	if got := app.Run(t.Context(), []string{"r", "td", "--storage-pool", "data", "node-1", "pvc-x"}); got != 0 {
		t.Fatalf("promote exit = %d (stderr: %s)", got, errBuf.String())
	}

	flags := getResource(t, app, "node-1").Flags
	if containsFlag(flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("promoted witness still carries TIE_BREAKER: %v", flags)
	}
}

// seedTwoDiskful gives the definition a second data-bearing replica,
// so demoting one of them is a legitimate move rather than the
// last-copy deletion the guard refuses.
func seedTwoDiskful(ctx context.Context, backend store.Store) {
	seedDiskful(ctx, backend)
	_ = backend.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-x", NodeName: "node-2"})
}

// The bare form flips whatever the replica currently is, as long as
// the definition keeps a copy of the data.
func TestResourceToggleDiskFlips(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedTwoDiskful)

	if got := app.Run(t.Context(), []string{"r", "td", "node-1", "pvc-x"}); got != 0 {
		t.Fatalf("toggle exit = %d (stderr: %s)", got, errBuf.String())
	}

	if !containsFlag(getResource(t, app, "node-1").Flags, apiv1.ResourceFlagDiskless) {
		t.Error("a bare toggle on a diskful replica did not demote it")
	}
}

// TestResourceToggleDiskRefusesTheLastDiskful: the satellite reconciles
// DISKLESS by deleting the backing volume, so demoting the only
// data-bearing replica destroys the data. Upstream LINSTOR refuses the
// same move; this CLI writes the CRD directly, so the guard has to be
// here.
func TestResourceToggleDiskRefusesTheLastDiskful(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedDiskful)

	if got := app.Run(t.Context(), []string{"r", "td", "node-1", "pvc-x"}); got == 0 {
		t.Fatalf("demoting the last diskful replica exited 0; want a refusal")
	}

	if msg := errBuf.String(); !strings.Contains(msg, "last diskful replica") {
		t.Errorf("refusal does not name the reason: %s", msg)
	}

	if containsFlag(getResource(t, app, "node-1").Flags, apiv1.ResourceFlagDiskless) {
		t.Error("a refused demotion still flipped the flag")
	}
}

// --force is the documented override: an operator who means it can
// still take the last copy down.
func TestResourceToggleDiskForceDemotesTheLastDiskful(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedDiskful)

	if got := app.Run(t.Context(), []string{"r", "td", "--force", "node-1", "pvc-x"}); got != 0 {
		t.Fatalf("forced demotion exit = %d (stderr: %s)", got, errBuf.String())
	}

	if !containsFlag(getResource(t, app, "node-1").Flags, apiv1.ResourceFlagDiskless) {
		t.Error("--force did not demote the replica")
	}
}

// A replica a consumer still has open is refused too: demoting it
// detaches DRBD underneath the mount.
func TestResourceToggleDiskRefusesAnInUseReplica(t *testing.T) {
	t.Parallel()

	inUse := true

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedTwoDiskful(ctx, backend)
		res, err := backend.Resources().Get(ctx, "pvc-x", "node-1")
		if err != nil {
			return
		}

		res.State.InUse = &inUse
		_ = backend.Resources().Update(ctx, &res)
	})

	if got := app.Run(t.Context(), []string{"r", "td", "node-1", "pvc-x"}); got == 0 {
		t.Fatalf("demoting an in-use replica exited 0; want a refusal")
	}

	if msg := errBuf.String(); !strings.Contains(msg, "in use") {
		t.Errorf("refusal does not name the reason: %s", msg)
	}
}

// `--migrate-from <src>` is add-before-drop: the destination is
// promoted and stamped with the source, and the source replica STAYS
// until the controller sees the copy is durable. Deleting it here
// would drop redundancy for the length of the resync.
func TestResourceToggleDiskMigrateFrom(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedDiskful)

	argv := []string{"r", "td", "node-2", "pvc-x", "--storage-pool", "data", "--migrate-from", "node-1"}
	if got := app.Run(t.Context(), argv); got != 0 {
		t.Fatalf("migrate exit = %d (stderr: %s)", got, errBuf.String())
	}

	dst := getResource(t, app, "node-2")
	if dst.Props["BlockstorMigratingFrom"] != "node-1" {
		t.Errorf("destination props = %v, want BlockstorMigratingFrom=node-1", dst.Props)
	}

	if containsFlag(dst.Flags, apiv1.ResourceFlagDiskless) {
		t.Error("migration destination was left diskless")
	}

	if _, err := appStore(t, app).Resources().Get(t.Context(), "pvc-x", "node-1"); err != nil {
		t.Errorf("the source replica was removed before the copy was durable: %v", err)
	}
}

// Migrating off a diskless source has nothing to copy, so it is
// refused rather than silently creating an empty replica.
func TestResourceToggleDiskMigrateFromDisklessSource(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})
		_ = backend.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-x", NodeName: "node-1", Flags: []string{apiv1.ResourceFlagDiskless},
		})
	})

	argv := []string{"r", "td", "node-2", "pvc-x", "--storage-pool", "data", "--migrate-from", "node-1"}
	if got := app.Run(t.Context(), argv); got == 0 {
		t.Error("migrating off a diskless source succeeded")
	}
}

// `--cancel` asks the satellite to unwind an in-flight conversion. It
// must NOT also flip DISKLESS: the reconciler flips it itself once the
// rollback has actually completed, so an observer sees the old state
// return only after the storage is really back.
func TestResourceToggleDiskCancel(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedDiskful)

	if got := app.Run(t.Context(), []string{"r", "td", "--cancel", "node-1", "pvc-x"}); got != 0 {
		t.Fatalf("cancel exit = %d (stderr: %s)", got, errBuf.String())
	}

	res := getResource(t, app, "node-1")
	if !res.ToggleDiskCancel {
		t.Error("--cancel did not request the rollback")
	}

	if containsFlag(res.Flags, apiv1.ResourceFlagDiskless) {
		t.Error("--cancel flipped DISKLESS itself instead of leaving it to the reconciler")
	}
}

// activate / deactivate toggle the INACTIVE flag.
func TestResourceActivateDeactivate(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedDiskful)

	if got := app.Run(t.Context(), []string{"r", "deactivate", "node-1", "pvc-x"}); got != 0 {
		t.Fatalf("deactivate exit = %d (stderr: %s)", got, errBuf.String())
	}

	if !containsFlag(getResource(t, app, "node-1").Flags, apiv1.ResourceFlagInactive) {
		t.Fatal("deactivate did not set INACTIVE")
	}

	if got := app.Run(t.Context(), []string{"r", "activate", "node-1", "pvc-x"}); got != 0 {
		t.Fatalf("activate exit = %d (stderr: %s)", got, errBuf.String())
	}

	if containsFlag(getResource(t, app, "node-1").Flags, apiv1.ResourceFlagInactive) {
		t.Error("activate did not clear INACTIVE")
	}
}

// `resource list-volumes` renders the per-volume view, filtered the
// same way `resource list` is.
func TestResourceListVolumes(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})
		_ = backend.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-x", NodeName: "node-1",
			Volumes: []apiv1.Volume{{VolumeNumber: 0, StoragePool: "data", DevicePath: "/dev/drbd1000"}},
		})
	})

	if got := app.Run(t.Context(), []string{"r", "lv"}); got != 0 {
		t.Fatalf("list-volumes exit = %d (stderr: %s)", got, errBuf.String())
	}

	if !strings.Contains(out.String(), "pvc-x") {
		t.Errorf("list-volumes is missing the resource:\n%s", out.String())
	}
}
