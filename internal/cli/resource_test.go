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

// A diskful replica the satellite can bind needs Props["StorPoolName"];
// without it the reconciler fails with `unknown storage pool ""` and the
// replica sits in Provisioning for good. The REST path resolves an
// omitted pool from the resource group, and this CLI writes the CRD
// directly, so it has to resolve it too.
func TestResourceCreateResolvesThePoolFromTheGroup(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
			Name: "rg-1",
			SelectFilter: apiv1.AutoSelectFilter{
				// Where linstor-csi lands the storage class's pool.
				StoragePoolList: []string{"data"},
			},
		})
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name:              "pvc-x",
			ResourceGroupName: "rg-1",
		})
	})

	if got := app.Run(t.Context(), []string{"r", "c", "node-1", "pvc-x"}); got != 0 {
		t.Fatalf("resource create exit = %d (stderr: %s)", got, errBuf.String())
	}

	if pool := getResource(t, app, "node-1").Props["StorPoolName"]; pool != "data" {
		t.Errorf("StorPoolName = %q, want data resolved from the group", pool)
	}
}

// A sibling that already has a pool answers before the group does, so a
// replica added to a live definition lands on the same backend.
func TestResourceCreateInheritsThePoolFromASibling(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})
		_ = backend.Resources().Create(ctx, &apiv1.Resource{
			Name:     "pvc-x",
			NodeName: "node-1",
			Props:    map[string]string{"StorPoolName": "tank"},
		})
	})

	if got := app.Run(t.Context(), []string{"r", "c", "node-2", "pvc-x"}); got != 0 {
		t.Fatalf("resource create exit = %d (stderr: %s)", got, errBuf.String())
	}

	if pool := getResource(t, app, "node-2").Props["StorPoolName"]; pool != "tank" {
		t.Errorf("StorPoolName = %q, want tank inherited from the sibling", pool)
	}
}

// --diskless says the replica has no storage on purpose, so nothing
// should be stamped on it.
func TestResourceCreateDisklessKeepsNoPool(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
			Name:         "rg-1",
			SelectFilter: apiv1.AutoSelectFilter{StoragePoolList: []string{"data"}},
		})
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name:              "pvc-x",
			ResourceGroupName: "rg-1",
		})
	})

	if got := app.Run(t.Context(), []string{"r", "c", "--diskless", "node-1", "pvc-x"}); got != 0 {
		t.Fatalf("resource create exit = %d (stderr: %s)", got, errBuf.String())
	}

	if pool := getResource(t, app, "node-1").Props["StorPoolName"]; pool != "" {
		t.Errorf("StorPoolName = %q on a diskless replica, want empty", pool)
	}
}

// Promotion gives a replica storage, so the same resolution applies:
// flipping the flag without a pool wedges the satellite.
func TestResourceToggleDiskPromotionResolvesThePool(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
			Name:         "rg-1",
			SelectFilter: apiv1.AutoSelectFilter{StoragePoolList: []string{"data"}},
		})
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name:              "pvc-x",
			ResourceGroupName: "rg-1",
		})
		_ = backend.Resources().Create(ctx, &apiv1.Resource{
			Name:     "pvc-x",
			NodeName: "node-1",
			Flags:    []string{apiv1.ResourceFlagDiskless},
		})
	})

	if got := app.Run(t.Context(), []string{"r", "td", "node-1", "pvc-x"}); got != 0 {
		t.Fatalf("promotion exit = %d (stderr: %s)", got, errBuf.String())
	}

	if pool := getResource(t, app, "node-1").Props["StorPoolName"]; pool != "data" {
		t.Errorf("StorPoolName = %q after promotion, want data", pool)
	}
}

// A negative volume number reaches the CRD, the satellite derives a
// device node from it, and the replica hangs waiting for a DRBD-ID that
// can never be allocated. REST rejects it; this path has to as well.
func TestVolumeDefinitionCreateRefusesANegativeNumber(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})
	})

	if got := app.Run(t.Context(), []string{"vd", "c", "pvc-x", "1G", "--vlmnr", "-3"}); got == 0 {
		t.Fatal("a negative volume number was accepted; want a refusal")
	}
}

// The upper bound is DRBD-9's addressable range, matching REST.
func TestVolumeDefinitionCreateRefusesAnOversizeNumber(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})
	})

	if got := app.Run(t.Context(), []string{"vd", "c", "pvc-x", "1G", "--vlmnr", "70000"}); got == 0 {
		t.Fatal("a volume number past the addressable range was accepted; want a refusal")
	}
}

// The size spellings an operator actually types. Rejecting one casing
// of a suffix is a papercut; reading a byte count as KiB is a volume
// 1024x too large.
func TestParseSizeSpellings(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})
	})

	// 16GiB in every casing the docs and the shell produce.
	for i, spelling := range []string{"16GiB", "16gib", "16Gib", "16Gi", "16G"} {
		argv := []string{"vd", "c", "pvc-x", spelling, "--vlmnr", string(rune('0' + i))}
		if got := app.Run(t.Context(), argv); got != 0 {
			t.Errorf("%s: exit = %d (stderr: %s)", spelling, got, errBuf.String())
		}
	}
}

// 512 bytes is not a whole KiB, and rounding a size silently is worse
// than refusing it.
func TestParseSizeRefusesAPartialKib(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})
	})

	if got := app.Run(t.Context(), []string{"vd", "c", "pvc-x", "512B"}); got == 0 {
		t.Fatal("512B was accepted; it is not a whole number of KiB")
	}
}

// A place count of zero or less places nothing, so reporting success
// hands back a volume with no replicas.
func TestPlaceCountRefusesZeroOrLess(t *testing.T) {
	t.Parallel()

	for _, count := range []string{"0", "-2"} {
		app, _, _ := newApp(t, func(ctx context.Context, backend store.Store) {
			_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})
		})

		if got := app.Run(t.Context(), []string{"rd", "ap", "pvc-x", "--place-count", count}); got == 0 {
			t.Errorf("--place-count %s was accepted as a no-op; want a refusal", count)
		}
	}
}

// An out-of-range port silently truncated on the int32 cast, so the
// node was created pointing at a port nobody listens on.
func TestNodeCreateRefusesAnOutOfRangePort(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, nil)

	argv := []string{"n", "c", "node-1", "10.0.0.1", "--port", "4294970000"}
	if got := app.Run(t.Context(), argv); got == 0 {
		t.Fatal("an out-of-range port was accepted")
	}
}

// A malformed address leaves a node that never comes online.
func TestNodeCreateRefusesAMalformedAddress(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, nil)

	if got := app.Run(t.Context(), []string{"n", "c", "node-1", "10.0.0.1 ; rm"}); got == 0 {
		t.Fatal("a malformed address was accepted")
	}
}
