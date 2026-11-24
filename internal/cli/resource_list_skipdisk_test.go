// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

func inUse(b bool) *bool { return &b }

// seedLostDisk is a diskful replica the satellite reports Diskless
// after an I/O detach, next to a healthy peer, on a definition that
// carries SkipDisk.
func seedLostDisk(ctx context.Context, backend store.Store) {
	_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "pvc-x",
		Props: map[string]string{"DrbdOptions/SkipDisk": "True"},
	})

	for node, disk := range map[string]string{"node-1": "Diskless", "node-2": "UpToDate"} {
		_ = backend.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-x", NodeName: node,
			Volumes: []apiv1.Volume{{VolumeNumber: 0, State: apiv1.VolumeState{DiskState: disk}}},
		})
	}

	_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-y"})
	_ = backend.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-y", NodeName: "node-1",
		Volumes: []apiv1.Volume{{VolumeNumber: 0, State: apiv1.VolumeState{DiskState: "UpToDate"}}},
	})
}

// A diskful replica that lost its disk is what `--faulty` exists to
// find; a SkipDisk set on its definition reaches the table and is
// explained on stderr, leaving stdout parseable.
func TestResourceListFaultyFindsALostDisk(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, seedLostDisk)

	if got := app.Run(t.Context(), []string{"r", "l", "--faulty", "--pastable"}); got != 0 {
		t.Fatalf("exit = %d (stderr: %s)", got, errBuf.String())
	}

	if !strings.Contains(out.String(), "Diskless, SkipDisk (RD)") {
		t.Errorf("--faulty did not list the replica that lost its disk:\n%s", out.String())
	}

	if strings.Contains(out.String(), "pvc-y") {
		t.Errorf("--faulty listed a healthy definition:\n%s", out.String())
	}

	if strings.Contains(out.String(), "DrbdOptions/SkipDisk is set") {
		t.Errorf("the SkipDisk help landed on stdout:\n%s", out.String())
	}

	if !strings.Contains(errBuf.String(), "DrbdOptions/SkipDisk is set") {
		t.Errorf("no SkipDisk help on stderr: %q", errBuf.String())
	}
}

// `volume list` shows the same SkipDisk marker.
func TestVolumeListShowsSkipDisk(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, seedLostDisk)

	if got := app.Run(t.Context(), []string{"v", "l", "--pastable"}); got != 0 {
		t.Fatalf("exit = %d (stderr: %s)", got, errBuf.String())
	}

	if strings.Count(out.String(), "SkipDisk (RD)") != 2 {
		t.Errorf("want the marker on both replicas of pvc-x:\n%s", out.String())
	}

	if !strings.Contains(errBuf.String(), "DrbdOptions/SkipDisk is set") {
		t.Errorf("no SkipDisk help on stderr: %q", errBuf.String())
	}
}

// A listing with no SkipDisk prints no help.
func TestResourceListNoSkipDiskNoHelp(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedSplitBrain)

	if got := app.Run(t.Context(), []string{"r", "l"}); got != 0 {
		t.Fatalf("exit = %d (stderr: %s)", got, errBuf.String())
	}

	if strings.Contains(errBuf.String(), "SkipDisk") {
		t.Errorf("SkipDisk help without a SkipDisk: %q", errBuf.String())
	}
}

// Two primaries are counted over the whole listing, not over the rows
// --faulty keeps: the faulty one of two stays yellow.
func TestResourceListFaultyCountsEveryPrimary(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})

		for node, disk := range map[string]string{"node-1": "Outdated", "node-2": "UpToDate"} {
			_ = backend.Resources().Create(ctx, &apiv1.Resource{
				Name: "pvc-x", NodeName: node,
				State:   apiv1.ResourceState{InUse: inUse(true)},
				Volumes: []apiv1.Volume{{VolumeNumber: 0, State: apiv1.VolumeState{DiskState: disk}}},
			})
		}
	})

	if got := app.Run(t.Context(), []string{"r", "l", "--faulty", "--color", "always"}); got != 0 {
		t.Fatalf("exit = %d (stderr: %s)", got, errBuf.String())
	}

	if strings.Contains(out.String(), "node-2") {
		t.Fatalf("--faulty kept the healthy primary:\n%s", out.String())
	}

	if !strings.Contains(out.String(), "\x1b[33mInUse") {
		t.Errorf("the faulty one of two primaries is not yellow:\n%q", out.String())
	}
}
