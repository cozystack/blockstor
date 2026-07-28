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

// seedSplitBrain is a replica whose local disk is UpToDate but whose
// peer link is down — the split-brain an operator hunts with --faulty.
func seedSplitBrain(ctx context.Context, backend store.Store) {
	_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})
	_ = backend.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-x", NodeName: "node-1",
		Volumes: []apiv1.Volume{{VolumeNumber: 0, State: apiv1.VolumeState{DiskState: "UpToDate"}}},
		LayerObject: &apiv1.ResourceLayer{
			Type: apiv1.LayerKindDRBD,
			Drbd: &apiv1.DrbdResourceLayer{
				Connections: map[string]apiv1.DrbdConnection{
					"node-2": {Connected: false, Message: "StandAlone"},
				},
			},
		},
	})
	_ = backend.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-x", NodeName: "node-9",
		Volumes: []apiv1.Volume{{VolumeNumber: 0, State: apiv1.VolumeState{DiskState: "UpToDate"}}},
		LayerObject: &apiv1.ResourceLayer{
			Type: apiv1.LayerKindDRBD,
			Drbd: &apiv1.DrbdResourceLayer{
				Connections: map[string]apiv1.DrbdConnection{"node-1": {Connected: true}},
			},
		},
	})
}

// A replica with a healthy local disk but a StandAlone peer is faulty.
// Judging on DiskState alone hides exactly the case the troubleshooting
// runbooks tell an operator to find this way.
func TestFaultyCatchesBrokenConnection(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, seedSplitBrain)

	if got := app.Run(t.Context(), []string{"r", "l", "--faulty"}); got != 0 {
		t.Fatalf("exit = %d (stderr: %s)", got, errBuf.String())
	}

	if !strings.Contains(out.String(), "node-1") {
		t.Errorf("--faulty dropped a replica with a down peer link:\n%s", out.String())
	}

	if strings.Contains(out.String(), "node-9") {
		t.Errorf("--faulty kept a fully healthy replica:\n%s", out.String())
	}
}

// --faulty is a filter, not a rendering choice: `-m` skips the
// renderer, so applying it there would return every replica for the
// one command whose purpose is to narrow to the broken ones.
func TestFaultyAppliesInMachineMode(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, seedSplitBrain)

	if got := app.Run(t.Context(), []string{"r", "l", "--faulty", "-m"}); got != 0 {
		t.Fatalf("exit = %d (stderr: %s)", got, errBuf.String())
	}

	if strings.Contains(out.String(), "node-9") {
		t.Errorf("-m ignored --faulty and returned healthy replicas:\n%s", out.String())
	}

	if !strings.Contains(out.String(), "node-1") {
		t.Errorf("-m --faulty lost the broken replica:\n%s", out.String())
	}
}

// A cell must never contain a newline: the pipe layout is a parsing
// contract, and an embedded newline splits the row mid-cell so
// `awk -F'|'` reads two malformed lines.
func TestNoCellBreaksTheRowLayout(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
			Name: "grp",
			SelectFilter: apiv1.AutoSelectFilter{
				PlaceCount: 3, StoragePool: "data", LayerStack: []string{"DRBD", "STORAGE"},
			},
		})
	})

	if got := app.Run(t.Context(), []string{"rg", "l"}); got != 0 {
		t.Fatalf("exit = %d (stderr: %s)", got, errBuf.String())
	}

	for _, line := range strings.Split(strings.TrimRight(out.String(), "\n"), "\n") {
		if !strings.HasPrefix(line, "|") && !strings.HasPrefix(line, "+") {
			t.Errorf("row layout broken by a multi-line cell:\n%s", out.String())

			break
		}
	}
}

// A flag that is parsed and then ignored is worse than one refused:
// `r l -o json | jq` would otherwise receive a human table with exit 0
// and fail somewhere else entirely.
func TestOutputFormatIsHonouredOrRefused(t *testing.T) {
	t.Parallel()

	app, out, _ := newApp(t, seedResource)

	if got := app.Run(t.Context(), []string{"r", "l", "-o", "json"}); got != 0 {
		t.Fatalf("-o json exit = %d", got)
	}

	if !strings.HasPrefix(strings.TrimSpace(out.String()), "[[") {
		t.Errorf("-o json printed a table:\n%s", out.String())
	}

	for _, bad := range [][]string{
		{"r", "l", "-o", "yaml"},
		{"r", "l", "--output-version", "v0"},
	} {
		rejecting, _, _ := newApp(t, seedResource)
		if got := rejecting.Run(t.Context(), bad); got != 2 {
			t.Errorf("%v exit = %d, want 2", bad, got)
		}
	}
}

// `sp l --storage-pools X` must actually narrow to X.
func TestStoragePoolListFiltersByPool(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.StoragePools().Create(ctx, &apiv1.StoragePool{NodeName: "node-1", StoragePoolName: "data"})
		_ = backend.StoragePools().Create(ctx, &apiv1.StoragePool{NodeName: "node-1", StoragePoolName: "other"})
	})

	if got := app.Run(t.Context(), []string{"sp", "l", "--storage-pools", "data"}); got != 0 {
		t.Fatalf("exit = %d (stderr: %s)", got, errBuf.String())
	}

	if strings.Contains(out.String(), "other") {
		t.Errorf("--storage-pools did not filter:\n%s", out.String())
	}
}

// `--pastable` drops the pipes an operator would otherwise have to
// strip by hand.
func TestPastableDropsTheBorders(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, seedResource)

	if got := app.Run(t.Context(), []string{"r", "l", "--pastable"}); got != 0 {
		t.Fatalf("exit = %d (stderr: %s)", got, errBuf.String())
	}

	if strings.Contains(out.String(), "|") || strings.Contains(out.String(), "+--") {
		t.Errorf("--pastable still drew the box:\n%s", out.String())
	}

	if !strings.Contains(out.String(), "pvc-x") {
		t.Errorf("--pastable lost the data:\n%s", out.String())
	}
}

// An extra positional after the key must not turn a delete into a set
// of the very key the operator wanted gone.
func TestDeletePropertyIgnoresAStrayValue(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.Nodes().Create(ctx, &apiv1.Node{
			Name: "node-1", Props: map[string]string{"Aux/probe": "v"},
		})
	})

	if got := app.Run(t.Context(), []string{"n", "dp", "node-1", "Aux/probe", "oops"}); got != 0 {
		t.Fatalf("exit = %d (stderr: %s)", got, errBuf.String())
	}

	if got := app.Run(t.Context(), []string{"n", "lp", "node-1"}); got != 0 {
		t.Fatalf("list exit = %d", got)
	}

	if strings.Contains(out.String(), "Aux/probe") {
		t.Errorf("delete-property set the key instead of removing it:\n%s", out.String())
	}
}

// `--force=false` must DISABLE force, and a bool flag given a
// non-boolean value is a rejection rather than a silently dropped one.
func TestBoolFlagsWithInlineValues(t *testing.T) {
	t.Parallel()

	seed := func(ctx context.Context, backend store.Store) {
		seedDefinition(ctx, backend)
		_ = backend.VolumeDefinitions().Create(ctx, "pvc-x", &apiv1.VolumeDefinition{
			VolumeNumber: 0, SizeKib: 2 << 20,
		})
	}

	app, _, _ := newApp(t, seed)
	if got := app.Run(t.Context(), []string{"vd", "s", "pvc-x", "0", "1G", "--force=false"}); got == 0 {
		t.Error("--force=false enabled force")
	}

	dropped, _, _ := newApp(t, nil)
	if got := dropped.Run(t.Context(), []string{"encryption", "create-passphrase", "-p=secret"}); got != 2 {
		t.Errorf("-p=secret exit = %d, want 2 rather than a silently dropped value", got)
	}
}
