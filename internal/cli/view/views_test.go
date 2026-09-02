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

package view_test

import (
	"regexp"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"

	"github.com/cozystack/blockstor/internal/cli/view"
)

// `storage-pool list` carries two contracts from this repo's scripts:
// the CanSnapshots cell renders True/False (a test greps case-
// insensitively for True), and a pool whose backing store vanished
// must NOT report Ok — that regression is exactly what Bug 83's
// recovery test watches for.
func TestStoragePoolList(t *testing.T) {
	t.Parallel()

	tbl := view.StoragePoolList([]apiv1.StoragePool{
		{
			StoragePoolName: "data", NodeName: "node-1", ProviderKind: "ZFS",
			FreeCapacity: 1 << 20, TotalCapacity: 2 << 20, SupportsSnapshot: true,
		},
		{
			StoragePoolName: "gone", NodeName: "node-2", ProviderKind: "ZFS",
			PoolMissing: true,
		},
	})

	wantCols := []string{"StoragePool", "Node", "Driver", "PoolName", "FreeCapacity", "TotalCapacity", "CanSnapshots", "State"}
	assertColumns(t, tbl, wantCols)

	if got := cellAt(t, tbl, 0, "CanSnapshots"); got != "True" {
		t.Errorf("CanSnapshots = %q, want True", got)
	}

	if got := cellAt(t, tbl, 0, "State"); got != "Ok" {
		t.Errorf("healthy pool State = %q, want Ok", got)
	}

	missing := cellAt(t, tbl, 1, "State")
	if missing == "Ok" {
		t.Errorf("a pool whose backing store is missing reported %q — the operator would never notice", missing)
	}

	if got := cellAt(t, tbl, 1, "CanSnapshots"); got != "False" {
		t.Errorf("CanSnapshots = %q, want False", got)
	}
}

// Capacities render in the human units the volume-definition contract
// also greps for, rather than raw KiB.
func TestCapacityFormatting(t *testing.T) {
	t.Parallel()

	tbl := view.StoragePoolList([]apiv1.StoragePool{{
		StoragePoolName: "data", FreeCapacity: 1 << 20, TotalCapacity: 3 << 20,
	}})

	if got := cellAt(t, tbl, 0, "FreeCapacity"); got != "1 GiB" {
		t.Errorf("FreeCapacity = %q, want %q", got, "1 GiB")
	}

	if got := cellAt(t, tbl, 0, "TotalCapacity"); got != "3 GiB" {
		t.Errorf("TotalCapacity = %q, want %q", got, "3 GiB")
	}
}

// `resource-definition list` must show the layer stack: a script locates
// the row and asserts it mentions DRBD and STORAGE.
func TestResourceDefinitionList(t *testing.T) {
	t.Parallel()

	tbl := view.ResourceDefinitionList([]apiv1.ResourceDefinition{{
		Name:              "pvc-x",
		ResourceGroupName: "sc-1",
		LayerStack:        []string{"DRBD", "STORAGE"},
	}})

	assertColumns(t, tbl, []string{"ResourceName", "Port", "ResourceGroup", "Layers", "State"})

	layers := cellAt(t, tbl, 0, "Layers")
	for _, want := range []string{"DRBD", "STORAGE"} {
		if !strings.Contains(layers, want) {
			t.Errorf("Layers = %q, want it to mention %q", layers, want)
		}
	}

	if got := cellAt(t, tbl, 0, "State"); got != "Ok" {
		t.Errorf("State = %q, want Ok", got)
	}

	deleting := view.ResourceDefinitionList([]apiv1.ResourceDefinition{{
		Name: "pvc-y", Flags: []string{"DELETE"},
	}})

	if got := cellAt(t, deleting, 0, "State"); got != "DELETING" {
		t.Errorf("State for a DELETE-flagged definition = %q, want DELETING", got)
	}
}

// `volume-definition list` renders sizes in units a script matches with
// `MiB|GiB`; raw KiB would fail that assertion.
func TestVolumeDefinitionList(t *testing.T) {
	t.Parallel()

	tbl := view.VolumeDefinitionList("pvc-x", []apiv1.VolumeDefinition{
		{VolumeNumber: 0, SizeKib: 102400},
		{VolumeNumber: 1, SizeKib: 1 << 20},
	})

	assertColumns(t, tbl, []string{"ResourceName", "VolumeNr", "VolumeMinor", "Size", "Gross", "State"})

	unit := regexp.MustCompile(`(MiB|GiB|TiB)`)
	for row := range 2 {
		size := cellAt(t, tbl, row, "Size")
		if !unit.MatchString(size) {
			t.Errorf("row %d Size = %q, want a MiB/GiB/TiB unit", row, size)
		}
	}

	if got := cellAt(t, tbl, 0, "Size"); got != "100 MiB" {
		t.Errorf("Size = %q, want %q", got, "100 MiB")
	}
}

// `snapshot list` must contain the snapshot name — several scripts
// assert presence by grepping for it — and report per-node completion.
func TestSnapshotList(t *testing.T) {
	t.Parallel()

	tbl := view.SnapshotList([]apiv1.Snapshot{{
		Name:              "snap-1",
		ResourceName:      "pvc-x",
		Nodes:             []string{"node-1", "node-2"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1 << 20}},
	}})

	assertColumns(t, tbl, []string{"ResourceName", "SnapshotName", "NodeNames", "Volumes", "Created", "State"})

	if got := cellAt(t, tbl, 0, "SnapshotName"); got != "snap-1" {
		t.Errorf("SnapshotName = %q, want snap-1", got)
	}

	if got := cellAt(t, tbl, 0, "NodeNames"); got != "node-1,node-2" {
		t.Errorf("NodeNames = %q, want the node list", got)
	}

	if got := cellAt(t, tbl, 0, "State"); got != "Successful" {
		t.Errorf("State = %q, want Successful", got)
	}

	failed := view.SnapshotList([]apiv1.Snapshot{{
		Name: "snap-bad", ResourceName: "pvc-x", Flags: []string{"FAILED"},
	}})

	if got := cellAt(t, failed, 0, "State"); got != "Failed" {
		t.Errorf("State for a FAILED snapshot = %q, want Failed", got)
	}
}

// `resource-group list` must contain the group name and surface the
// placement policy an operator is usually checking.
func TestResourceGroupList(t *testing.T) {
	t.Parallel()

	tbl := view.ResourceGroupList([]apiv1.ResourceGroup{{
		Name:        "sc-1",
		Description: "default",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  3,
			StoragePool: "data",
			LayerStack:  []string{"DRBD", "STORAGE"},
		},
	}})

	assertColumns(t, tbl, []string{"ResourceGroup", "SelectFilter", "VlmNrs", "Description"})

	if got := cellAt(t, tbl, 0, "ResourceGroup"); got != "sc-1" {
		t.Errorf("ResourceGroup = %q, want sc-1", got)
	}

	filter := cellAt(t, tbl, 0, "SelectFilter")
	for _, want := range []string{"PlaceCount: 3", "StoragePool(s): data", "DRBD"} {
		if !strings.Contains(filter, want) {
			t.Errorf("SelectFilter = %q, want it to mention %q", filter, want)
		}
	}
}

// `volume list` joins the replica's volumes with their definitions.
func TestVolumeList(t *testing.T) {
	t.Parallel()

	tbl := view.VolumeList([]apiv1.Resource{{
		Name:     "pvc-x",
		NodeName: "node-1",
		Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			StoragePool:  "data",
			DevicePath:   "/dev/drbd1000",
			AllocatedKib: 1 << 20,
			State:        apiv1.VolumeState{DiskState: "UpToDate"},
		}},
	}})

	assertColumns(t, tbl, []string{"Node", "Resource", "StoragePool", "VolumeNr", "MinorNr", "DeviceName", "Allocated", "InUse", "State"})

	if got := cellAt(t, tbl, 0, "DeviceName"); got != "/dev/drbd1000" {
		t.Errorf("DeviceName = %q", got)
	}

	if got := cellAt(t, tbl, 0, "State"); got != "UpToDate" {
		t.Errorf("State = %q, want UpToDate", got)
	}

	if got := cellAt(t, tbl, 0, "Allocated"); got != "1 GiB" {
		t.Errorf("Allocated = %q, want 1 GiB", got)
	}
}

func assertColumns(t *testing.T, tbl *metav1.Table, want []string) {
	t.Helper()

	if len(tbl.ColumnDefinitions) != len(want) {
		got := make([]string, 0, len(tbl.ColumnDefinitions))
		for i := range tbl.ColumnDefinitions {
			got = append(got, tbl.ColumnDefinitions[i].Name)
		}

		t.Fatalf("columns = %v, want %v", got, want)
	}

	for i, name := range want {
		if tbl.ColumnDefinitions[i].Name != name {
			t.Errorf("column[%d] = %q, want %q", i, tbl.ColumnDefinitions[i].Name, name)
		}
	}
}
