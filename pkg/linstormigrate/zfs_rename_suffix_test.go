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

package linstormigrate

import (
	"testing"

	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// A snapshot whose replica LINSTOR already deleted lives under the zvol LINSTOR
// renamed, and LINSTOR recorded the rename suffix on that node's snapshot. The
// converter carries it, keyed by the node, so the satellite finds the snapshot
// by name when it is deleted or restored from.
func TestSnapshotCarriesLinstorsRenameSuffixPerNode(t *testing.T) {
	t.Parallel()

	const suffix = "2026-01-02T03-04-05-678"

	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
			{NodeName: "NODE-B", NodeDspName: "node-b", NodeType: 2, UUID: "n-b"},
		},
		ResourceDefinitions: []ResourceDefinitionRow{
			{ResourceName: "PVC-S", ResourceDspName: "pvc-s", LayerStack: `["DRBD","STORAGE"]`, UUID: "rd"},
			{ResourceName: "PVC-S", ResourceDspName: "pvc-s", SnapshotName: "SNAP", SnapshotDspName: "snap", ResourceFlags: snapDfnFlagSuccessful, UUID: "sd"},
		},
		VolumeDefinitions: []VolumeDefinitionRow{{ResourceName: "PVC-S", VlmNr: 0, VlmSize: 1048576, UUID: "vd"}},
		Resources: []ResourceRow{
			{NodeName: "NODE-B", ResourceName: "PVC-S", UUID: "r-b"},
			{NodeName: "NODE-A", ResourceName: "PVC-S", SnapshotName: "SNAP", UUID: "s-a"},
			{NodeName: "NODE-B", ResourceName: "PVC-S", SnapshotName: "SNAP", UUID: "s-b"},
		},
		PropsContainers: []PropsContainerRow{
			{PropsInstance: "/SNAPS/NODE-A/PVC-S/SNAP", PropKey: zfs.RenameSuffixProp, PropValue: suffix},
		},
	}

	res, err := convertForTest(t, dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	if len(res.Snapshots) != 1 {
		t.Fatalf("snapshots %d, want 1; warnings %v", len(res.Snapshots), res.Warnings)
	}

	props := res.Snapshots[0].Spec.Props

	if got := props[zfs.SnapshotRenameSuffixProp("node-a")]; got != suffix {
		t.Errorf("node-a suffix %q, want %q (props %v)", got, suffix, props)
	}

	if got, ok := props[zfs.SnapshotRenameSuffixProp("node-b")]; ok {
		t.Errorf("node-b got a suffix %q it never had", got)
	}
}
