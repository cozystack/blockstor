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
)

// The literal bit values in these tests are copied from the `enum Flags`
// of each object in the LINSTOR v1.33.1 source, not from the constants
// under test, so a constant that drifts to another table's layout fails
// here instead of agreeing with itself.
const (
	linstorRscFlagClean          int64 = 1 << 0 // Resource.Flags.CLEAN
	linstorRscDfnFlagDelete      int64 = 1 << 0 // ResourceDefinition.Flags.DELETE
	linstorRscDfnFlagRestore     int64 = 1 << 1 // ResourceDefinition.Flags.RESTORE_TARGET
	linstorSnapDfnFlagSuccessful int64 = 1 << 0 // SnapshotDefinition.Flags.SUCCESSFUL
)

// flagDump is one two-node definition with a replica on each node and
// one snapshot placed on both, every flag column zero unless set.
func flagDump() *Dump {
	return &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
			{NodeName: "NODE-B", NodeDspName: "node-b", NodeType: 2, UUID: "n-b"},
		},
		ResourceDefinitions: []ResourceDefinitionRow{
			{ResourceName: "PVC-F", ResourceDspName: "pvc-f", LayerStack: `["DRBD","STORAGE"]`, UUID: "rd-f"},
			{
				ResourceName: "PVC-F", ResourceDspName: "pvc-f", SnapshotName: "SNAP", SnapshotDspName: "snap",
				ResourceFlags: linstorSnapDfnFlagSuccessful, UUID: "sd-f",
			},
		},
		VolumeDefinitions: []VolumeDefinitionRow{
			{ResourceName: "PVC-F", VlmNr: 0, VlmSize: 1048576, UUID: "vd-f"},
		},
		Resources: []ResourceRow{
			{NodeName: "NODE-A", ResourceName: "PVC-F", UUID: "r-a"},
			{NodeName: "NODE-B", ResourceName: "PVC-F", UUID: "r-b"},
			{NodeName: "NODE-A", ResourceName: "PVC-F", SnapshotName: "SNAP", UUID: "s-a"},
			{NodeName: "NODE-B", ResourceName: "PVC-F", SnapshotName: "SNAP", UUID: "s-b"},
		},
	}
}

func convertFlagDump(t *testing.T, mutate func(*Dump)) *Result {
	t.Helper()

	dump := flagDump()
	mutate(dump)

	res, err := convertForTest(t, dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	return res
}

// A resource definition's DELETE is bit 0; bit 1 is RESTORE_TARGET. Read
// with a replica's layout, a definition LINSTOR was deleting came across
// live and one being restored was dropped with its replicas.
func TestResourceDefinitionFlagsReadTheirOwnEnum(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		flags   int64
		convert bool
	}{
		{name: "DELETE is skipped", flags: linstorRscDfnFlagDelete, convert: false},
		{name: "RESTORE_TARGET converts", flags: linstorRscDfnFlagRestore, convert: true},
		{name: "DELETE wins over RESTORE_TARGET", flags: linstorRscDfnFlagDelete | linstorRscDfnFlagRestore, convert: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			res := convertFlagDump(t, func(d *Dump) { d.ResourceDefinitions[0].ResourceFlags = tc.flags })

			if got := len(res.ResourceDefinitions) == 1; got != tc.convert {
				t.Fatalf("definition converted = %v, want %v; warnings %v", got, tc.convert, res.Warnings)
			}

			if got := len(res.Resources) == 2; got != tc.convert {
				t.Errorf("both replicas converted = %v, want %v; warnings %v", got, tc.convert, res.Warnings)
			}

			if got := hasWarning(res, "resource definition pvc-f: marked DELETE"); got == tc.convert {
				t.Errorf("DELETE skip reported = %v, want %v; warnings %v", got, !tc.convert, res.Warnings)
			}
		})
	}
}

// Bit 0 of a replica row is CLEAN, not DELETE: a clean replica converts.
func TestReplicaCleanBitIsNotDelete(t *testing.T) {
	t.Parallel()

	res := convertFlagDump(t, func(d *Dump) { d.Resources[0].ResourceFlags = linstorRscFlagClean })

	if len(res.Resources) != 2 {
		t.Errorf("converted %d replicas, want 2 (CLEAN is not DELETE); warnings %v", len(res.Resources), res.Warnings)
	}
}

// The package constants agree with LINSTOR's per-table enums.
func TestFlagConstantsMatchLinstor(t *testing.T) {
	t.Parallel()

	for name, pair := range map[string][2]int64{
		"rscDfnFlagDelete": {rscDfnFlagDelete, linstorRscDfnFlagDelete},
	} {
		if pair[0] != pair[1] {
			t.Errorf("%s = %d, LINSTOR has %d", name, pair[0], pair[1])
		}
	}
}
