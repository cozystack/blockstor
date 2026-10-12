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
	"slices"
	"testing"
)

// layerChain is one replica's LAYER_RESOURCE_IDS chain on node, ids
// from first, root first.
func layerChain(first int32, node, rd string, kinds ...string) []LayerResourceIDRow {
	rows := make([]LayerResourceIDRow, 0, len(kinds))

	for i, kind := range kinds {
		row := LayerResourceIDRow{LayerResourceID: first + int32(i), LayerResourceKind: kind, NodeName: node, ResourceName: rd}
		if i > 0 {
			row.LayerResourceParentID = ptr(first + int32(i) - 1)
		}

		rows = append(rows, row)
	}

	return rows
}

// stackDump is a one-RD dump on two nodes whose RD row asks for
// rdStack and whose replicas are built as chains.
func stackDump(rdStack string, chains ...[]LayerResourceIDRow) *Dump {
	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
			{NodeName: "NODE-B", NodeDspName: "node-b", NodeType: 2, UUID: "n-b"},
		},
		ResourceGroups: []ResourceGroupRow{
			{ResourceGroupName: "RG", ResourceGroupDspName: "rg", LayerStack: `["STORAGE"]`, UUID: "rg"},
		},
		ResourceDefinitions: []ResourceDefinitionRow{
			{ResourceName: "PVC-S", ResourceDspName: "pvc-s", ResourceGroupName: "RG", LayerStack: rdStack, UUID: "rd-s"},
		},
		VolumeDefinitions: []VolumeDefinitionRow{
			{ResourceName: "PVC-S", VlmNr: 0, VlmSize: 1048576, UUID: "vd-s"},
		},
	}

	for _, chain := range chains {
		dump.LayerResourceIDs = append(dump.LayerResourceIDs, chain...)
	}

	return dump
}

func convertedStack(t *testing.T, dump *Dump) ([]string, *Result) {
	t.Helper()

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	for _, rd := range res.ResourceDefinitions {
		if rd.Name == "pvc-s" {
			return rd.Spec.LayerStack, res
		}
	}

	t.Fatalf("pvc-s did not convert; warnings: %v", res.Warnings)

	return nil, nil
}

// LINSTOR resolves the stack per resource and often leaves the RD row
// empty, in a group whose own stack is something else (seen on a real
// cluster: a [STORAGE] group holding DRBD volumes). The converted RD
// carries what is on disk, so blockstor never pins the group's stack
// over it.
func TestAnEmptyRDStackIsReadOffItsReplicas(t *testing.T) {
	t.Parallel()

	for name, kinds := range map[string][]string{
		"DRBD":           {"DRBD", "STORAGE"},
		"DRBD over LUKS": {"DRBD", "LUKS", "STORAGE"},
		"LUKS only":      {"LUKS", "STORAGE"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, res := convertedStack(t, stackDump("[]",
				layerChain(1, "NODE-A", "PVC-S", kinds...), layerChain(10, "NODE-B", "PVC-S", kinds...)))
			if !slices.Equal(got, kinds) {
				t.Fatalf("stack %v, want %v; warnings %v", got, kinds, res.Warnings)
			}
		})
	}
}

// Snapshot instances and suffixed children (DRBD external metadata)
// are not the replica's data path and do not shape its stack.
func TestSnapshotAndSuffixedLayersDoNotShapeTheStack(t *testing.T) {
	t.Parallel()

	meta := LayerResourceIDRow{LayerResourceID: 20, LayerResourceKind: "STORAGE", LayerResourceParentID: ptr(int32(1)), LayerResourceSuffix: ".meta", NodeName: "NODE-A", ResourceName: "PVC-S"}
	snap := LayerResourceIDRow{LayerResourceID: 21, LayerResourceKind: "STORAGE", NodeName: "NODE-A", ResourceName: "PVC-S", SnapshotName: "SNAP"}

	got, res := convertedStack(t, stackDump("[]", layerChain(1, "NODE-A", "PVC-S", "DRBD", "LUKS", "STORAGE"), []LayerResourceIDRow{meta, snap}))
	if !slices.Equal(got, []string{"DRBD", "LUKS", "STORAGE"}) {
		t.Fatalf("stack %v; warnings %v", got, res.Warnings)
	}
}

// Replicas built two ways leave nothing to read: the stack stays unset
// and the operator is told which ways.
func TestReplicasThatDisagreeLeaveTheStackUnset(t *testing.T) {
	t.Parallel()

	got, res := convertedStack(t, stackDump("[]",
		layerChain(1, "NODE-A", "PVC-S", "DRBD", "LUKS", "STORAGE"), layerChain(10, "NODE-B", "PVC-S", "DRBD", "STORAGE")))
	if len(got) != 0 {
		t.Fatalf("stack %v, want unset", got)
	}

	if !hasWarning(res, "built as DRBD,LUKS,STORAGE and DRBD,STORAGE") {
		t.Errorf("disagreement not reported; warnings %v", res.Warnings)
	}
}

// A stack the RD row names is kept, and a replica built otherwise is
// reported: the satellite would try to rebuild it.
func TestAReplicaBuiltOtherwiseThanItsRDIsReported(t *testing.T) {
	t.Parallel()

	got, res := convertedStack(t, stackDump(`["DRBD","LUKS","STORAGE"]`,
		layerChain(1, "NODE-A", "PVC-S", "DRBD", "LUKS", "STORAGE"), layerChain(10, "NODE-B", "PVC-S", "DRBD", "STORAGE")))
	if !slices.Equal(got, []string{"DRBD", "LUKS", "STORAGE"}) {
		t.Fatalf("stack %v, want the RD row's", got)
	}

	if !hasWarning(res, "a replica was built as DRBD,STORAGE") {
		t.Errorf("mismatch not reported; warnings %v", res.Warnings)
	}

	if hasWarning(res, "a replica was built as DRBD,LUKS,STORAGE") {
		t.Errorf("a matching replica was reported; warnings %v", res.Warnings)
	}
}

// No replica, nothing to read: the RD stays as the row has it.
func TestAnEmptyRDStackWithoutReplicasStaysEmpty(t *testing.T) {
	t.Parallel()

	got, _ := convertedStack(t, stackDump("[]"))
	if len(got) != 0 {
		t.Fatalf("stack %v, want unset", got)
	}
}
