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

// A diskless replica (a client or a tiebreaker) is built as DRBD over
// STORAGE whatever its definition's stack, so it neither shapes the stack
// nor makes the diskful replicas look divergent.
func TestADisklessReplicaDoesNotShapeTheStack(t *testing.T) {
	t.Parallel()

	for name, rdStack := range map[string]string{"empty": "[]", "named": `["DRBD","LUKS","STORAGE"]`} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dump := stackDump(rdStack,
				layerChain(1, "NODE-A", "PVC-S", "DRBD", "LUKS", "STORAGE"), layerChain(10, "NODE-B", "PVC-S", "DRBD", "STORAGE"))
			dump.Resources = []ResourceRow{
				{NodeName: "NODE-A", ResourceName: "PVC-S", UUID: "r-a"},
				{NodeName: "NODE-B", ResourceName: "PVC-S", ResourceFlags: resourceFlagDiskless, UUID: "r-b"},
			}

			got, res := convertedStack(t, dump)
			if !slices.Equal(got, []string{"DRBD", "LUKS", "STORAGE"}) {
				t.Fatalf("stack %v; warnings %v", got, res.Warnings)
			}

			if hasWarning(res, "built as") {
				t.Errorf("a diskless replica was reported as divergent; warnings %v", res.Warnings)
			}
		})
	}
}

// A definition whose replicas cannot be adopted under one stack is not
// converted, nor are its replicas: adopted as it is, a replica would come up
// under a stack it was not built with, DRBD metadata over a LUKS volume's
// ciphertext among them. Both shapes are refused and named: replicas built
// two ways under a row with no stack, and a replica built otherwise than the
// stack the row names.
func TestADefinitionWhoseReplicasDisagreeOnTheStackIsNotConverted(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		rowStack, reason string
	}{
		"replicas built two ways": {rowStack: "[]", reason: "built as DRBD,LUKS,STORAGE and DRBD,STORAGE"},
		"a replica built otherwise": {
			rowStack: `["DRBD","LUKS","STORAGE"]`, reason: "a replica was built as DRBD,STORAGE",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			res, err := Convert(stackDump(tc.rowStack,
				layerChain(1, "NODE-A", "PVC-S", "DRBD", "LUKS", "STORAGE"), layerChain(10, "NODE-B", "PVC-S", "DRBD", "STORAGE")))
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}

			for i := range res.ResourceDefinitions {
				if res.ResourceDefinitions[i].Name == "pvc-s" {
					t.Fatalf("pvc-s converted with stack %v", res.ResourceDefinitions[i].Spec.LayerStack)
				}
			}

			if len(res.Resources) != 0 {
				t.Errorf("%d replicas of a skipped definition converted", len(res.Resources))
			}

			if !hasWarning(res, tc.reason) || !hasWarning(res, "skipped with its replicas") {
				t.Errorf("the refusal does not name %q; warnings %v", tc.reason, res.Warnings)
			}
		})
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
