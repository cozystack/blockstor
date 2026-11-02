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

	k8sstore "github.com/cozystack/blockstor/pkg/store/k8s"
)

// An adopted replica and snapshot store their parents the way the store
// writes them: the CR name in spec, the LINSTOR spelling in an annotation.
// The controller and the satellite Get the parent by the spec value, so a
// mixed-case spelling there would miss it.
func TestAdoptedChildrenStoreTheirParentsUnderCRNames(t *testing.T) {
	t.Parallel()

	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-MIX", NodeDspName: "Node-Mix", NodeType: 2, UUID: "n-a"},
		},
		ResourceDefinitions: []ResourceDefinitionRow{
			{ResourceName: "PVC-MIX", ResourceDspName: "Pvc-Mix", LayerStack: `["DRBD","STORAGE"]`, UUID: "rd-mix"},
			{
				ResourceName: "PVC-MIX", ResourceDspName: "Pvc-Mix", SnapshotName: "SNAP", SnapshotDspName: "snap",
				ResourceFlags: snapDfnFlagSuccessful, UUID: "sd-mix",
			},
		},
		VolumeDefinitions: []VolumeDefinitionRow{
			{ResourceName: "PVC-MIX", VlmNr: 0, VlmSize: 1048576, UUID: "vd-mix"},
		},
		Resources: []ResourceRow{
			{NodeName: "NODE-MIX", ResourceName: "PVC-MIX", UUID: "r-mix"},
			{NodeName: "NODE-MIX", ResourceName: "PVC-MIX", SnapshotName: "SNAP", UUID: "s-mix"},
		},
	}

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	if len(res.Resources) != 1 || len(res.Snapshots) != 1 {
		t.Fatalf("resources %d, snapshots %d, want 1 each; warnings %v", len(res.Resources), len(res.Snapshots), res.Warnings)
	}

	r := res.Resources[0]
	if r.Spec.ResourceDefinitionName != "pvc-mix" || r.Spec.NodeName != "node-mix" {
		t.Errorf("replica spec stores %q on %q, want the CR names pvc-mix on node-mix",
			r.Spec.ResourceDefinitionName, r.Spec.NodeName)
	}

	if got := r.Annotations[k8sstore.AnnotationLinstorRDName]; got != "Pvc-Mix" {
		t.Errorf("replica keeps definition spelling %q, want Pvc-Mix", got)
	}

	if got := r.Annotations[k8sstore.AnnotationLinstorNodeName]; got != "Node-Mix" {
		t.Errorf("replica keeps node spelling %q, want Node-Mix", got)
	}

	s := res.Snapshots[0]
	if s.Spec.ResourceDefinitionName != "pvc-mix" {
		t.Errorf("snapshot spec stores %q, want the CR name pvc-mix", s.Spec.ResourceDefinitionName)
	}

	if got := s.Annotations[k8sstore.AnnotationLinstorRDName]; got != "Pvc-Mix" {
		t.Errorf("snapshot keeps definition spelling %q, want Pvc-Mix", got)
	}
}

// A lowercase parent needs no annotation: the spec value is its spelling.
func TestALowercaseParentGetsNoSpellingAnnotation(t *testing.T) {
	t.Parallel()

	dump := &Dump{
		Nodes: []NodeRow{
			{NodeName: "NODE-A", NodeDspName: "node-a", NodeType: 2, UUID: "n-a"},
		},
		ResourceDefinitions: []ResourceDefinitionRow{
			{ResourceName: "PVC-A", ResourceDspName: "pvc-a", LayerStack: `["DRBD","STORAGE"]`, UUID: "rd-a"},
		},
		VolumeDefinitions: []VolumeDefinitionRow{
			{ResourceName: "PVC-A", VlmNr: 0, VlmSize: 1048576, UUID: "vd-a"},
		},
		Resources: []ResourceRow{
			{NodeName: "NODE-A", ResourceName: "PVC-A", UUID: "r-a"},
		},
	}

	res, err := Convert(dump)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	if len(res.Resources) != 1 {
		t.Fatalf("resources %d, want 1; warnings %v", len(res.Resources), res.Warnings)
	}

	for _, key := range []string{k8sstore.AnnotationLinstorRDName, k8sstore.AnnotationLinstorNodeName} {
		if v, ok := res.Resources[0].Annotations[key]; ok {
			t.Errorf("annotation %s = %q on a lowercase parent", key, v)
		}
	}
}
