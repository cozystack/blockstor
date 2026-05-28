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

package dispatcher_test

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
)

// TestBuildDesiredOmitsUnresolvedLocalNodeID is the Bug 360 prevention
// pin: when the controller has NOT yet allocated this node's
// Status.DRBDNodeID (nil), BuildDesired MUST OMIT the local `node-id`
// key from DrbdOptions entirely rather than render an ambiguous
// `node-id 0`. The omitted key is the unambiguous "unresolved" signal
// the satellite's refuseUnresolvedLocalNodeID gate keys on to defer
// create-md / up — so neither the kernel slot NOR the on-disk v09
// metadata is ever burned with a bogus id 0 (the (119) ambiguous
// node id wedge). A literal `node-id 0` here would be
// indistinguishable from a legitimate LowestFreeNodeID==0 allocation.
func TestBuildDesiredOmitsUnresolvedLocalNodeID(t *testing.T) {
	rdName := "pvc-b360"

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	// Target with NO allocated DRBDNodeID (nil Status) — the
	// auto-place initial-create-burst window Bug 360 fires in.
	target := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rdName + "-n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               "n1",
			StoragePool:            "pool",
		},
	}

	got := dispatcher.BuildDesired(target, nil, nil, nil, rd, nil)
	if got == nil {
		t.Fatalf("BuildDesired returned nil")
	}

	if v, ok := got.DrbdOptions["node-id"]; ok {
		t.Errorf("unresolved local node-id must be OMITTED, but node-id=%q was rendered", v)
	}
}

// TestBuildDesiredRendersAllocatedLocalNodeIDZero pins the
// counterpart: a target legitimately allocated node-id 0 still
// renders `node-id 0`. The Bug 360 omission must trigger ONLY on the
// nil (unresolved) Status, never collapse a real id-0 allocation.
func TestBuildDesiredRendersAllocatedLocalNodeIDZero(t *testing.T) {
	rdName := "pvc-b360-zero"
	targetID := int32(0)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	target := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rdName + "-n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               "n1",
			StoragePool:            "pool",
		},
		Status: blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &targetID},
	}

	got := dispatcher.BuildDesired(target, nil, nil, nil, rd, nil)
	if got == nil {
		t.Fatalf("BuildDesired returned nil")
	}

	if got.DrbdOptions["node-id"] != "0" {
		t.Errorf("allocated local node-id = %q, want \"0\" (real id-0 must render, not be omitted)",
			got.DrbdOptions["node-id"])
	}
}
