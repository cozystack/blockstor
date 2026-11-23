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

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
)

// A volume definition flagged DELETE is no volume of the replica any more:
// leaving it out of the desired state is what takes it out of DRBD before
// the satellite deletes its storage.
func TestBuildDesiredLeavesOutAVolumeDefinitionBeingDeleted(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1 << 20},
				{VolumeNumber: 1, SizeKib: 1 << 20, Flags: []string{blockstoriov1alpha1.VolumeDefinitionFlagDelete}},
			},
		},
	}
	target := &blockstoriov1alpha1.Resource{
		Spec: blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-vdd", NodeName: "n1"},
	}

	got := dispatcher.BuildDesired(target, nil, nil, nil, rd, nil)
	if len(got.Volumes) != 1 || got.Volumes[0].VolumeNumber != 0 {
		t.Fatalf("desired volumes %+v, want only volume 0", got.Volumes)
	}
}
