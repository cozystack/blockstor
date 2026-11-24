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

package dispatcher

import (
	"testing"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// TestWithDeviceSizesTakesLargestReplica: a replica getting a new lower
// disk learns the largest DRBD device size any replica of the volume
// reports, its own included, per volume. The sizes are those of a 20 GiB
// volume adopted from LINSTOR, whose device is 8 KiB larger than the
// definition.
func TestWithDeviceSizesTakesLargestReplica(t *testing.T) {
	t.Parallel()

	rd := &blockstoriov1alpha1.ResourceDefinition{}
	rd.Spec.VolumeDefinitions = []blockstoriov1alpha1.ResourceDefinitionVolume{
		{VolumeNumber: 0, SizeKib: 20971520},
		{VolumeNumber: 1, SizeKib: 4096},
	}

	target := &blockstoriov1alpha1.Resource{}
	target.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{{VolumeNumber: 0, DeviceSizeKib: 20971520}}

	peers := []blockstoriov1alpha1.Resource{{}, {}}
	peers[0].Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{{VolumeNumber: 0, DeviceSizeKib: 20971528}}
	peers[1].Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{{VolumeNumber: 1}}

	vols := withDeviceSizes(buildVolumes(rd, target), target, peers)

	if got := vols[0].GetDeviceSizeKib(); got != 20971528 {
		t.Errorf("volume 0: got %d KiB, want the largest reported 20971528", got)
	}

	if got := vols[1].GetDeviceSizeKib(); got != 0 {
		t.Errorf("volume 1: no replica reports a size, got %d", got)
	}
}
