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

package v1_test

import (
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// Only the answered copy forgets the satellite's report: a store may
// hand back slices and layers it still holds, and forgetting through
// them would erase the last report from the store itself.
func TestForgetSatelliteStateLeavesSharedDataAlone(t *testing.T) {
	t.Parallel()

	inUse := true
	vols := []apiv1.Volume{{VolumeNumber: 0, State: apiv1.VolumeState{DiskState: "UpToDate"}}}
	drbd := &apiv1.DrbdResourceLayer{
		Connections: map[string]apiv1.DrbdConnection{"n1": {Connected: true}},
		DrbdVolumes: []apiv1.DrbdVolume{{DiskState: "UpToDate"}},
	}
	storage := &apiv1.StorageResourceLayer{StorageVolumes: []apiv1.StorageVolumeLayer{{DiskState: "UpToDate"}}}
	layer := &apiv1.ResourceLayer{
		Type:     apiv1.LayerKindDRBD,
		Drbd:     drbd,
		Children: []apiv1.ResourceLayer{{Type: apiv1.LayerKindStorage, Storage: storage}},
	}

	rsc := apiv1.Resource{
		Name: "pvc-x", NodeName: "N2",
		State:       apiv1.ResourceState{InUse: &inUse, DrbdState: "UpToDate"},
		Volumes:     vols,
		LayerObject: layer,
	}

	apiv1.ForgetStateOnOfflineNodes([]apiv1.Resource{rsc}, []apiv1.Node{{Name: "n2", ConnectionStatus: apiv1.NodeTypeOffline}})

	if vols[0].State.DiskState != "UpToDate" || len(drbd.Connections) != 1 ||
		drbd.DrbdVolumes[0].DiskState != "UpToDate" || storage.StorageVolumes[0].DiskState != "UpToDate" ||
		layer.Drbd != drbd || layer.Children[0].Storage != storage {
		t.Fatal("forgetting the satellite state changed data the caller still holds")
	}

	answered := []apiv1.Resource{rsc}
	apiv1.ForgetStateOnOfflineNodes(answered, []apiv1.Node{{Name: "n2", ConnectionStatus: apiv1.NodeTypeOffline}})

	got := answered[0]
	if got.State.InUse != nil || got.State.DrbdState != "" || got.Volumes[0].State.DiskState != "" ||
		got.LayerObject.Drbd.Connections != nil || got.LayerObject.Drbd.DrbdVolumes[0].DiskState != "" ||
		got.LayerObject.Children[0].Storage.StorageVolumes[0].DiskState != "" {
		t.Errorf("replica on the OFFLINE node (spelled in another case) kept satellite state: %+v", got)
	}
}
