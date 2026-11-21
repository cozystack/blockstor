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

	apiv1.ForgetStaleSatelliteState([]apiv1.Resource{rsc}, []apiv1.Node{{Name: "n2", ConnectionStatus: apiv1.NodeTypeOffline}})

	if vols[0].State.DiskState != "UpToDate" || len(drbd.Connections) != 1 ||
		drbd.DrbdVolumes[0].DiskState != "UpToDate" || storage.StorageVolumes[0].DiskState != "UpToDate" ||
		layer.Drbd != drbd || layer.Children[0].Storage != storage {
		t.Fatal("forgetting the satellite state changed data the caller still holds")
	}

	answered := []apiv1.Resource{rsc}
	apiv1.ForgetStaleSatelliteState(answered, []apiv1.Node{{Name: "n2", ConnectionStatus: apiv1.NodeTypeOffline}})

	got := answered[0]
	if got.State.InUse != nil || got.State.DrbdState != "" || got.Volumes[0].State.DiskState != "" ||
		got.LayerObject.Drbd.Connections != nil {
		t.Errorf("replica on the OFFLINE node (spelled in another case) kept satellite state: %+v", got)
	}

	// LINSTOR answers the layer volumes from the layer data the
	// controller merged from the last apply, which it keeps while the
	// satellite is offline.
	if got.LayerObject.Drbd.DrbdVolumes[0].DiskState != "UpToDate" ||
		got.LayerObject.Children[0].Storage.StorageVolumes[0].DiskState != "UpToDate" {
		t.Errorf("layer volumes lost the disk state LINSTOR keeps: drbd %+v, storage %+v",
			got.LayerObject.Drbd.DrbdVolumes, got.LayerObject.Children[0].Storage.StorageVolumes)
	}
}

// Once a rebooted node is ONLINE again, its satellite brings the
// replicas up one by one; until it reports one, the store still holds
// what the satellite before the reboot said. LINSTOR shows state only
// from the satellite it is connected to now, so such a replica reads
// with no state. A missing incarnation on either side, from a satellite
// older than the field or a replica nothing has reported to since,
// proves nothing and leaves the state shown.
func TestForgetStaleSatelliteStateByIncarnation(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		node, rsc  string
		wantForget bool
	}{
		{"reported before the restart", "inc-2", "inc-1", true},
		{"reported since the restart", "inc-2", "inc-2", false},
		{"satellite stamps no incarnation", "", "inc-1", false},
		{"replica carries no incarnation", "inc-2", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			inUse := false
			answered := []apiv1.Resource{{
				Name: "pvc-x", NodeName: "n2", SatelliteIncarnation: tc.rsc,
				State:   apiv1.ResourceState{InUse: &inUse, DrbdState: "UpToDate"},
				Volumes: []apiv1.Volume{{State: apiv1.VolumeState{DiskState: "UpToDate"}}},
			}}
			nodes := []apiv1.Node{{Name: "N2", ConnectionStatus: apiv1.NodeTypeOnline, SatelliteIncarnation: tc.node}}

			apiv1.ForgetStaleSatelliteState(answered, nodes)

			forgotten := answered[0].State.InUse == nil && answered[0].Volumes[0].State.DiskState == ""
			if forgotten != tc.wantForget {
				t.Errorf("forgotten = %v, want %v: %+v", forgotten, tc.wantForget, answered[0])
			}
		})
	}
}
