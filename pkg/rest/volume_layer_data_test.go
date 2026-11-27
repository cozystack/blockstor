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

package rest

import (
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// pythonIndexedKeys are the keys python linstor 1.29 reads from a
// volume layer's data with a plain index, per layer type: a missing
// one is a KeyError in `linstor r l` or `linstor v l`.
func pythonIndexedKeys(kind string) []string {
	if kind == apiv1.LayerKindDRBD {
		return []string{"drbd_volume_definition", "device_path", "allocated_size_kib", "usable_size_kib"}
	}

	return nil
}

// TestEveryVolumeLayerCarriesData: LINSTOR sends `data` on every entry
// of a volume's layer_data_list, and python linstor 1.29 indexes it for
// DRBD, LUKS and STORAGE alike, so `linstor r l` against blockstor
// failed with KeyError: 'data' on the DRBD entry. Every entry of every
// volume, on a replica with a disk and on a diskless one, in the view
// and in the per-replica volume list, carries it now, and a DRBD entry
// carries the minor of its device.
func TestEveryVolumeLayerCarriesData(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:       "pvc-layers",
		LayerStack: []string{apiv1.LayerKindDRBD, apiv1.LayerKindLUKS, apiv1.LayerKindStorage},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-layers", &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 32 * 1024}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	for _, seed := range []apiv1.Resource{
		{
			Name: "pvc-layers", NodeName: "node-a",
			Volumes: []apiv1.Volume{{
				VolumeNumber: 0, DevicePath: "/dev/drbd1003", AllocatedKib: 1024, UsableKib: 32 * 1024,
				State: apiv1.VolumeState{DiskState: "UpToDate"},
				// What the k8s store projects for every reported volume.
				LayerDataList: []apiv1.VolumeLayerData{{Type: apiv1.LayerKindDRBD}, {Type: apiv1.LayerKindStorage}},
			}},
		},
		{Name: "pvc-layers", NodeName: "node-b", Flags: []string{apiv1.ResourceFlagDiskless}},
	} {
		if err := st.Resources().Create(ctx, &seed); err != nil {
			t.Fatalf("seed Resource: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	var view []struct {
		NodeName string           `json:"node_name"`
		Volumes  []map[string]any `json:"volumes"`
	}

	getJSON(t, base+"/v1/view/resources", &view)

	if len(view) != 2 {
		t.Fatalf("view: got %d replicas, want 2", len(view))
	}

	for _, rsc := range view {
		assertLayersCarryData(t, rsc.NodeName+" view", rsc.Volumes)
	}

	var perReplica []map[string]any

	getJSON(t, base+"/v1/resource-definitions/pvc-layers/resources/node-a/volumes", &perReplica)
	assertLayersCarryData(t, "node-a volumes", perReplica)

	drbd := perReplica[0]["layer_data_list"].([]any)[0].(map[string]any)["data"].(map[string]any)
	if minor := drbd["drbd_volume_definition"].(map[string]any)["minor_number"]; minor != float64(1003) {
		t.Errorf("DRBD minor_number: got %v, want 1003", minor)
	}
}

func assertLayersCarryData(t *testing.T, where string, vols []map[string]any) {
	t.Helper()

	if len(vols) == 0 {
		t.Fatalf("%s: no volumes", where)
	}

	for _, vol := range vols {
		layers, _ := vol["layer_data_list"].([]any)
		if len(layers) == 0 {
			t.Fatalf("%s: empty layer_data_list", where)
		}

		for _, raw := range layers {
			layer, _ := raw.(map[string]any)
			kind, _ := layer["type"].(string)

			data, ok := layer["data"].(map[string]any)
			if !ok {
				t.Errorf("%s: %s entry has no data: %v", where, kind, layer)

				continue
			}

			for _, key := range pythonIndexedKeys(kind) {
				if _, ok := data[key]; !ok {
					t.Errorf("%s: %s data lacks %q: %v", where, kind, key, data)
				}
			}

			if kind == apiv1.LayerKindDRBD {
				def, _ := data["drbd_volume_definition"].(map[string]any)
				for _, key := range []string{"resource_name_suffix", "volume_number", "minor_number"} {
					if _, ok := def[key]; !ok {
						t.Errorf("%s: drbd_volume_definition lacks %q: %v", where, key, def)
					}
				}
			}
		}
	}
}
