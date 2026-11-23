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

	lapi "github.com/LINBIT/golinstor/client"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestResourceViewCarriesReservedCapacity reads the resource view the way
// linstor-csi's ReservedCapacity does: by node and storage pool, adding up
// the usable size in the STORAGE layer data of each volume. Without the
// layer data every pool reported nothing promised, and a StorageClass with
// overProvision offered the whole over-provisioned size again and again.
func TestResourceViewCarriesReservedCapacity(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	seedOnlineNodeWithPool(t, st, "n1")
	seedRDWithVD(t, st, "pvc-cap", 4096)
	seedRDWithVD(t, st, "pvc-new", 2048)

	err := st.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, "pvc-cap", func(rd *apiv1.ResourceDefinition) error {
		rd.LayerStack = apiv1.DefaultLayerStack()

		return nil
	})
	if err != nil {
		t.Fatalf("seed layer stack: %v", err)
	}

	for _, res := range []apiv1.Resource{
		{
			Name: "pvc-cap", NodeName: "n1", Props: map[string]string{"StorPoolName": "zfs-thin"},
			Volumes: []apiv1.Volume{{VolumeNumber: 0, StoragePool: "zfs-thin"}},
		},
		// A diskful replica its satellite has not reported yet has
		// promised its size all the same: LINSTOR knows its volumes
		// from the moment it exists, and a burst of CreateVolume reads
		// the capacity while the new replicas are in this state.
		{Name: "pvc-new", NodeName: "n1", Props: map[string]string{"StorPoolName": "zfs-thin"}},
	} {
		if err := st.Resources().Create(ctx, &res); err != nil {
			t.Fatalf("seed replica: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	reses, err := newClient(t, base).Resources.GetResourceView(ctx,
		&lapi.ListOpts{Node: []string{"n1"}, StoragePool: []string{"zfs-thin"}})
	if err != nil {
		t.Fatalf("GetResourceView: %v", err)
	}

	var reserved int64

	for i := range reses {
		for j := range reses[i].Volumes {
			layers := reses[i].Volumes[j].LayerDataList
			if len(layers) == 0 {
				continue
			}

			if sv, ok := layers[len(layers)-1].Data.(*lapi.StorageVolume); ok {
				reserved += sv.UsableSizeKib
			}
		}
	}

	if reserved != 4096+2048 {
		t.Errorf("reserved capacity of zfs-thin on n1: %d KiB, want %d", reserved, 4096+2048)
	}

	other, err := newClient(t, base).Resources.GetResourceView(ctx,
		&lapi.ListOpts{StoragePool: []string{"lvm-thin"}})
	if err != nil {
		t.Fatalf("GetResourceView: %v", err)
	}

	if len(other) != 0 {
		t.Errorf("a pool with no volume on it listed %d resources", len(other))
	}
}
