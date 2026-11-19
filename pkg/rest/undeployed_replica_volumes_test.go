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
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// LINSTOR creates a replica's volumes together with the replica, so
// between the placement and the satellite building it every REST read
// already lists one volume per volume definition, without a device
// path or disk state. blockstor listed only what the satellite had
// reported. In that window linstor-csi's ControllerPublishVolume got a
// 404 for volume 0 of a local volume, and linstor-scheduler-extender
// panicked indexing volumes[0] of a replica in the resource view.
//
// The fixture is a two-volume definition with a diskful replica the
// satellite has reported volume 0 of and not volume 1, and a second
// replica it has not reported at all.
func seedUndeployedReplicas(t *testing.T) string {
	t.Helper()

	st := store.NewInMemory()
	ctx := t.Context()

	for _, node := range []string{"n1", "n2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: node}); err != nil {
			t.Fatalf("seed node: %v", err)
		}
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-new"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, vn := range []int32{0, 1} {
		if err := st.VolumeDefinitions().Create(ctx, "pvc-new", &apiv1.VolumeDefinition{
			VolumeNumber: vn, SizeKib: 1 << 20,
		}); err != nil {
			t.Fatalf("seed VD %d: %v", vn, err)
		}
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-new", NodeName: "n1",
		Props: map[string]string{"StorPoolName": "data"},
		Volumes: []apiv1.Volume{{
			VolumeNumber: 0, StoragePool: "data", DevicePath: "/dev/zvol/data/pvc-new_00000",
		}},
	}); err != nil {
		t.Fatalf("seed reported replica: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-new", NodeName: "n2",
		Props: map[string]string{"StorPoolName": "data"},
	}); err != nil {
		t.Fatalf("seed unbuilt replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	t.Cleanup(stop)

	return base
}

func getJSON(t *testing.T, url string, out any) {
	t.Helper()

	resp := httpGet(t, url)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d, want 200", url, resp.StatusCode)
	}

	err := json.NewDecoder(resp.Body).Decode(out)
	if err != nil {
		t.Fatalf("GET %s: decode: %v", url, err)
	}
}

func volumeNumbers(vols []apiv1.Volume) []int32 {
	out := make([]int32, 0, len(vols))
	for i := range vols {
		out = append(out, vols[i].VolumeNumber)
	}

	return out
}

func assertDefinitionVolumes(t *testing.T, where string, vols []apiv1.Volume) {
	t.Helper()

	got := volumeNumbers(vols)
	if len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("%s: volume numbers = %v, want [0 1]", where, got)
	}

	for i := range vols {
		if vols[i].StoragePool != "data" {
			t.Errorf("%s: volume %d pool = %q, want the replica's pool", where, vols[i].VolumeNumber, vols[i].StoragePool)
		}
	}
}

func TestUnbuiltVolumeAnswersWithoutDevicePath(t *testing.T) {
	t.Parallel()

	base := seedUndeployedReplicas(t)

	// linstor-csi's Attach: GET volume 0 of the replica on the target
	// node. A 404 here failed ControllerPublishVolume outright.
	var vol apiv1.Volume

	getJSON(t, base+"/v1/resource-definitions/pvc-new/resources/n2/volumes/0", &vol)

	if vol.VolumeNumber != 0 || vol.DevicePath != "" || vol.State.DiskState != "" {
		t.Errorf("unbuilt volume 0 = %+v, want volume 0 with no device path and no disk state", vol)
	}

	// A reported volume keeps what the satellite reported.
	getJSON(t, base+"/v1/resource-definitions/pvc-new/resources/n1/volumes/0", &vol)

	if vol.DevicePath != "/dev/zvol/data/pvc-new_00000" {
		t.Errorf("reported volume 0 device path = %q, want the reported one", vol.DevicePath)
	}

	// A volume number with no definition is still a 404.
	resp := httpGet(t, base+"/v1/resource-definitions/pvc-new/resources/n2/volumes/7")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("undefined volume 7: status %d, want 404", resp.StatusCode)
	}
}

func TestUnbuiltReplicaListsEveryDefinedVolume(t *testing.T) {
	t.Parallel()

	base := seedUndeployedReplicas(t)

	for _, node := range []string{"n1", "n2"} {
		var vols []apiv1.Volume

		getJSON(t, base+"/v1/resource-definitions/pvc-new/resources/"+node+"/volumes", &vols)
		assertDefinitionVolumes(t, "volume list on "+node, vols)

		var res apiv1.ResourceWithVolumes

		getJSON(t, base+"/v1/resource-definitions/pvc-new/resources/"+node, &res)
		assertDefinitionVolumes(t, "resource get on "+node, res.Volumes)
	}

	var list []apiv1.ResourceWithVolumes

	getJSON(t, base+"/v1/resource-definitions/pvc-new/resources", &list)

	if len(list) != 2 {
		t.Fatalf("resource list: %d replicas, want 2", len(list))
	}

	for i := range list {
		assertDefinitionVolumes(t, "resource list "+list[i].NodeName, list[i].Volumes)
	}

	// linstor-scheduler-extender reads volumes[0].state.disk_state of
	// every replica in the view.
	var view []apiv1.ResourceWithVolumes

	getJSON(t, base+"/v1/view/resources?resources=pvc-new", &view)

	if len(view) != 2 {
		t.Fatalf("view: %d replicas, want 2", len(view))
	}

	for i := range view {
		assertDefinitionVolumes(t, "view "+view[i].NodeName, view[i].Volumes)
	}

	if view[0].Volumes[0].DevicePath != "/dev/zvol/data/pvc-new_00000" {
		t.Errorf("view: reported volume 0 on n1 lost its device path: %+v", view[0].Volumes[0])
	}
}
