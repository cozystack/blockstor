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
	"fmt"
	"net/http"
	"strings"
	"testing"

	lapi "github.com/LINBIT/golinstor/client"
	"github.com/LINBIT/golinstor/devicelayerkind"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// csiConditionIssues is linstor-csi v1.11.2's ConditionFromResources
// (pkg/client/linstor.go), which the external health monitor runs on
// every volume. A replica without a state object, or whose DRBD layer
// reports promotion_score 0, makes the volume abnormal. The two causes
// come back apart, as the nodes linstor-csi would name in the
// VolumeConditionAbnormal event, because this tree only answers for
// the first: it does not publish promotion_score yet, so every DRBD
// replica it serves decodes as score 0.
func csiConditionIssues(ress []lapi.ResourceWithVolumes) ([]string, []string) {
	var noState, zeroScore []string

	for i := range ress {
		res := &ress[i]

		if res.State == nil {
			noState = append(noState, res.NodeName)

			continue
		}

		if drbd := csiDrbdLayer(res.LayerObject); drbd != nil && drbd.PromotionScore == 0 {
			zeroScore = append(zeroScore, res.NodeName)
		}
	}

	return noState, zeroScore
}

// csiDrbdLayer is linstor-csi v1.11.2's util.GetDrbdLayer.
func csiDrbdLayer(layer *lapi.ResourceLayer) *lapi.DrbdResource {
	if layer == nil {
		return nil
	}

	if layer.Type == devicelayerkind.Drbd {
		return layer.Drbd
	}

	for i := range layer.Children {
		if drbd := csiDrbdLayer(&layer.Children[i]); drbd != nil {
			return drbd
		}
	}

	return nil
}

// csiInUse is the predicate linstor-csi v1.11.2 uses in Attach, Detach
// and Delete to count replicas a consumer holds open.
func csiInUse(r *lapi.Resource) bool {
	return r.State != nil && r.State.InUse != nil && *r.State.InUse
}

// The REST contract linstor-csi holds a replica to, per layer stack,
// on every endpoint it reads replicas from. LINSTOR builds the state
// object whenever the satellite has reported the resource and leaves
// in_use null for a STORAGE-topped one (StorageLayer reports it as
// unknown), so the replica goes out as `"state": {}`. A LUKS-topped
// one reports in_use false. Sending no state object at all made
// linstor-csi flag every healthy local volume as abnormal.
//
// The DRBD replica carries the layer payload the k8s store builds for
// an observed DRBD replica, so linstor-csi's DRBD branch runs on it.
// Only the state half of that check is asserted: promotion_score is
// not published by this tree, and the score half is tested where it
// is.
func TestResourceStateMatchesLinstorCSIContract(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	type want struct {
		stack []string
		layer *apiv1.ResourceLayer
		// inUse is the in_use key the state object carries; nil means
		// the key is absent.
		inUse *bool
	}

	unused := false

	cases := map[string]want{
		"pvc-local":     {stack: []string{"STORAGE"}},
		"pvc-encrypted": {stack: []string{"LUKS", "STORAGE"}, inUse: &unused},
		"pvc-drbd": {
			stack: []string{"DRBD", "STORAGE"},
			layer: &apiv1.ResourceLayer{
				Type: apiv1.LayerKindDRBD,
				Drbd: &apiv1.DrbdResourceLayer{
					TCPPorts: []int32{7000},
					DrbdVolumes: []apiv1.DrbdVolume{{
						VolumeNumber: 0,
						DiskState:    "UpToDate",
						DevicePath:   "/dev/drbd1000",
					}},
				},
				Children: []apiv1.ResourceLayer{{Type: apiv1.LayerKindStorage}},
			},
			inUse: &unused,
		},
	}

	for name, c := range cases {
		if err := st.ResourceDefinitions().Create(ctx,
			&apiv1.ResourceDefinition{Name: name, LayerStack: c.stack}); err != nil {
			t.Fatalf("seed RD: %v", err)
		}

		observed := false

		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:        name,
			NodeName:    "worker-1",
			LayerObject: c.layer,
			State:       apiv1.ResourceState{InUse: &observed},
			Volumes:     []apiv1.Volume{{VolumeNumber: 0}},
		}); err != nil {
			t.Fatalf("seed Resource: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	t.Cleanup(stop)

	t.Run("wire", func(t *testing.T) {
		t.Parallel()

		for name, c := range cases {
			for _, url := range []string{
				base + "/v1/view/resources?resources=" + name,
				base + "/v1/resource-definitions/" + name + "/resources",
				base + "/v1/resource-definitions/" + name + "/resources/worker-1",
			} {
				assertWireState(t, url, c.inUse)
			}
		}
	})

	t.Run("linstor-csi", func(t *testing.T) {
		t.Parallel()

		client := newClient(t, base)

		for name, c := range cases {
			view, err := client.Resources.GetResourceView(t.Context(),
				&lapi.ListOpts{Resource: []string{name}})
			if err != nil {
				t.Fatalf("%s: GetResourceView: %v", name, err)
			}

			if len(view) != 1 {
				t.Fatalf("%s: view lists %d replicas, want 1", name, len(view))
			}

			if noState, _ := csiConditionIssues(view); len(noState) > 0 {
				t.Errorf("%s: linstor-csi reports Resource with issues on node(s): %s",
					name, strings.Join(noState, ","))
			}

			if c.layer != nil && csiDrbdLayer(view[0].LayerObject) == nil {
				t.Errorf("%s: view carries no DRBD layer for linstor-csi to check", name)
			}

			ress, err := client.Resources.GetAll(t.Context(), name)
			if err != nil {
				t.Fatalf("%s: GetAll: %v", name, err)
			}

			for i := range ress {
				if ress[i].State == nil {
					t.Errorf("%s: GetAll replica on %s has no state", name, ress[i].NodeName)
				}

				if csiInUse(&ress[i]) {
					t.Errorf("%s: linstor-csi counts the idle replica on %s as in use",
						name, ress[i].NodeName)
				}
			}
		}
	})
}

// assertWireState decodes url (a list or a single replica) and checks
// that every replica carries a state object whose in_use key matches
// wantInUse. Only in_use is compared: it is the one key linstor-csi
// reads out of the state.
func assertWireState(t *testing.T, url string, wantInUse *bool) {
	t.Helper()

	resp := httpGet(t, url)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s: status %d", url, resp.StatusCode)
	}

	var raw json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatalf("%s: decode: %v", url, err)
	}

	var replicas []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &replicas); err != nil {
		var single map[string]json.RawMessage
		if err := json.Unmarshal(raw, &single); err != nil {
			t.Fatalf("%s: neither a list nor a replica: %s", url, raw)
		}

		replicas = []map[string]json.RawMessage{single}
	}

	if len(replicas) != 1 {
		t.Fatalf("%s: %d replicas, want 1", url, len(replicas))
	}

	stateRaw, present := replicas[0]["state"]
	if !present || string(stateRaw) == "null" {
		t.Errorf("%s: state = %s (present %v), want an object", url, stateRaw, present)

		return
	}

	var state map[string]any
	if err := json.Unmarshal(stateRaw, &state); err != nil {
		t.Errorf("%s: state %s is not an object: %v", url, stateRaw, err)

		return
	}

	got, hasInUse := state["in_use"]

	switch {
	case wantInUse == nil && hasInUse:
		t.Errorf("%s: state %s carries in_use, want it absent", url, stateRaw)
	case wantInUse != nil && (!hasInUse || got != *wantInUse):
		t.Errorf("%s: state %s, want in_use %s", url, stateRaw, fmt.Sprint(*wantInUse))
	}
}
