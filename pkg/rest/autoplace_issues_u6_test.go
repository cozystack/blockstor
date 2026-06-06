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
	"io"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestU139ContradictoryConstraintsReturns409 is the REST-layer half of
// the upstream issue U139 / U94 FIX-CANDIDATE pin: an autoplace whose
// topology constraints are self-contradictory
// (`--replicas-on-same zone` AND `--replicas-on-different zone`) and
// therefore can only place fewer replicas than requested MUST surface a
// 409 FAIL_NOT_ENOUGH_NODES envelope — never a 200 "success on 0/1
// nodes". This is the operator-visible guard against the upstream
// "successfully autoplaced on 0 nodes" report.
//
// The placer under-places (placed < want); runPlaceAndReport turns that
// into the structured 409 envelope (apiCallRcFailNotEnoughNodes).
func TestU139ContradictoryConstraintsReturns409(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	mk := func(name, zone string) {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name:  name,
			Type:  apiv1.NodeTypeSatellite,
			Props: map[string]string{"Aux/zone": zone},
		}); err != nil {
			t.Fatalf("seed node %s: %v", name, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName: name, StoragePoolName: "pool",
			ProviderKind:  apiv1.StoragePoolKindLVMThin,
			FreeCapacity:  1000,
			TotalCapacity: 1000,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", name, err)
		}
	}

	mk("a1", "zone-a")
	mk("b1", "zone-b")

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:       "rd-u139",
		LayerStack: []string{apiv1.LayerKindDRBD, apiv1.LayerKindStorage},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:          2,
			StoragePool:         "pool",
			ReplicasOnSame:      []string{"zone"},
			ReplicasOnDifferent: []string{"zone"},
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/rd-u139/autoplace", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 (contradictory constraints must not succeed-on-zero)",
			resp.StatusCode)
	}

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	var rcs []apiv1.APICallRc
	if err := json.Unmarshal(raw, &rcs); err != nil {
		t.Fatalf("decode envelope: %v (body %s)", err, raw)
	}

	if len(rcs) == 0 {
		t.Fatalf("expected a non-empty ApiCallRc envelope, got %s", raw)
	}

	// The sub-code must be FAIL_NOT_ENOUGH_NODES (996), OR-ed with the
	// MASK_ERROR bit — the exact wire shape upstream LINSTOR emits so
	// cli-parity classifiers and the Python CLI render the actionable
	// criteria block.
	const failNotEnoughNodes = int64(996)

	matched := false

	for _, rc := range rcs {
		if rc.RetCode&failNotEnoughNodes == failNotEnoughNodes {
			matched = true

			break
		}
	}

	if !matched {
		t.Errorf("expected a FAIL_NOT_ENOUGH_NODES (996) sub-code in the envelope, got %s", raw)
	}

	// At most one replica may have been stamped (the first pins a zone);
	// the second can never satisfy both rules. Crucially the call did NOT
	// report success.
	got, err := st.Resources().ListByDefinition(ctx, "rd-u139")
	if err != nil {
		t.Fatalf("list resources: %v", err)
	}

	if len(got) >= 2 {
		t.Errorf("contradictory constraints must not yield 2 replicas: %+v", got)
	}
}

// TestU139UnsatisfiableNodeListReturns409 pins the second U139/U94
// envelope at the REST layer: an autoplace pinned to a node that has no
// diskful pool of the requested kind under-places and surfaces the 409,
// not a success. Mirrors the placer-layer
// TestU139UnsatisfiableNodeListUnderPlaces but through the full HTTP
// handler so the wire shape is asserted end-to-end.
func TestU139UnsatisfiableNodeListReturns409(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	// Only a diskless pool — no diskful candidate survives matchesPoolFilter.
	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName: "n1", StoragePoolName: "DfltDisklessStorPool",
		ProviderKind: apiv1.StoragePoolKindDiskless,
	}); err != nil {
		t.Fatalf("seed diskless pool: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:       "rd-u139b",
		LayerStack: []string{apiv1.LayerKindDRBD, apiv1.LayerKindStorage},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:   1,
			NodeNameList: []string{"n1"},
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/rd-u139b/autoplace", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 (no diskful pool on the only candidate node)",
			resp.StatusCode)
	}

	raw, _ := io.ReadAll(resp.Body)
	if !strings.Contains(strings.ToLower(string(raw)), "not enough") {
		t.Errorf("body should carry the not-enough-nodes envelope; got %s", raw)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "rd-u139b")
	if len(got) != 0 {
		t.Errorf("no diskful replica must be created: %+v", got)
	}
}
