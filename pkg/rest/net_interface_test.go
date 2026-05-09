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

// TestNetInterfaceCreateAppendsToNode adds a NetInterface to a Node
// without disturbing the rest of the spec. The interface lives
// inline on Node.Spec.NetInterfaces[]; we don't mint a separate CRD.
func TestNetInterfaceCreateAppendsToNode(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.1"},
		},
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.NetInterface{
		Name: "replication", Address: "10.10.0.1", SatellitePort: 7000,
	})

	resp := httpPost(t, base+"/v1/nodes/n1/net-interfaces", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Nodes().Get(ctx, "n1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if len(got.NetInterfaces) != 2 {
		t.Fatalf("interface count: got %d, want 2", len(got.NetInterfaces))
	}

	names := map[string]bool{}
	for _, n := range got.NetInterfaces {
		names[n.Name] = true
	}

	if !names["default"] || !names["replication"] {
		t.Errorf("expected both default + replication; got %v", got.NetInterfaces)
	}
}

// TestNetInterfaceCreateReplacesSameName: idempotent — second create
// with the same name overwrites in place rather than duplicating.
func TestNetInterfaceCreateReplacesSameName(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.1"},
		},
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.NetInterface{Name: "default", Address: "192.168.1.5"})

	resp := httpPost(t, base+"/v1/nodes/n1/net-interfaces", body)
	_ = resp.Body.Close()

	got, _ := st.Nodes().Get(ctx, "n1")
	if len(got.NetInterfaces) != 1 {
		t.Errorf("idempotent create grew the list: %v", got.NetInterfaces)
	}

	if got.NetInterfaces[0].Address != "192.168.1.5" {
		t.Errorf("address not overwritten: %v", got.NetInterfaces[0])
	}
}

// TestNetInterfaceUpdatePathNameWins: the URL's {name} is the
// authoritative interface identifier; a name in the body is ignored.
func TestNetInterfaceUpdatePathNameWins(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "replication", Address: "10.10.0.1"},
		},
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Send body with a different name to prove the path wins.
	body, _ := json.Marshal(apiv1.NetInterface{Name: "evil", Address: "10.10.0.99"})

	resp := httpPut(t, base+"/v1/nodes/n1/net-interfaces/replication", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, _ := st.Nodes().Get(ctx, "n1")
	if len(got.NetInterfaces) != 1 {
		t.Fatalf("count: got %d, want 1; interfaces=%v", len(got.NetInterfaces), got.NetInterfaces)
	}

	if got.NetInterfaces[0].Name != "replication" {
		t.Errorf("name overridden by body: %v", got.NetInterfaces[0])
	}

	if got.NetInterfaces[0].Address != "10.10.0.99" {
		t.Errorf("address not updated: %v", got.NetInterfaces[0])
	}
}

// TestNetInterfaceDelete drops the interface; subsequent delete is a
// no-op (idempotent).
func TestNetInterfaceDelete(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.1"},
			{Name: "replication", Address: "10.10.0.1"},
		},
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	for range 2 {
		resp := httpDelete(t, base+"/v1/nodes/n1/net-interfaces/replication")
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("delete status: got %d, want 204", resp.StatusCode)
		}
	}

	got, _ := st.Nodes().Get(ctx, "n1")
	if len(got.NetInterfaces) != 1 {
		t.Errorf("count: got %d, want 1; interfaces=%v", len(got.NetInterfaces), got.NetInterfaces)
	}

	if got.NetInterfaces[0].Name != "default" {
		t.Errorf("wrong interface survived: %v", got.NetInterfaces[0])
	}
}
