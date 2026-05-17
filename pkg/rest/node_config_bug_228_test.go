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

// Bug 228 (P3) — `GET /v1/nodes/{node}/config` was missing.
// Upstream LINSTOR exposes the satellite-side config projection for
// a single node (Java `controller/.../Nodes.java:getStltConfig` and
// `JsonGenTypes.SatelliteConfig`). Used by the python CLI's
// `linstor node config` diagnostic to surface satellite log levels,
// bind address, port, and connection type.
//
// blockstor doesn't run upstream's StltConfig push protocol; we
// project what we know — the node's primary NetInterface drives the
// `net.bind_address` / `net.port` / `net.com_type` block, and a
// blockstor-defined log level + special-satellite flag fill the rest.
// Pre-fix this 404s, denying operators the diagnostic.

// TestBug228NodeConfigReturnsProjection: a registered node exposes
// a non-empty config projection — at minimum the `net` block must
// surface the primary interface's bind address + port. Pre-fix 404s.
func TestBug228NodeConfigReturnsProjection(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1", Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.5", SatellitePort: 3366, SatelliteEncryptionType: "PLAIN"},
		},
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/nodes/n1/config")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	// Decode loosely so the test stays robust to additive wire
	// extensions — we only pin the fields operators rely on
	// (net.bind_address + net.port).
	var got map[string]any

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	netBlock, ok := got["net"].(map[string]any)
	if !ok {
		t.Fatalf("response missing `net` block: %v", got)
	}

	if got, want := netBlock["bind_address"], "10.0.0.5"; got != want {
		t.Errorf("net.bind_address: got %v, want %q", got, want)
	}

	// JSON numbers decode as float64; the satellite port is an integer.
	if got, want := netBlock["port"], float64(3366); got != want {
		t.Errorf("net.port: got %v, want %v", got, want)
	}
}

// TestBug228NodeConfigUnknownNode: unknown node must 404.
func TestBug228NodeConfigUnknownNode(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/nodes/ghost/config")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}
