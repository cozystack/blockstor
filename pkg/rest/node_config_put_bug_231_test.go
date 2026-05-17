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
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 231 (P2) — `PUT /v1/nodes/{node}/config` returned 405.
// Bug 228 wired only GET on the same path; python-linstor's
// `linstor node set-log-level <node> <LVL>` issues a PUT with the
// same `SatelliteConfig` wire shape used by GET (see Java
// `Nodes.java:setConfig` at line 645 — body is parsed via
// `objectMapper.readValue(jsonData, JsonGenTypes.SatelliteConfig.class)`).
//
// Pre-fix the route was unregistered, so the apiserver's default
// method-mismatch handler returned 405 + the Bug 109 typed envelope —
// clean error, but operators had no way to change a satellite's log
// level remotely. The fix is to wire the PUT handler accepting the
// same body shape GET emits.
//
// blockstor doesn't run upstream's StltConfig push protocol, so the
// only field that has runtime effect is `log.level` (or the flat
// `log_level` alias). The handler accepts the body, applies the
// log level via the existing parseLogLevel + runtimeLogLevel path
// when present, and returns 200 + a standard MASK_INFO envelope so
// the CLI's success line renders cleanly. Other fields are
// accepted-and-ignored with a TODO for the satellite-side push.

// TestBug231NodeConfigPutFlatLogLevel: PUT with the flat shape
// `{"log_level":"DEBUG"}` against a seeded node MUST return 200 (not
// 405). Pre-fix the route was unregistered.
func TestBug231NodeConfigPutFlatLogLevel(t *testing.T) {
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

	body, _ := json.Marshal(map[string]string{"log_level": "DEBUG"})

	resp := httpPut(t, base+"/v1/nodes/n1/config", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("PUT /v1/nodes/n1/config: got %d, want 200 (Bug 231 — route unregistered, body=%q)",
			resp.StatusCode, string(raw))
	}

	// Envelope MUST be a `[]APICallRc` — the wire shape every other
	// write-side endpoint uses so python-linstor renders the success
	// line uniformly.
	var rcs []apiv1.APICallRc

	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 || rcs[0].Message == "" {
		t.Errorf("envelope rcs=%+v; want non-empty []APICallRc with a message", rcs)
	}
}

// TestBug231NodeConfigPutNestedLog: PUT with the nested shape
// `{"log":{"level":"DEBUG"}}` (the upstream Java wire shape — see
// `JsonGenTypes.SatelliteConfigLog`) MUST also return 200. The handler
// must accept both forms symmetric with the controller-config PUT
// (Bug 159) so the CLI's nested-body callers don't fall through to
// 400/405.
func TestBug231NodeConfigPutNestedLog(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1", Type: apiv1.NodeTypeSatellite,
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]any{
		"log": map[string]string{"level": "DEBUG"},
	})

	resp := httpPut(t, base+"/v1/nodes/n1/config", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("PUT nested log block: got %d, want 200 (Bug 231, body=%q)",
			resp.StatusCode, string(raw))
	}
}

// TestBug231NodeConfigPutUnknownNode: PUT against an unknown node MUST
// 404 (not 405). The Bug 228 GET sibling already 404s on unknown
// nodes; PUT must do the same so the CLI's typed error path fires.
func TestBug231NodeConfigPutUnknownNode(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(map[string]string{"log_level": "DEBUG"})

	resp := httpPut(t, base+"/v1/nodes/ghost/config", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("PUT /v1/nodes/ghost/config: got %d, want 404", resp.StatusCode)
	}
}

// TestBug231NodeConfigPutMalformedBody: malformed JSON MUST return
// 400 + the standard error envelope (not 405, not 500). The Bug
// 158/161 typed-envelope contract applies to this handler the same
// way every other write-side endpoint uses decodeJSON.
func TestBug231NodeConfigPutMalformedBody(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1", Type: apiv1.NodeTypeSatellite,
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/nodes/n1/config", []byte("not json"))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("PUT malformed: got %d, want 400 (body=%q)",
			resp.StatusCode, string(raw))
	}

	var rcs []apiv1.APICallRc

	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 || rcs[0].Message == "" {
		t.Errorf("error envelope rcs=%+v; want non-empty []APICallRc with a message", rcs)
	}
}
