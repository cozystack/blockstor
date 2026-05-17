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

// Bug 233 (P3) — `PUT /v1/resource-definitions/{rd}/volume-definitions/{vlmNr}/encryption-passphrase`
// returned 404. Upstream LINSTOR exposes per-VD LUKS passphrase
// rotation here (Java
// `VolumeDefinitions.java:modifyVolumeDefinitionPassphrase` at line
// 278; body shape is `JsonGenTypes.VolumeDefinitionModifyPassphrase`,
// i.e. `{"new_passphrase":"…"}`).
//
// blockstor's satellite-side `cryptsetup luksChangeKey` orchestration
// doesn't exist yet, so the REST endpoint accepts the body and
// stores the supplied passphrase on the VD's props under the
// upstream-compatible `DrbdOptions/Encrypt/Passphrase` key — the
// downstream reconciler will pick it up once the cluster-side
// rotation lands (Phase 12). The wire shape is what matters for
// upstream parity so `linstor vd set-passphrase` doesn't 404.
//
// The handler also accepts the bare-string `PassPhraseEnter` shape
// (Bug 173 dual-form), since golinstor and strict-OpenAPI clients
// posting `"…"` directly are common enough that rejecting them at the
// VD route while the cluster-passphrase PATCH accepts them would be
// a confusing asymmetry.

// seedVDForBug233 seeds an RD + VD pair so the PUT under test has a
// real target to rotate. Centralised so the test cases don't drift on
// the seed-shape invariants.
func seedVDForBug233(t *testing.T, st store.Store) {
	t.Helper()

	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "rd-luks",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "rd-luks", &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}
}

// TestBug233VDPassphraseRouteRegistered: PUT against a real RD+VD
// MUST NOT 404. Pre-fix the route was unregistered. Status code is
// 200 (happy path) or 501 (cluster-side orchestration pending) —
// anything that isn't 404 satisfies the wire-parity contract.
func TestBug233VDPassphraseRouteRegistered(t *testing.T) {
	st := store.NewInMemory()
	seedVDForBug233(t, st)

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{"new_passphrase": "rotated-secret"})

	resp := httpPut(t,
		base+"/v1/resource-definitions/rd-luks/volume-definitions/0/encryption-passphrase", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("PUT VD passphrase: got 404, want 200 or 501 (Bug 233 — route unregistered, body=%q)",
			string(raw))
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotImplemented {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("PUT VD passphrase: got %d, want 200 or 501 (body=%q)",
			resp.StatusCode, string(raw))
	}

	// Whatever the status, the wire envelope MUST be a non-empty
	// `[]APICallRc` so python-linstor renders the operator-visible
	// line uniformly.
	var rcs []apiv1.APICallRc

	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 || rcs[0].Message == "" {
		t.Errorf("envelope rcs=%+v; want non-empty []APICallRc with a message", rcs)
	}
}

// TestBug233VDPassphraseBareStringShape: golinstor and the upstream
// `PassPhraseEnter: type: string` strict-spec clients post a bare
// JSON string body. The handler must accept it symmetric with the
// Bug 173 cluster-passphrase PATCH so a `--curl` operator running
// `curl -d '"…"'` doesn't fall through to 400.
func TestBug233VDPassphraseBareStringShape(t *testing.T) {
	st := store.NewInMemory()
	seedVDForBug233(t, st)

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal("rotated-bare-secret")

	resp := httpPut(t,
		base+"/v1/resource-definitions/rd-luks/volume-definitions/0/encryption-passphrase", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("PUT VD passphrase bare string: got 404, want 200 or 501 (Bug 233, body=%q)",
			string(raw))
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotImplemented {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("PUT VD passphrase bare string: got %d, want 200 or 501 (body=%q)",
			resp.StatusCode, string(raw))
	}
}

// TestBug233VDPassphraseUnknownRD: a missing parent RD MUST 404 (not
// 405). Mirrors every other per-RD route's typed-not-found surface.
func TestBug233VDPassphraseUnknownRD(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(map[string]string{"new_passphrase": "rotated-secret"})

	resp := httpPut(t,
		base+"/v1/resource-definitions/ghost/volume-definitions/0/encryption-passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("PUT unknown RD: got %d, want 404", resp.StatusCode)
	}
}

// TestBug233VDPassphraseEmptyValue: `{"new_passphrase":""}` MUST 400.
// Empty passphrase rotations would erase the per-VD LUKS key
// silently — the same Bug 172-class data-loss surface this codebase
// fences off everywhere. Bare empty string `""` MUST also 400 (same
// guard via proofOfKnowledge).
func TestBug233VDPassphraseEmptyValue(t *testing.T) {
	cases := []struct {
		name string
		body []byte
	}{
		{"wrapped-empty", mustMarshalBug233(map[string]string{"new_passphrase": ""})},
		{"bare-empty", mustMarshalBug233("")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := store.NewInMemory()
			seedVDForBug233(t, st)

			base, stop := startServerWithStore(t, st)
			defer stop()

			resp := httpPut(t,
				base+"/v1/resource-definitions/rd-luks/volume-definitions/0/encryption-passphrase", tc.body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusBadRequest {
				raw, _ := io.ReadAll(resp.Body)
				t.Fatalf("PUT empty passphrase: got %d, want 400 (Bug 233 data-loss guard, body=%q)",
					resp.StatusCode, string(raw))
			}

			var rcs []apiv1.APICallRc

			if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
				t.Fatalf("decode envelope: %v", err)
			}

			if len(rcs) == 0 || rcs[0].Message == "" {
				t.Errorf("envelope rcs=%+v; want non-empty []APICallRc with a message", rcs)
			}
		})
	}
}

// TestBug233VDPassphraseMalformedBody: non-JSON garbage MUST 400 +
// standard envelope. Mirrors the Bug 158/161 contract every
// write-side endpoint upholds via decodeJSON.
func TestBug233VDPassphraseMalformedBody(t *testing.T) {
	st := store.NewInMemory()
	seedVDForBug233(t, st)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t,
		base+"/v1/resource-definitions/rd-luks/volume-definitions/0/encryption-passphrase",
		[]byte("not json"))
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

// mustMarshalBug233 is a tiny test helper that json.Marshals or
// fails the test. Keeps the cases table above concise.
func mustMarshalBug233(v any) []byte {
	out, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}

	return out
}
