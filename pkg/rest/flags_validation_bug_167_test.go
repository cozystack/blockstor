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
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 167 (P2) — RD/Resource `flags[]` accepts arbitrary strings.
//
// `linstor rd c X --flags YOLOFLAG` (or the equivalent wire shape
// `{"flags":["YOLOFLAG"]}`) was accepted and the RD persisted with a
// nonsense flag. Same hole for Resource create — `{"flags":["YOLOFLAG"]}`
// stuck a phantom flag onto the CRD that the satellite reconciler then
// either ignored (best case) or branched on a typo of a real flag
// (worst case: a typo for `DISKLESS` could quietly diverge placement
// semantics across releases).
//
// The LINSTOR enum is documented upstream (server/src/main/java/com/
// linbit/linstor/core/objects/ResourceDefinition.java::Flags and
// Resource.java::Flags) but neither create nor modify enforced it at
// the REST boundary. The fix gates every wire path that accepts a
// flags slice against a per-type allow-list and refuses unknown flags
// with a 400 + LINSTOR envelope naming both the offending input and
// the canonical set.

// TestBug167RDCreateRefusesUnknownFlag pins the wire-boundary refusal
// for `POST /v1/resource-definitions` carrying an unknown flag.
func TestBug167RDCreateRefusesUnknownFlag(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{
			Name:  "poke167",
			Flags: []string{"YOLOFLAG"},
		},
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got := mustReadBody(t, resp)
		t.Fatalf("status: got %d, want 400 (Bug 167 unknown RD flag). Body: %s", resp.StatusCode, got)
	}

	got := mustReadBody(t, resp)
	rc := mustDecodeEnvelope(t, got)
	assertErrorEnvelope(t, rc, got)

	// The envelope must name the offending flag so operators can fix the call.
	if !strings.Contains(string(got), "YOLOFLAG") {
		t.Errorf("envelope must cite the offending flag value: %s", got)
	}

	// And surface the canonical allow-list (the RD `DELETE` flag is
	// the most stable name across upstream releases).
	if !strings.Contains(string(got), "DELETE") {
		t.Errorf("envelope must list allowed flags (expected at least DELETE): %s", got)
	}

	// No phantom RD persisted.
	if _, getErr := st.ResourceDefinitions().Get(t.Context(), "poke167"); getErr == nil {
		t.Errorf("RD poke167 persisted despite 400 — wire gate did not refuse the body")
	}
}

// TestBug167RDCreateAcceptsKnownFlags asserts the happy-path
// regression guard: an RD-create that carries only documented flags
// must still succeed (201 Created). Without this guard, a too-strict
// allow-list could regress legitimate workflows (snapshot restore,
// upstream-replica spawn).
func TestBug167RDCreateAcceptsKnownFlags(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{
			Name: "poke167ok",
			// RESTORE_TARGET is the upstream LINSTOR RD flag used by
			// snapshot-restore (server/src/main/java/com/linbit/linstor/
			// core/objects/ResourceDefinition.java::Flags). Pinning a
			// non-DELETE flag here keeps the test honest: an empty
			// allow-list would 400 even on valid input.
			Flags: []string{"RESTORE_TARGET"},
		},
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		got := mustReadBody(t, resp)
		t.Fatalf("status: got %d, want 201 (Bug 167 valid RD flag must pass). Body: %s", resp.StatusCode, got)
	}

	rd, getErr := st.ResourceDefinitions().Get(t.Context(), "poke167ok")
	if getErr != nil {
		t.Fatalf("Get RD: %v", getErr)
	}

	if len(rd.Flags) != 1 || rd.Flags[0] != "RESTORE_TARGET" {
		t.Errorf("RD flags: got %v, want [RESTORE_TARGET]", rd.Flags)
	}
}

// TestBug167ResourceCreateRefusesUnknownFlag pins the same wire-boundary
// refusal for `POST /v1/resource-definitions/{rd}/resources` carrying
// an unknown Resource flag. Pre-fix the phantom flag persisted onto
// the Resource CRD and the satellite reconciler then had to guess
// whether the typo was a no-op or a misspelled `DISKLESS`.
func TestBug167ResourceCreateRefusesUnknownFlag(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd167"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed Node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{
			NodeName: "n1",
			Flags:    []string{"YOLOFLAG"},
		},
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/rd167/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got := mustReadBody(t, resp)
		t.Fatalf("status: got %d, want 400 (Bug 167 unknown Resource flag). Body: %s", resp.StatusCode, got)
	}

	got := mustReadBody(t, resp)
	rc := mustDecodeEnvelope(t, got)
	assertErrorEnvelope(t, rc, got)

	if !strings.Contains(string(got), "YOLOFLAG") {
		t.Errorf("envelope must cite the offending flag value: %s", got)
	}

	// `DISKLESS` is the canonical Resource flag the upstream CLI and
	// every wire-shape client touches; if the allow-list rendering
	// drops it the envelope wouldn't help the operator.
	if !strings.Contains(string(got), "DISKLESS") {
		t.Errorf("envelope must list allowed Resource flags (expected at least DISKLESS): %s", got)
	}

	// No phantom Resource CRD.
	if _, getErr := st.Resources().Get(ctx, "rd167", "n1"); getErr == nil {
		t.Errorf("Resource rd167.n1 persisted despite 400 — wire gate did not refuse the body")
	}
}

// TestBug167RDModifyRefusesUnknownFlag pins the same refusal for the
// `PUT /v1/resource-definitions/{rd}` modify path. The legacy "PUT the
// full ResourceDefinition read-side shape" wire convention is still
// in use (Bug 161 follow-up), so a stray `flags` value on the modify
// envelope must be refused with the same envelope shape as the create
// path.
func TestBug167RDModifyRefusesUnknownFlag(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	// Seed an RD so the modify path passes its existence gate before
	// reaching the flag-validation step under test.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd167mod"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Use a raw body — the legacy modify shape accepts the full
	// read-side ResourceDefinition keys at the top level (Bug 161).
	body := []byte(`{"flags":["YOLOFLAG"]}`)

	resp := httpPut(t, base+"/v1/resource-definitions/rd167mod", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got := mustReadBody(t, resp)
		t.Fatalf("status: got %d, want 400 (Bug 167 unknown RD flag on modify). Body: %s", resp.StatusCode, got)
	}

	got := mustReadBody(t, resp)
	rc := mustDecodeEnvelope(t, got)
	assertErrorEnvelope(t, rc, got)

	if !strings.Contains(string(got), "YOLOFLAG") {
		t.Errorf("envelope must cite the offending flag value: %s", got)
	}

	// And the merge must not have written the phantom flag.
	stored, getErr := st.ResourceDefinitions().Get(ctx, "rd167mod")
	if getErr != nil {
		t.Fatalf("Get RD after refused modify: %v", getErr)
	}

	for _, f := range stored.Flags {
		if f == "YOLOFLAG" {
			t.Errorf("RD persisted phantom flag YOLOFLAG despite 400: stored=%v", stored.Flags)
		}
	}
}
