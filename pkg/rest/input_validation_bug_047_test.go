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

// BUG-047 (csi-sanity conformance): the RD-name validator was
// lowercase-only RFC-1123 — STRICTER than upstream LINSTOR — so
// csi-sanity's uppercase-hex CreateVolume names (forwarded verbatim as
// RD names by the upstream linstor-csi sidecar) were rejected with
// code=Internal, failing 19 conformance specs.
//
// Oracle evidence (upstream LINSTOR controller, dev stand 2026-06-14)
// established that upstream ACCEPTS uppercase / underscore / trailing
// hyphen and treats names case-insensitively, with a leading-letter
// requirement and a 2–48 length window. The validator now mirrors
// `^[A-Za-z][A-Za-z0-9_-]{1,47}$`.
//
// These tests pin the relaxed-but-still-bounded ruleset at the REST
// wire boundary AND assert the k8s store round-trips the accepted
// names (so DeleteVolume / Publish / Stage resolve the same RD later).

// TestBug047UppercaseNamesAccepted is the primary repro: the exact
// name shapes csi-sanity emits (and the upstream oracle accepts) must
// now 201 and persist, addressable via the store under the same name.
func TestBug047UppercaseNamesAccepted(t *testing.T) {
	t.Parallel()

	cases := []string{
		"TestUpperCase123",            // oracle: SUCCESS
		"pvc-2A1B4B95EA8C4D7E",        // csi-sanity-shaped uppercase hex
		"Has_Underscore_1",            // underscore allowed upstream
		"MixedCase-with_underscore-9", // full charset
		"foo-",                        // trailing hyphen allowed upstream
		"Foobar",                      // leading-uppercase, mixed
	}

	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			base, stop := startServerWithStore(t, st)
			defer stop()

			body, _ := json.Marshal(apiv1.ResourceDefinitionCreate{
				ResourceDefinition: apiv1.ResourceDefinition{Name: name},
			})

			resp := httpPost(t, base+"/v1/resource-definitions", body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusCreated {
				got, _ := readAllBody(resp)
				t.Fatalf("status: got %d, want 201 for upstream-valid name %q. Body: %s",
					resp.StatusCode, name, got)
			}

			// The store must resolve the RD under the SAME name the
			// caller used — this is the CSI lifecycle invariant
			// (CreateVolume name == volume_id; later Delete/Publish use
			// it verbatim). The k8s Name() helper folds case onto a
			// lowercase slug internally, but Get(name) routes through
			// the same helper, so the round-trip is by-name stable.
			if _, err := st.ResourceDefinitions().Get(t.Context(), name); err != nil {
				t.Errorf("RD %q not resolvable by original name after create: %v", name, err)
			}
		})
	}
}

// TestBug047NameValidatorTable is the unit-level table over the
// validator itself, pinning the exact upstream ruleset from the oracle
// run (see input_validation.go header for the captured outputs).
func TestBug047NameValidatorTable(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		valid bool
	}{
		// Accepted upstream.
		{"TestUpperCase123", true},
		{"pvc-2A1B4B95EA8C4D7E", true},
		{"Has_Underscore_1", true},
		{"foo-", true},
		{"ab", true}, // minimum length 2
		{"pvc-c8a1d6b9-3e2f-4d1b-8e8f-2c5e9e8e8e8e", true},
		// Rejected upstream.
		{"", false},                       // empty
		{"a", false},                      // length 1 < min 2
		{"1foo", false},                   // leading digit
		{"-foo", false},                   // leading hyphen
		{"_foo", false},                   // leading underscore
		{"foo.bar", false},                // embedded dot
		{"Foo Bar", false},                // embedded space
		{"foo/bar", false},                // path separator
		{string(make([]byte, 49)), false}, // over length cap (also non-letter bytes)
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := validateLinstorName("resource definition", tc.name)
			if tc.valid && err != nil {
				t.Errorf("validateLinstorName(%q) = %v, want nil (upstream accepts)", tc.name, err)
			}

			if !tc.valid && err == nil {
				t.Errorf("validateLinstorName(%q) = nil, want error (upstream rejects)", tc.name)
			}
		})
	}
}

// TestBug047LowercaseProvisionerPathUnchanged guards the real-world
// invariant the task emphasises: already-valid lowercase
// `pvc-<uuid>` names (the external-provisioner path) must behave
// EXACTLY as before — 201 + addressable by the same name, no
// case-fold surprise.
func TestBug047LowercaseProvisionerPathUnchanged(t *testing.T) {
	t.Parallel()

	cases := []string{
		"pvc-1",
		"pvc-c8a1d6b9-3e2f-4d1b-8e8f-2c5e9e8e8e8e",
		"snapshot-restore-target",
	}

	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			base, stop := startServerWithStore(t, st)
			defer stop()

			body, _ := json.Marshal(apiv1.ResourceDefinitionCreate{
				ResourceDefinition: apiv1.ResourceDefinition{Name: name},
			})

			resp := httpPost(t, base+"/v1/resource-definitions", body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusCreated {
				got, _ := readAllBody(resp)
				t.Fatalf("status: got %d, want 201 for lowercase name %q. Body: %s",
					resp.StatusCode, name, got)
			}

			rd, err := st.ResourceDefinitions().Get(t.Context(), name)
			if err != nil {
				t.Fatalf("RD %q not persisted: %v", name, err)
			}

			if rd.Name != name {
				t.Errorf("lowercase name mutated on round-trip: got %q, want %q", rd.Name, name)
			}
		})
	}
}
