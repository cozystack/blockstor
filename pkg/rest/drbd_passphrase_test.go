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

// TestDRBDPassphraseSet writes the per-RD shared secret onto the
// ResourceDefinition's props. The satellite consumes it via the same
// props pipeline that already powers ApplyResources.drbd_options.
func TestDRBDPassphraseSet(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{"passphrase": "supersecret"})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/encryption-passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(t.Context(), "pvc-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Props["DrbdOptions/Net/shared-secret"] != "supersecret" {
		t.Errorf("expected shared-secret stored; got %v", got.Props)
	}
}

// TestDRBDPassphraseEmpty: empty passphrase → 400.
func TestDRBDPassphraseEmpty(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/encryption-passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestDRBDPassphraseUnknownRD: 404 for missing RD.
func TestDRBDPassphraseUnknownRD(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(map[string]string{"passphrase": "supersecret"})

	resp := httpPost(t, base+"/v1/resource-definitions/ghost/encryption-passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// A passphrase carrying a newline or a NUL has no spelling in the drbd.conf
// string the secret becomes, so it is refused here rather than stored.
// Accepted, it returns 200 and then strands the resource on the node, where
// nothing points back at this call.
//
// A quote is NOT in that set: drbd-utils escapes it and drbdadm reads it back,
// so refusing one would turn away a passphrase the cluster can carry.
func TestDRBDPassphraseUnrepresentable(t *testing.T) {
	for name, passphrase := range map[string]string{
		"newline": "se\ncret",
		"nul":     "se\x00cret",
	} {
		st := store.NewInMemory()
		if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
			t.Fatalf("seed: %v", err)
		}

		base, stop := startServerWithStore(t, st)

		body, _ := json.Marshal(map[string]string{"passphrase": passphrase})

		resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/encryption-passphrase", body)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status: got %d, want 400", name, resp.StatusCode)
		}

		rd, err := st.ResourceDefinitions().Get(t.Context(), "pvc-1")
		if err != nil {
			t.Fatalf("%s: get: %v", name, err)
		}

		if got := rd.Props[drbdSharedSecretKey]; got != "" {
			t.Errorf("%s: the refused passphrase was stored anyway: %q", name, got)
		}

		stop()
	}
}

// The refusal is narrow on purpose. A quote and a backslash both have a
// spelling in drbd.conf, so a passphrase carrying either is stored and
// rendered escaped rather than turned away at the edge.
func TestDRBDPassphraseAcceptsQuotesAndBackslashes(t *testing.T) {
	const passphrase = `se"cr\et`

	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{"passphrase": passphrase})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/encryption-passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 — drbdadm round-trips both characters", resp.StatusCode)
	}

	rd, err := st.ResourceDefinitions().Get(t.Context(), "pvc-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if got := rd.Props[drbdSharedSecretKey]; got != passphrase {
		t.Errorf("stored passphrase = %q, want %q", got, passphrase)
	}
}
