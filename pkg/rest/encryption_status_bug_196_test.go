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

	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 196 (P2 SPEC): `GET /v1/encryption/passphrase` (operationId
// `passphraseStatus` in upstream OpenAPI) was unwired — the catch-all
// turned `linstor encryption status` into a 404 envelope. The state
// is fully derivable in-process from `s.passphraseUnlocked` +
// readPassphrase, so the fix is a 6-line handler.
//
// Wire shape (upstream-faithful — `PassphraseStatus` schema in
// docs/rest_v1_openapi.yaml lines 8357-8367):
//
//   - response body is an ARRAY of `{"status": "<enum>"}` (upstream's
//     OpenAPI wraps the singleton object in a slice, matching the
//     `[]ApiCallRc` convention used by every other LINSTOR REST
//     reply);
//   - the enum has three values: "unset" (no passphrase has ever
//     been POSTed), "locked" (passphrase set but the controller's
//     in-memory unlock flag is false — typical after restart), or
//     "unlocked" (passphrase set and the proof-of-knowledge unlock
//     is active).
//
// The task description suggested a `{"is_set":bool,"is_unlocked":bool}`
// shape, but the upstream OpenAPI source-of-truth is the tri-state
// status enum. Implementing the upstream shape is the only way
// `linstor encryption status` actually parses the reply — diverging
// here would just trade one unwired endpoint for one mis-shaped one.

// TestBug196EncryptionStatusReportsUnset: a fresh cluster with no
// passphrase ever set → status "unset". Also pins HTTP 200 (NOT 404
// from the catch-all — that's the whole bug).
func TestBug196EncryptionStatusReportsUnset(t *testing.T) {
	srv := &Server{
		Addr:      pickFreeAddr(t),
		Store:     store.NewInMemory(),
		Client:    newFakeRESTClient(t),
		Namespace: testRESTNamespace,
	}

	base, stop := startServerCustom(t, srv)
	defer stop()

	got := requirePassphraseStatus(t, base)
	if got != "unset" {
		t.Errorf("status on empty cluster: got %q, want %q", got, "unset")
	}
}

// TestBug196EncryptionStatusReportsUnlocked: POST a passphrase →
// handler flips `passphraseUnlocked` to true → GET status reports
// "unlocked".
func TestBug196EncryptionStatusReportsUnlocked(t *testing.T) {
	srv := &Server{
		Addr:      pickFreeAddr(t),
		Store:     store.NewInMemory(),
		Client:    newFakeRESTClient(t),
		Namespace: testRESTNamespace,
	}

	base, stop := startServerCustom(t, srv)
	defer stop()

	resp := httpPost(t, base+"/v1/encryption/passphrase",
		[]byte(`{"new_passphrase":"hunter2-hunter2"}`))

	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("seed POST: got %d, want 201", resp.StatusCode)
	}

	got := requirePassphraseStatus(t, base)
	if got != "unlocked" {
		t.Errorf("status after create: got %q, want %q", got, "unlocked")
	}
}

// TestBug196EncryptionStatusReportsSetButLocked: simulate the
// post-restart state — the Secret carries a passphrase but the
// in-memory unlock flag is false. Status MUST be "locked" so the
// operator knows to run `linstor encryption enter-passphrase`.
//
// We retain a direct pointer to the Server so the test can flip the
// in-memory unlock flag back to false without going through the
// HTTP wire — the field is the source-of-truth for the "locked vs
// unlocked" distinction and there's no PATCH verb that lands the
// flag in `false` (the wrong-passphrase PATCH path leaves the flag
// at its prior value to avoid leaking auth state).
func TestBug196EncryptionStatusReportsSetButLocked(t *testing.T) {
	srv := &Server{
		Addr:      pickFreeAddr(t),
		Store:     store.NewInMemory(),
		Client:    newFakeRESTClient(t),
		Namespace: testRESTNamespace,
	}

	base, stop := startServerCustom(t, srv)
	defer stop()

	resp := httpPost(t, base+"/v1/encryption/passphrase",
		[]byte(`{"new_passphrase":"hunter2-hunter2"}`))

	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("seed POST: got %d, want 201", resp.StatusCode)
	}

	// Simulate a controller restart: passphrase Secret survives,
	// in-memory unlock flag resets to zero (false).
	srv.passphraseUnlocked.Store(false)

	got := requirePassphraseStatus(t, base)
	if got != "locked" {
		t.Errorf("status after simulated restart: got %q, want %q", got, "locked")
	}
}

// requirePassphraseStatus GETs `/v1/encryption/passphrase` and
// returns the single `status` field from the upstream envelope. Pins
// both the HTTP status (200, NOT the 404 the catch-all emits pre-fix)
// AND the wire shape — a 200 with the wrong field name is just as
// broken as no route.
func requirePassphraseStatus(t *testing.T, base string) string {
	t.Helper()

	resp := httpGet(t, base+"/v1/encryption/passphrase")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status code: got %d, want 200", resp.StatusCode)
	}

	var arr []struct {
		Status string `json:"status"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&arr); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	if len(arr) != 1 {
		t.Fatalf("response array length: got %d, want 1", len(arr))
	}

	return arr[0].Status
}
