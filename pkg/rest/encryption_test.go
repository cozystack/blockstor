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

// TestPassphraseEnterRequiresExisting: PATCH unlocks an existing
// cluster passphrase. Without one set yet → 412 (precondition).
func TestPassphraseEnterRequiresExisting(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(map[string]string{"new_passphrase": "secret"})

	resp := httpPatch(t, base+"/v1/encryption/passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Errorf("status: got %d, want 412", resp.StatusCode)
	}
}

// TestPassphraseCreateThenEnter: POST creates the cluster passphrase;
// PATCH then unlocks with that same passphrase.
func TestPassphraseCreateThenEnter(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	createBody, _ := json.Marshal(map[string]string{"new_passphrase": "secret"})
	resp := httpPost(t, base+"/v1/encryption/passphrase", createBody)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create: got %d, want 201", resp.StatusCode)
	}

	enterBody, _ := json.Marshal(map[string]string{"new_passphrase": "secret"})

	enterResp := httpPatch(t, base+"/v1/encryption/passphrase", enterBody)
	_ = enterResp.Body.Close()

	if enterResp.StatusCode != http.StatusOK {
		t.Errorf("enter: got %d, want 200", enterResp.StatusCode)
	}
}

// TestPassphraseCreateTwiceConflicts: a second POST without first
// removing the existing passphrase → 409.
func TestPassphraseCreateTwiceConflicts(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(map[string]string{"new_passphrase": "secret"})

	first := httpPost(t, base+"/v1/encryption/passphrase", body)
	_ = first.Body.Close()

	second := httpPost(t, base+"/v1/encryption/passphrase", body)
	_ = second.Body.Close()

	if second.StatusCode != http.StatusConflict {
		t.Errorf("second create: got %d, want 409", second.StatusCode)
	}
}

// TestPassphraseModifyHappyPath: PUT with the right old → 200, the
// stored passphrase rotates to the new value, and a subsequent PATCH
// with the new value unlocks while the old fails. This is the
// cluster-wrapping-key rotation path documented in
// docs/layer-stack.md (per-volume LUKS headers don't re-encrypt;
// only the wrapping key in the controller's KV rotates).
func TestPassphraseModifyHappyPath(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	// Seed the initial passphrase via REST POST.
	createBody, _ := json.Marshal(map[string]string{"new_passphrase": "old-secret"})
	resp := httpPost(t, base+"/v1/encryption/passphrase", createBody)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("seed create: got %d, want 201", resp.StatusCode)
	}

	// Rotate.
	modifyBody, _ := json.Marshal(map[string]string{
		"old_passphrase": "old-secret",
		"new_passphrase": "new-secret",
	})
	modifyResp := httpPut(t, base+"/v1/encryption/passphrase", modifyBody)
	_ = modifyResp.Body.Close()

	if modifyResp.StatusCode != http.StatusOK {
		t.Fatalf("modify: got %d, want 200", modifyResp.StatusCode)
	}

	// New passphrase unlocks.
	newEnter, _ := json.Marshal(map[string]string{"new_passphrase": "new-secret"})
	newResp := httpPatch(t, base+"/v1/encryption/passphrase", newEnter)
	_ = newResp.Body.Close()

	if newResp.StatusCode != http.StatusOK {
		t.Errorf("PATCH with new passphrase: got %d, want 200", newResp.StatusCode)
	}

	// Old passphrase no longer works.
	oldEnter, _ := json.Marshal(map[string]string{"new_passphrase": "old-secret"})
	oldResp := httpPatch(t, base+"/v1/encryption/passphrase", oldEnter)
	_ = oldResp.Body.Close()

	if oldResp.StatusCode == http.StatusOK {
		t.Errorf("PATCH with stale passphrase must fail; got 200")
	}
}

// TestPassphraseModifyWrongOld: PUT with the wrong old → 403; the
// stored passphrase must NOT change.
func TestPassphraseModifyWrongOld(t *testing.T) {
	st := store.NewInMemory()
	base, stop := startServerWithStore(t, st)
	defer stop()

	createBody, _ := json.Marshal(map[string]string{"new_passphrase": "real-secret"})
	resp := httpPost(t, base+"/v1/encryption/passphrase", createBody)
	_ = resp.Body.Close()

	body, _ := json.Marshal(map[string]string{
		"old_passphrase": "wrong-guess",
		"new_passphrase": "would-be-new",
	})
	modifyResp := httpPut(t, base+"/v1/encryption/passphrase", body)
	_ = modifyResp.Body.Close()

	if modifyResp.StatusCode != http.StatusForbidden {
		t.Fatalf("modify with wrong old: got %d, want 403", modifyResp.StatusCode)
	}

	// Old must still unlock — rotation didn't take effect.
	enterBody, _ := json.Marshal(map[string]string{"new_passphrase": "real-secret"})
	enterResp := httpPatch(t, base+"/v1/encryption/passphrase", enterBody)
	_ = enterResp.Body.Close()

	if enterResp.StatusCode != http.StatusOK {
		t.Errorf("real passphrase no longer unlocks after failed modify: got %d", enterResp.StatusCode)
	}
}

// TestPassphraseModifyBeforeCreate: PUT before any passphrase has
// been created → 412 (Precondition Failed). Pins the "you can't
// modify what doesn't exist" path.
func TestPassphraseModifyBeforeCreate(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(map[string]string{
		"old_passphrase": "anything",
		"new_passphrase": "anything",
	})
	resp := httpPut(t, base+"/v1/encryption/passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Errorf("PUT before POST: got %d, want 412", resp.StatusCode)
	}
}

// TestPassphraseModifyBadJSON: malformed body → 400 before touching
// the store.
func TestPassphraseModifyBadJSON(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPut(t, base+"/v1/encryption/passphrase", []byte("{not json"))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}
