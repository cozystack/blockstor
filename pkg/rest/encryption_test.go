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
