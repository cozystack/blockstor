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

// TestBug121KVSModifyReturnsEnvelope pins Bug 121: PUT /v1/key-value-store/{name}
// with a well-formed GenericPropsModify body must return 200 + a LINSTOR
// `[]APICallRc` envelope, NOT 200 with an empty body. python-linstor's
// KeyValueStore.modify codepath JSON-decodes the body unconditionally —
// the pre-fix empty body tripped `json.loads("")` and surfaced as
//
//	Error: Unable to parse REST json data: Expecting value: line 1 column 1 (char 0)
//
// at the operator's CLI, even though the underlying bag had been
// mutated successfully. The success ret_code must include the
// MASK_INFO bit so the CLI's `rc.is_success()` branch fires.
func TestBug121KVSModifyReturnsEnvelope(t *testing.T) {
	kvBagMu.Lock()
	kvBag = map[string]map[string]string{}
	kvBagMu.Unlock()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"X": "y"},
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	resp := httpPut(t, base+"/v1/key-value-store/testkv1", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if decodeErr := json.NewDecoder(resp.Body).Decode(&rcs); decodeErr != nil {
		t.Fatalf("decode envelope: %v", decodeErr)
	}

	if len(rcs) == 0 {
		t.Fatal("envelope: got 0 entries, want at least 1")
	}

	if rcs[0].RetCode&maskInfo == 0 {
		t.Errorf("ret_code: got %#x, want MASK_INFO bit set", rcs[0].RetCode)
	}

	if rcs[0].Message == "" {
		t.Error("message: got empty, want operator-visible text")
	}

	// Underlying mutation must still have been applied.
	kvBagMu.Lock()
	got := kvBag["testkv1"]["X"]
	kvBagMu.Unlock()

	if got != "y" {
		t.Errorf("kvBag[testkv1][X]: got %q, want %q", got, "y")
	}
}

// TestBug121KVSDeleteReturnsEnvelope pins the DELETE half of Bug 121:
// DELETE /v1/key-value-store/{name} previously returned 200 with an
// empty body, which broke python-linstor's `delete` codepath the same
// way modify did. Must return 200 + envelope with MASK_INFO ret_code.
func TestBug121KVSDeleteReturnsEnvelope(t *testing.T) {
	kvBagMu.Lock()
	kvBag = map[string]map[string]string{
		"testkv1": {"X": "y"},
	}
	kvBagMu.Unlock()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t, base+"/v1/key-value-store/testkv1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if decodeErr := json.NewDecoder(resp.Body).Decode(&rcs); decodeErr != nil {
		t.Fatalf("decode envelope: %v", decodeErr)
	}

	if len(rcs) == 0 {
		t.Fatal("envelope: got 0 entries, want at least 1")
	}

	if rcs[0].RetCode&maskInfo == 0 {
		t.Errorf("ret_code: got %#x, want MASK_INFO bit set", rcs[0].RetCode)
	}

	if rcs[0].Message == "" {
		t.Error("message: got empty, want operator-visible text")
	}

	// Underlying instance must actually be gone.
	kvBagMu.Lock()
	_, stillThere := kvBag["testkv1"]
	kvBagMu.Unlock()

	if stillThere {
		t.Error("kvBag[testkv1] still present after DELETE")
	}
}

// TestBug122KVSPutRawJSONRefusedOrAccepted pins Bug 122: a PUT whose
// body has only unknown top-level keys (i.e. raw JSON-document-style
// `{"X":"y2"}` instead of the GenericPropsModify wrapper
// `{"override_props":{"X":"y2"}}`) MUST NOT silently drop the
// mutation. We pick the safer reject branch: respond with 400 + an
// envelope explaining the expected wire shape. The MASK_ERROR bit
// must be set so python-linstor's CLI prints the cause instead of
// silently succeeding.
func TestBug122KVSPutRawJSONRefusedOrAccepted(t *testing.T) {
	kvBagMu.Lock()
	kvBag = map[string]map[string]string{
		"testkv1": {"X": "y"},
	}
	kvBagMu.Unlock()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	// Raw JSON document, no override_props / delete_props /
	// delete_namespaces wrapper.
	resp := httpPut(t, base+"/v1/key-value-store/testkv1", []byte(`{"X":"y2"}`))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if decodeErr := json.NewDecoder(resp.Body).Decode(&rcs); decodeErr != nil {
		t.Fatalf("decode envelope: %v", decodeErr)
	}

	if len(rcs) == 0 {
		t.Fatal("envelope: got 0 entries, want at least 1")
	}

	// MASK_ERROR (high bit) must be set so the CLI's is_error()
	// branch prints the operator-visible cause.
	if rcs[0].RetCode >= 0 {
		t.Errorf("ret_code: got %#x, want MASK_ERROR bit set (negative int64)",
			rcs[0].RetCode)
	}

	if rcs[0].Message == "" {
		t.Error("message: got empty, want operator-visible text")
	}

	// CRITICAL: the underlying state must not have been mutated by
	// the rejected request — the pre-fix bug was a silent drop, so
	// X must still be y, not y2.
	kvBagMu.Lock()
	got := kvBag["testkv1"]["X"]
	kvBagMu.Unlock()

	if got != "y" {
		t.Errorf("kvBag[testkv1][X] after rejected raw-JSON PUT: got %q, want %q",
			got, "y")
	}
}

// TestBug122KVSPutWithProperBodyPersists confirms that the rejection
// branch in TestBug122KVSPutRawJSONRefusedOrAccepted does not break
// the happy path: a proper GenericPropsModify body still applies and
// persists the mutation, and still returns the 200+envelope shape
// Bug 121 fixes.
func TestBug122KVSPutWithProperBodyPersists(t *testing.T) {
	kvBagMu.Lock()
	kvBag = map[string]map[string]string{
		"testkv1": {"X": "y"},
	}
	kvBagMu.Unlock()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"X": "y2"},
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	resp := httpPut(t, base+"/v1/key-value-store/testkv1", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if decodeErr := json.NewDecoder(resp.Body).Decode(&rcs); decodeErr != nil {
		t.Fatalf("decode envelope: %v", decodeErr)
	}

	if len(rcs) == 0 || rcs[0].RetCode&maskInfo == 0 {
		t.Fatalf("envelope: got %+v, want one entry with MASK_INFO ret_code", rcs)
	}

	kvBagMu.Lock()
	got := kvBag["testkv1"]["X"]
	kvBagMu.Unlock()

	if got != "y2" {
		t.Errorf("kvBag[testkv1][X]: got %q, want %q", got, "y2")
	}
}
