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

// bug187Passphrase is the v10 canary value the deny-list MUST scrub
// from every read-side endpoint family Bugs 187-190 cover. Distinct
// from bug181Passphrase so a regression on either fix surfaces
// without spillover.
const bug187Passphrase = "hunter2-v10-poc"

// assertBug187NoLeak asserts the response body does NOT carry the
// v10 canary substring. Substring match is the operationally
// meaningful predicate: any cleartext occurrence is greppable.
func assertBug187NoLeak(t *testing.T, label string, body []byte) {
	t.Helper()

	if strings.Contains(string(body), bug187Passphrase) {
		t.Errorf("%s leaked passphrase %q in body: %s", label, bug187Passphrase, body)
	}
}

// assertBug187HasRedaction confirms the redaction marker is present
// in the body. Same shape as the Bug 181 helper — accepts both
// the bare `<redacted>` and the HTML-escaped form Go's
// `json.Encoder` emits by default.
func assertBug187HasRedaction(t *testing.T, label string, body []byte) {
	t.Helper()

	raw := string(body)
	if !strings.Contains(raw, redactedPropValue) &&
		!strings.Contains(raw, htmlEscapedRedactedMarker) {
		t.Errorf("%s missing redaction marker in body: %s", label, body)
	}
}

// seedLinstorRemote seeds the in-memory remote registry by POSTing
// a remote through the create handler. The Bug 119 envelope (201 +
// `[]APICallRc`) is asserted as a smoke check so the seed only
// proceeds when the create surface itself is healthy.
func seedLinstorRemote(t *testing.T, base, name, urlStr, passphrase string) {
	t.Helper()

	body, err := json.Marshal(map[string]string{
		"remote_name": name,
		"url":         urlStr,
		"passphrase":  passphrase,
	})
	if err != nil {
		t.Fatalf("marshal remote body: %v", err)
	}

	resp := httpPost(t, base+"/v1/remotes/linstor", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("seed remote: got %d, want 201", resp.StatusCode)
	}
}

// -----------------------------------------------------------------------
// Bug 187 — Remote list / Linstor-remote get
// -----------------------------------------------------------------------

// TestBug187RemoteListRedactsPassphrase covers `GET /v1/remotes`. The
// in-memory `linstorRemoteRegistry` returned every entry verbatim,
// so an operator with read-only access could grep the passphrase
// out of the envelope `linstor_remotes[]` slot.
func TestBug187RemoteListRedactsPassphrase(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	seedLinstorRemote(t, base, "peer187a", "http://peer.example:3370", bug187Passphrase)

	resp := httpGet(t, base+"/v1/remotes")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBug187NoLeak(t, "GET /v1/remotes", body)
	assertBug187HasRedaction(t, "GET /v1/remotes", body)

	// Key preservation — operators must still see "encryption IS
	// configured" so the redacted view doesn't look like an absent
	// secret. The `passphrase` JSON key is what tooling greps for.
	if !strings.Contains(string(body), `"passphrase"`) {
		t.Errorf("GET /v1/remotes dropped passphrase key entirely; body=%s", body)
	}
}

// TestBug187RemoteLinstorGetRedactsPassphrase covers the typed-array
// `GET /v1/remotes/linstor`. Same registry, same leak — pin both
// surfaces so a partial fix (envelope but not typed) fails the test.
func TestBug187RemoteLinstorGetRedactsPassphrase(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	seedLinstorRemote(t, base, "peer187b", "http://peer.example:3370", bug187Passphrase)

	resp := httpGet(t, base+"/v1/remotes/linstor")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBug187NoLeak(t, "GET /v1/remotes/linstor", body)
	assertBug187HasRedaction(t, "GET /v1/remotes/linstor", body)

	if !strings.Contains(string(body), `"passphrase"`) {
		t.Errorf("GET /v1/remotes/linstor dropped passphrase key entirely; body=%s", body)
	}
}

// -----------------------------------------------------------------------
// Bug 188 — RD-scoped Resource list / get
// -----------------------------------------------------------------------

// seedRDScopedResourceWithSensitiveProps stages an RD + one Resource
// on a node whose `Props` bag carries the v10 canary. `/v1/view/
// resources` already redacts via Bug 115, but the RD-scoped read
// path (`/v1/resource-definitions/{rd}/resources[/{node}]`) bypassed
// the same scrub.
func seedRDScopedResourceWithSensitiveProps(t *testing.T, st store.Store, rd, node string) {
	t.Helper()

	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: rd,
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     rd,
		NodeName: node,
		Props: map[string]string{
			"DrbdOptions/EncryptPassphrase": bug187Passphrase,
		},
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}
}

// TestBug188RDScopedResourceListRedactsPassphrase covers
// `GET /v1/resource-definitions/{rd}/resources`. Sibling
// `/v1/view/resources` already scrubs via Bug 115; the RD-scoped
// read path was missed.
func TestBug188RDScopedResourceListRedactsPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedRDScopedResourceWithSensitiveProps(t, st, "rd188", "n188")

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/rd188/resources")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBug187NoLeak(t, "GET RD-scoped resources list", body)
	assertBug187HasRedaction(t, "GET RD-scoped resources list", body)
}

// TestBug188RDScopedResourceGetRedactsPassphrase covers
// `GET /v1/resource-definitions/{rd}/resources/{node}`. The
// per-replica read path emits the same wire shape so the same
// redaction MUST land here.
func TestBug188RDScopedResourceGetRedactsPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedRDScopedResourceWithSensitiveProps(t, st, "rd188", "n188")

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/rd188/resources/n188")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBug187NoLeak(t, "GET RD-scoped resource", body)
	assertBug187HasRedaction(t, "GET RD-scoped resource", body)
}

// -----------------------------------------------------------------------
// Bug 189 — KV store list / get
// -----------------------------------------------------------------------

// seedKVWithSensitiveProps stages a single KV instance via the PUT
// surface so the bag persists through the process-local store. Used
// by both list + get assertions.
func seedKVWithSensitiveProps(t *testing.T, base, instance, sensitiveKey string) {
	t.Helper()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{
			sensitiveKey: bug187Passphrase,
			"benign":     "value",
		},
	})
	if err != nil {
		t.Fatalf("marshal KV body: %v", err)
	}

	resp := httpPut(t, base+"/v1/key-value-store/"+instance, body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("seed KV: got %d, want 200", resp.StatusCode)
	}
}

// TestBug189KVSListRedactsSensitiveProps covers
// `GET /v1/key-value-store`. linstor-csi stamps backup credentials
// here, so the leak is real exposure.
func TestBug189KVSListRedactsSensitiveProps(t *testing.T) {
	// kvBag is process-local; reset under lock so test ordering can't
	// leak entries seeded by sibling tests in the same package.
	kvBagMu.Lock()
	kvBag = map[string]map[string]string{}
	kvBagMu.Unlock()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	seedKVWithSensitiveProps(t, base, "csi-189-list", "backup-passphrase")

	resp := httpGet(t, base+"/v1/key-value-store")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBug187NoLeak(t, "GET KV list", body)
	assertBug187HasRedaction(t, "GET KV list", body)

	if !strings.Contains(string(body), "backup-passphrase") {
		t.Errorf("GET KV list dropped passphrase key entirely; body=%s", body)
	}
}

// TestBug189KVSGetRedactsSensitiveProps covers
// `GET /v1/key-value-store/{instance}`. The single-element-array
// shape (Bug 121) emits the same Props map — so the same redaction
// MUST land.
func TestBug189KVSGetRedactsSensitiveProps(t *testing.T) {
	kvBagMu.Lock()
	kvBag = map[string]map[string]string{}
	kvBagMu.Unlock()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	seedKVWithSensitiveProps(t, base, "csi-189-get", "backup-password")

	resp := httpGet(t, base+"/v1/key-value-store/csi-189-get")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBug187NoLeak(t, "GET KV instance", body)
	assertBug187HasRedaction(t, "GET KV instance", body)

	if !strings.Contains(string(body), "backup-password") {
		t.Errorf("GET KV instance dropped passphrase key entirely; body=%s", body)
	}
}

// -----------------------------------------------------------------------
// Bug 190 — VolumeGroup list / get (nested under RG)
// -----------------------------------------------------------------------

// seedRGWithVGSensitiveProps stages an RG containing one VolumeGroup
// whose Props carries the v10 canary. Sibling Bug 181 redacts RG
// itself; the nested VG was missed.
func seedRGWithVGSensitiveProps(t *testing.T, st store.Store, rg string, vlmNr int32) {
	t.Helper()

	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: rg,
		VolumeGroups: []apiv1.VolumeGroup{{
			VolumeNumber: vlmNr,
			Props: map[string]string{
				"DrbdOptions/EncryptPassphrase": bug187Passphrase,
			},
		}},
	}); err != nil {
		t.Fatalf("seed RG with VG: %v", err)
	}
}

// TestBug190VolumeGroupListRedactsPassphrase covers
// `GET /v1/resource-groups/{rg}/volume-groups`. Sibling RG-level
// redaction landed in Bug 181; the VG list nested under the RG
// was missed.
func TestBug190VolumeGroupListRedactsPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedRGWithVGSensitiveProps(t, st, "rg190", 0)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-groups/rg190/volume-groups")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBug187NoLeak(t, "GET VG list", body)
	assertBug187HasRedaction(t, "GET VG list", body)
}

// TestBug190VolumeGroupGetRedactsPassphrase covers
// `GET /v1/resource-groups/{rg}/volume-groups/{vlmNr}`. Per-VG
// read surface from the same nested registry.
func TestBug190VolumeGroupGetRedactsPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedRGWithVGSensitiveProps(t, st, "rg190", 0)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-groups/rg190/volume-groups/0")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBug187NoLeak(t, "GET VG", body)
	assertBug187HasRedaction(t, "GET VG", body)
}
