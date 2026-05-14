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
	"context"
	"encoding/json"
	"net/http"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
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

// TestPassphraseCreateConflictsOnDifferent: a second POST with a
// DIFFERENT passphrase against an established cluster → 409. Pins
// scenario 6.W12's "POST is not how operators rotate" rule; the PUT
// endpoint exists for rotation and requires the old passphrase.
func TestPassphraseCreateConflictsOnDifferent(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	first, _ := json.Marshal(map[string]string{"new_passphrase": "secret"})
	firstResp := httpPost(t, base+"/v1/encryption/passphrase", first)
	_ = firstResp.Body.Close()

	if firstResp.StatusCode != http.StatusCreated {
		t.Fatalf("first create: got %d, want 201", firstResp.StatusCode)
	}

	second, _ := json.Marshal(map[string]string{"new_passphrase": "different"})
	secondResp := httpPost(t, base+"/v1/encryption/passphrase", second)
	_ = secondResp.Body.Close()

	if secondResp.StatusCode != http.StatusConflict {
		t.Errorf("second create with different passphrase: got %d, want 409", secondResp.StatusCode)
	}
}

// TestPassphraseCreateSameIdempotent: a second POST with the SAME
// passphrase against an established cluster → 200. Pins scenario
// 6.W12's "idempotent on same passphrase" contract so a retried
// `linstor encryption create-passphrase` (e.g. controller crash-loop
// during bootstrap) doesn't blow up the bootstrapper.
func TestPassphraseCreateSameIdempotent(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(map[string]string{"new_passphrase": "secret"})

	first := httpPost(t, base+"/v1/encryption/passphrase", body)
	_ = first.Body.Close()

	if first.StatusCode != http.StatusCreated {
		t.Fatalf("first create: got %d, want 201", first.StatusCode)
	}

	second := httpPost(t, base+"/v1/encryption/passphrase", body)
	_ = second.Body.Close()

	if second.StatusCode != http.StatusOK {
		t.Errorf("idempotent same-passphrase create: got %d, want 200", second.StatusCode)
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

// TestPassphraseModifyWrongOld: PUT with the wrong old → 401
// (Unauthorized); the stored passphrase must NOT change. Scenario
// 6.W14 narrows upstream's historical 403 down to 401 — symmetric
// with the W13 PATCH-mismatch surface so `linstor encryption
// modify-passphrase` and `linstor encryption enter-passphrase`
// can share the same "wrong passphrase" CLI error string keyed
// off the status code. A regression that flipped this back to
// 200 would silently rotate the cluster master key to an
// attacker-supplied value after a single guess.
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

	if modifyResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("modify with wrong old: got %d, want 401", modifyResp.StatusCode)
	}

	// Old must still unlock — rotation didn't take effect.
	enterBody, _ := json.Marshal(map[string]string{"new_passphrase": "real-secret"})
	enterResp := httpPatch(t, base+"/v1/encryption/passphrase", enterBody)
	_ = enterResp.Body.Close()

	if enterResp.StatusCode != http.StatusOK {
		t.Errorf("real passphrase no longer unlocks after failed modify: got %d", enterResp.StatusCode)
	}
}

// TestPassphraseModifySameNewIdempotent: PUT with old == new and the
// right old → 200 + MASK_INFO envelope. Scenario 6.W14 pins the
// idempotent no-op so a retried `linstor encryption modify-passphrase`
// (e.g. operator runs the command twice from a script after a 502
// from a transient apiserver blip) lands on the same final state
// rather than rotating-then-failing on the second hop. Same-value
// rotation MUST also flip the in-memory unlock flag (the operator
// just demonstrated proof-of-knowledge), atomic with the W12/W13
// unlock-on-success paths.
func TestPassphraseModifySameNewIdempotent(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	createBody, _ := json.Marshal(map[string]string{"new_passphrase": "stay-the-same"})
	resp := httpPost(t, base+"/v1/encryption/passphrase", createBody)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("seed create: got %d, want 201", resp.StatusCode)
	}

	body, _ := json.Marshal(map[string]string{
		"old_passphrase": "stay-the-same",
		"new_passphrase": "stay-the-same",
	})

	modifyResp := httpPut(t, base+"/v1/encryption/passphrase", body)
	defer func() { _ = modifyResp.Body.Close() }()

	if modifyResp.StatusCode != http.StatusOK {
		t.Fatalf("same-new modify: got %d, want 200", modifyResp.StatusCode)
	}

	var rcs []struct {
		RetCode int64  `json:"ret_code"`
		Message string `json:"message,omitempty"`
	}

	if err := json.NewDecoder(modifyResp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) != 1 {
		t.Fatalf("envelope entries: got %d, want 1", len(rcs))
	}

	const maskInfo int64 = 0x0001_0000_0000
	if rcs[0].RetCode&maskInfo == 0 {
		t.Errorf("ret_code = %x, want MASK_INFO bit set", rcs[0].RetCode)
	}

	// Same-new still unlocks: subsequent PATCH with the
	// unchanged passphrase MUST succeed (the unlock flag was
	// flipped by the no-op rotation).
	enterBody, _ := json.Marshal(map[string]string{"new_passphrase": "stay-the-same"})
	enterResp := httpPatch(t, base+"/v1/encryption/passphrase", enterBody)
	_ = enterResp.Body.Close()

	if enterResp.StatusCode != http.StatusOK {
		t.Errorf("post-noop PATCH: got %d, want 200", enterResp.StatusCode)
	}
}

// TestPassphraseModifyHappyPathEnvelope pins that a successful
// rotation returns a `[]ApiCallRc` body with the MASK_INFO bit set
// — symmetric with the rest of the write-side endpoints (snapshots,
// storage pools, etc.) and what python-linstor's CLI loop decodes
// to print the success line. Scenario 6.W14 explicitly calls out
// the envelope so a regression that returned an empty 200 body
// would silently break the CLI's "Master passphrase modified."
// log line.
func TestPassphraseModifyHappyPathEnvelope(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	createBody, _ := json.Marshal(map[string]string{"new_passphrase": "v1"})
	resp := httpPost(t, base+"/v1/encryption/passphrase", createBody)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("seed create: got %d, want 201", resp.StatusCode)
	}

	modifyBody, _ := json.Marshal(map[string]string{
		"old_passphrase": "v1",
		"new_passphrase": "v2",
	})

	modifyResp := httpPut(t, base+"/v1/encryption/passphrase", modifyBody)
	defer func() { _ = modifyResp.Body.Close() }()

	if modifyResp.StatusCode != http.StatusOK {
		t.Fatalf("modify: got %d, want 200", modifyResp.StatusCode)
	}

	var rcs []struct {
		RetCode int64  `json:"ret_code"`
		Message string `json:"message,omitempty"`
	}

	if err := json.NewDecoder(modifyResp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) != 1 {
		t.Fatalf("envelope entries: got %d, want 1", len(rcs))
	}

	const maskInfo int64 = 0x0001_0000_0000
	if rcs[0].RetCode&maskInfo == 0 {
		t.Errorf("ret_code = %x, want MASK_INFO bit set", rcs[0].RetCode)
	}

	if rcs[0].Message == "" {
		t.Errorf("envelope message empty; want an operator-facing line")
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

// TestPassphraseCreateBadJSON: malformed body → 400.
func TestPassphraseCreateBadJSON(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/encryption/passphrase", []byte("{not-json"))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestPassphraseCreateMissingNew: POST with empty new_passphrase →
// 400. Pinned because a regression that allowed empty values would
// "unlock" the cluster with the empty string and silently downgrade
// at-rest encryption to no-op.
func TestPassphraseCreateMissingNew(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, err := json.Marshal(passphraseRequest{}) // NewPassphrase omitted
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/encryption/passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestPassphraseEnterBadJSON: malformed body → 400.
func TestPassphraseEnterBadJSON(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPatch(t, base+"/v1/encryption/passphrase", []byte("{not-json"))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestPassphraseModifyWithoutCreate: PUT before any prior POST →
// 412 Precondition Failed. The operator must POST first to establish
// the cluster passphrase; this surface is what golinstor's
// `linstor encryption modify` checks.
func TestPassphraseModifyWithoutCreate(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, err := json.Marshal(passphraseRequest{
		OldPassphrase: "doesnt-matter",
		NewPassphrase: "new",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/encryption/passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Errorf("status: got %d, want 412", resp.StatusCode)
	}
}

// TestPassphraseEnterMismatch: PATCH with the wrong passphrase
// against an established cluster → 401 (unauthorized). Scenario
// 6.W13 narrows upstream's historical 403 down to 401 — the
// request was syntactically allowed but the proof-of-knowledge
// failed, which is precisely what 401 carries (vs. 403's
// "authenticated but disallowed"). `linstor encryption
// enter-passphrase` keys off the status code to print "wrong
// passphrase" rather than "permission denied". Pinned because the
// passphrase is the master key for at-rest LUKS volume keys — a
// regression that flipped this to 200 would silently grant unlock
// access to any caller, breaking the encryption boundary.
func TestPassphraseEnterMismatch(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	createBody, _ := json.Marshal(map[string]string{"new_passphrase": "right"})
	createResp := httpPost(t, base+"/v1/encryption/passphrase", createBody)
	_ = createResp.Body.Close()

	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("seed create: got %d, want 201", createResp.StatusCode)
	}

	wrongBody, _ := json.Marshal(map[string]string{"new_passphrase": "WRONG"})

	resp := httpPatch(t, base+"/v1/encryption/passphrase", wrongBody)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status: got %d, want 401 (mismatched passphrase)", resp.StatusCode)
	}
}

// passphraseSecretTestNamespace is the namespace the Secret-path
// tests pin the controller to. The exact value doesn't matter so
// long as it's stable across reads + writes.
const passphraseSecretTestNamespace = "blockstor-system"

// newSecretPathServer constructs a Server wired with a fake
// controller-runtime client + a Store, so the Secret-backed
// passphrase path is exercised end-to-end (POST → Secret created,
// PATCH → Secret read).
func newSecretPathServer(t *testing.T) *Server {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1 to scheme: %v", err)
	}

	if err := blockstoriov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("blockstor to scheme: %v", err)
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	return &Server{
		Addr:      pickFreeAddr(t),
		Store:     store.NewInMemory(),
		Client:    cli,
		Namespace: passphraseSecretTestNamespace,
	}
}

// TestPassphraseSecretCreateLandsInSecret POSTs against the
// Secret-backed path and asserts the data lands in a native
// Secret rather than the KV store.
func TestPassphraseSecretCreateLandsInSecret(t *testing.T) {
	srv := newSecretPathServer(t)
	cli := srv.Client

	base, stop := startServerCustom(t, srv)
	defer stop()

	body, _ := json.Marshal(map[string]string{"new_passphrase": "topsecret"})
	resp := httpPost(t, base+"/v1/encryption/passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create: got %d, want 201", resp.StatusCode)
	}

	var sec corev1.Secret

	err := cli.Get(context.Background(), client.ObjectKey{
		Namespace: passphraseSecretTestNamespace,
		Name:      defaultPassphraseSecretName,
	}, &sec)
	if err != nil {
		t.Fatalf("Secret should exist after POST: %v", err)
	}

	if got := string(sec.Data[passphraseSecretKey]); got != "topsecret" {
		t.Errorf("Secret data: got %q, want %q", got, "topsecret")
	}
}

// TestPassphraseSecretCreateIdempotentSameValue pins that POSTing
// the same passphrase twice against the Secret-backed path returns
// 200 on the second call (scenario 6.W12 idempotency) and leaves the
// Secret data byte-for-byte unchanged.
func TestPassphraseSecretCreateIdempotentSameValue(t *testing.T) {
	srv := newSecretPathServer(t)
	cli := srv.Client

	base, stop := startServerCustom(t, srv)
	defer stop()

	body, _ := json.Marshal(map[string]string{"new_passphrase": "topsecret"})

	first := httpPost(t, base+"/v1/encryption/passphrase", body)
	_ = first.Body.Close()

	if first.StatusCode != http.StatusCreated {
		t.Fatalf("first create: got %d, want 201", first.StatusCode)
	}

	second := httpPost(t, base+"/v1/encryption/passphrase", body)
	_ = second.Body.Close()

	if second.StatusCode != http.StatusOK {
		t.Errorf("idempotent re-POST: got %d, want 200", second.StatusCode)
	}

	var sec corev1.Secret

	err := cli.Get(context.Background(), client.ObjectKey{
		Namespace: passphraseSecretTestNamespace,
		Name:      defaultPassphraseSecretName,
	}, &sec)
	if err != nil {
		t.Fatalf("Secret missing after idempotent re-POST: %v", err)
	}

	if got := string(sec.Data[passphraseSecretKey]); got != "topsecret" {
		t.Errorf("Secret data: got %q, want %q", got, "topsecret")
	}
}

// TestPassphraseSecretCreateConflictsOnDifferentValue pins that
// POSTing a different passphrase against an established cluster
// returns 409 + leaves the Secret data unchanged. Scenario 6.W12
// reserves rotation for the PUT (modify) endpoint.
func TestPassphraseSecretCreateConflictsOnDifferentValue(t *testing.T) {
	srv := newSecretPathServer(t)
	cli := srv.Client

	base, stop := startServerCustom(t, srv)
	defer stop()

	original, _ := json.Marshal(map[string]string{"new_passphrase": "original"})

	resp := httpPost(t, base+"/v1/encryption/passphrase", original)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("seed create: got %d, want 201", resp.StatusCode)
	}

	rotate, _ := json.Marshal(map[string]string{"new_passphrase": "rotated"})

	second := httpPost(t, base+"/v1/encryption/passphrase", rotate)
	_ = second.Body.Close()

	if second.StatusCode != http.StatusConflict {
		t.Errorf("conflicting create: got %d, want 409", second.StatusCode)
	}

	var sec corev1.Secret

	err := cli.Get(context.Background(), client.ObjectKey{
		Namespace: passphraseSecretTestNamespace,
		Name:      defaultPassphraseSecretName,
	}, &sec)
	if err != nil {
		t.Fatalf("Secret missing: %v", err)
	}

	if got := string(sec.Data[passphraseSecretKey]); got != "original" {
		t.Errorf("Secret data must NOT rotate via POST: got %q, want %q", got, "original")
	}
}

// TestPassphraseSecretEnterMatchesSecret PATCHes against the
// Secret-backed path and asserts the right passphrase unlocks +
// the wrong one is forbidden — symmetric to the KV-path tests
// but proving the Secret is the source of truth.
func TestPassphraseSecretEnterMatchesSecret(t *testing.T) {
	srv := newSecretPathServer(t)

	base, stop := startServerCustom(t, srv)
	defer stop()

	createBody, _ := json.Marshal(map[string]string{"new_passphrase": "right"})
	resp := httpPost(t, base+"/v1/encryption/passphrase", createBody)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("seed create: got %d, want 201", resp.StatusCode)
	}

	rightBody, _ := json.Marshal(map[string]string{"new_passphrase": "right"})

	okResp := httpPatch(t, base+"/v1/encryption/passphrase", rightBody)
	_ = okResp.Body.Close()

	if okResp.StatusCode != http.StatusOK {
		t.Errorf("PATCH with right passphrase: got %d, want 200", okResp.StatusCode)
	}

	wrongBody, _ := json.Marshal(map[string]string{"new_passphrase": "wrong"})

	noResp := httpPatch(t, base+"/v1/encryption/passphrase", wrongBody)
	_ = noResp.Body.Close()

	if noResp.StatusCode != http.StatusUnauthorized {
		t.Errorf("PATCH with wrong passphrase: got %d, want 401", noResp.StatusCode)
	}
}

// TestPassphraseSecretModifyRotatesSecret PUTs against the
// Secret-backed path and asserts the rotation lands in the
// Secret's data — proving writePassphraseSecret does an Update
// (not a Create) on the second call.
func TestPassphraseSecretModifyRotatesSecret(t *testing.T) {
	srv := newSecretPathServer(t)
	cli := srv.Client

	base, stop := startServerCustom(t, srv)
	defer stop()

	createBody, _ := json.Marshal(map[string]string{"new_passphrase": "v1"})
	resp := httpPost(t, base+"/v1/encryption/passphrase", createBody)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("seed create: got %d, want 201", resp.StatusCode)
	}

	modifyBody, _ := json.Marshal(map[string]string{
		"old_passphrase": "v1",
		"new_passphrase": "v2",
	})

	modifyResp := httpPut(t, base+"/v1/encryption/passphrase", modifyBody)
	_ = modifyResp.Body.Close()

	if modifyResp.StatusCode != http.StatusOK {
		t.Fatalf("modify: got %d, want 200", modifyResp.StatusCode)
	}

	var sec corev1.Secret

	err := cli.Get(context.Background(), client.ObjectKey{
		Namespace: passphraseSecretTestNamespace,
		Name:      defaultPassphraseSecretName,
	}, &sec)
	if err != nil {
		t.Fatalf("Secret missing after rotate: %v", err)
	}

	if got := string(sec.Data[passphraseSecretKey]); got != "v2" {
		t.Errorf("rotated Secret data: got %q, want %q", got, "v2")
	}
}

// TestPassphraseSecretHonoursControllerConfigRef pins that the
// ControllerConfig.Spec.PassphraseSecretRef.Name override routes
// reads + writes to the configured Secret rather than the default.
func TestPassphraseSecretHonoursControllerConfigRef(t *testing.T) {
	srv := newSecretPathServer(t)
	cli := srv.Client

	const customName = "my-org-passphrase"

	err := cli.Create(context.Background(), &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			PassphraseSecretRef: &blockstoriov1alpha1.PassphraseSecretRef{Name: customName},
		},
	})
	if err != nil {
		t.Fatalf("seed ControllerConfig: %v", err)
	}

	base, stop := startServerCustom(t, srv)
	defer stop()

	body, _ := json.Marshal(map[string]string{"new_passphrase": "x"})
	resp := httpPost(t, base+"/v1/encryption/passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create: got %d, want 201", resp.StatusCode)
	}

	// The named Secret must exist.
	var sec corev1.Secret

	err = cli.Get(context.Background(), client.ObjectKey{
		Namespace: passphraseSecretTestNamespace, Name: customName,
	}, &sec)
	if err != nil {
		t.Fatalf("custom-named Secret missing: %v", err)
	}

	// The default-named one must NOT have been touched.
	err = cli.Get(context.Background(), client.ObjectKey{
		Namespace: passphraseSecretTestNamespace, Name: defaultPassphraseSecretName,
	}, &sec)
	if err == nil {
		t.Errorf("default-named Secret was created when override was set")
	}
}
