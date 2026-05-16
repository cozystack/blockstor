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

// Bug 172 — P1 SECURITY / DATA-LOSS regression on
// PUT /v1/encryption/passphrase.
//
// Pre-fix: `handlePassphraseModify` called `req.proofOfKnowledge()`
// (which returns "" when neither `new_passphrase` nor `passphrase` is
// present) without an empty-string guard. The handler then verified
// the supplied `old_passphrase` against the stored one, fell through
// to `writePassphrase(ctx, "")`, returned 200 + "Master passphrase
// modified", and silently wiped the cluster passphrase Secret to an
// empty byte string. Every LUKS-using RD becomes un-openable on the
// next reconcile, and the operator-visible SUCCESS envelope masks the
// destruction.
//
// Sibling `handlePassphraseCreate` (line 137) already has the
// `if want == ""` guard the Bug 165 fix introduced. The asymmetry is
// the bug. The fix ports that guard verbatim into the PUT handler.
//
// These tests pin:
//   - PUT with only `old_passphrase` (no new value at all) → 400 +
//     standard `[]APICallRc` error envelope citing the missing new
//     passphrase, and the underlying Secret data MUST stay byte-for-
//     byte unchanged.
//   - PUT with `{"old_passphrase":"…","new_passphrase":""}` → 400 +
//     envelope (explicit empty on the canonical field).
//   - PUT with `{"old_passphrase":"…","passphrase":""}` → 400 +
//     envelope (explicit empty on the W13 alias).
//   - PUT with both old + non-empty new → 200 (happy-path regression
//     guard so the fix doesn't over-reject).
//   - PUT with the wrong old passphrase → 401, Secret unchanged
//     (Bug 165/W14 contract regression guard).

package rest

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// seedBug172Passphrase POSTs a known seed passphrase to the
// Secret-backed apiserver under test and asserts the Secret data
// landed correctly so subsequent assertions about the Secret state
// have a known baseline.
func seedBug172Passphrase(t *testing.T, base string, cli client.Client, seed string) {
	t.Helper()

	body, _ := json.Marshal(map[string]string{"new_passphrase": seed})

	resp := httpPost(t, base+"/v1/encryption/passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("seed create: got %d, want 201", resp.StatusCode)
	}

	var sec corev1.Secret

	err := cli.Get(context.Background(), client.ObjectKey{
		Namespace: passphraseSecretTestNamespace,
		Name:      defaultPassphraseSecretName,
	}, &sec)
	if err != nil {
		t.Fatalf("seed Secret missing: %v", err)
	}

	if got := string(sec.Data[passphraseSecretKey]); got != seed {
		t.Fatalf("seed Secret data: got %q, want %q", got, seed)
	}
}

// assertBug172SecretIntact reads the passphrase Secret and fails if
// it doesn't still hold `want`. The whole point of Bug 172's
// regression guards is to prove a PUT didn't silently rotate the
// Secret to an empty/garbage value, so the assertion runs after every
// rejection case.
func assertBug172SecretIntact(t *testing.T, cli client.Client, want string) {
	t.Helper()

	var sec corev1.Secret

	err := cli.Get(context.Background(), client.ObjectKey{
		Namespace: passphraseSecretTestNamespace,
		Name:      defaultPassphraseSecretName,
	}, &sec)
	if err != nil {
		t.Fatalf("Secret missing after rejected PUT: %v", err)
	}

	if got := string(sec.Data[passphraseSecretKey]); got != want {
		t.Fatalf("Secret data after rejected PUT: got %q, want %q (Bug 172 — silent erase regression)", got, want)
	}
}

// assertBug172ErrorEnvelope decodes the response body and fails if it
// isn't a non-empty `[]APICallRc` carrying a message. The Bug 165
// "POST refuses empty" sibling pins the same envelope shape; we hold
// the rotation surface to the same contract so python-linstor's CLI
// loop renders the operator-visible error line consistently.
func assertBug172ErrorEnvelope(t *testing.T, body io.Reader) {
	t.Helper()

	var rcs []apiv1.APICallRc

	if err := json.NewDecoder(body).Decode(&rcs); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}

	if len(rcs) == 0 || rcs[0].Message == "" {
		t.Errorf("error envelope rcs=%+v; want non-empty []APICallRc with a message", rcs)
	}
}

// TestBug172ModifyRefusesEmptyNewPassphrase is the canonical Bug 172
// red: PUT `/v1/encryption/passphrase` with only `old_passphrase` (no
// `new_passphrase` and no `passphrase` alias) MUST 400, and the
// underlying Secret MUST stay byte-for-byte at the seeded value. The
// pre-fix handler returned 200 + "Master passphrase modified" while
// silently wiping the Secret to empty bytes.
func TestBug172ModifyRefusesEmptyNewPassphrase(t *testing.T) {
	srv := newSecretPathServer(t)
	cli := srv.Client

	base, stop := startServerCustom(t, srv)
	defer stop()

	const seed = "secret-before"

	seedBug172Passphrase(t, base, cli, seed)

	// Body carries ONLY old_passphrase. Pre-fix: proofOfKnowledge() →
	// "", have == seed, have == OldPassphrase (passes auth), want !=
	// have (skips idempotent no-op), writePassphrase(ctx, "") runs,
	// 200 returned, Secret silently erased.
	body, _ := json.Marshal(map[string]string{"old_passphrase": seed})

	resp := httpPut(t, base+"/v1/encryption/passphrase", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("PUT {old_passphrase:…}: got %d, want 400 (Bug 172 — silent erase, body=%q)",
			resp.StatusCode, string(raw))
	}

	assertBug172ErrorEnvelope(t, resp.Body)

	// Operator-visible damage check: Secret MUST still hold the seed
	// value byte-for-byte. The "200 + erase" pre-fix path is exactly
	// the data-loss surface this test guards against.
	assertBug172SecretIntact(t, cli, seed)
}

// TestBug172ModifyRefusesExplicitEmptyNewPassphrase: same defect
// surface as the implicit-omission case, but with the canonical
// `new_passphrase` field explicitly set to "". A naive fix that only
// guarded the implicit-omission path (e.g. by checking
// `req.NewPassphrase == "" && req.Passphrase == ""` rather than
// `proofOfKnowledge() == ""`) could still accept this. The Bug 165
// helper centralises both into proofOfKnowledge so the same guard
// covers explicit + implicit alike.
func TestBug172ModifyRefusesExplicitEmptyNewPassphrase(t *testing.T) {
	srv := newSecretPathServer(t)
	cli := srv.Client

	base, stop := startServerCustom(t, srv)
	defer stop()

	const seed = "secret-before"

	seedBug172Passphrase(t, base, cli, seed)

	body, _ := json.Marshal(map[string]string{
		"old_passphrase": seed,
		"new_passphrase": "",
	})

	resp := httpPut(t, base+"/v1/encryption/passphrase", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("PUT explicit empty new_passphrase: got %d, want 400 (Bug 172, body=%q)",
			resp.StatusCode, string(raw))
	}

	assertBug172ErrorEnvelope(t, resp.Body)
	assertBug172SecretIntact(t, cli, seed)
}

// TestBug172ModifyRefusesEmptyPassphraseAlias: the W13 alias
// `{"passphrase":""}` MUST also be rejected. Bug 165's
// proofOfKnowledge() reads both `new_passphrase` and `passphrase`, so
// an explicit empty on the alias must trip the same guard. Without
// this case a `--curl` client posting `{"passphrase":""}` would still
// erase the Secret on a pre-fix build.
func TestBug172ModifyRefusesEmptyPassphraseAlias(t *testing.T) {
	srv := newSecretPathServer(t)
	cli := srv.Client

	base, stop := startServerCustom(t, srv)
	defer stop()

	const seed = "secret-before"

	seedBug172Passphrase(t, base, cli, seed)

	body, _ := json.Marshal(map[string]string{
		"old_passphrase": seed,
		"passphrase":     "",
	})

	resp := httpPut(t, base+"/v1/encryption/passphrase", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("PUT explicit empty passphrase alias: got %d, want 400 (Bug 172, body=%q)",
			resp.StatusCode, string(raw))
	}

	assertBug172ErrorEnvelope(t, resp.Body)
	assertBug172SecretIntact(t, cli, seed)
}

// TestBug172ModifyAcceptsValidNewPassphrase pins the happy-path
// regression guard: a well-formed PUT with both old + a non-empty new
// passphrase MUST still 200 and rotate the Secret. The Bug 172 fix
// adds an early `if want == ""` guard right after proofOfKnowledge();
// a regression that over-tightened (e.g. checking `want == have`
// instead, which would block legitimate rotations) would surface here.
func TestBug172ModifyAcceptsValidNewPassphrase(t *testing.T) {
	srv := newSecretPathServer(t)
	cli := srv.Client

	base, stop := startServerCustom(t, srv)
	defer stop()

	const (
		seed  = "secret-before"
		fresh = "secret-after"
	)

	seedBug172Passphrase(t, base, cli, seed)

	body, _ := json.Marshal(map[string]string{
		"old_passphrase": seed,
		"new_passphrase": fresh,
	})

	resp := httpPut(t, base+"/v1/encryption/passphrase", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("happy-path PUT: got %d, want 200 (body=%q)", resp.StatusCode, string(raw))
	}

	// Secret rotated to the new value.
	assertBug172SecretIntact(t, cli, fresh)
}

// TestBug172ModifyVerifiesOldPassphrase pins the W14 contract that a
// PUT with the wrong old passphrase MUST be rejected and MUST NOT
// mutate the Secret. Without this guard a regression that hoisted
// `if want == ""` ABOVE the auth check could turn the auth surface
// into a no-op for an attacker who guesses an empty-new probe first.
// The current implementation (auth → want=="" — wait, the fix puts
// `want==""` BEFORE auth, mirroring handlePassphraseCreate, but a
// callable wrong-old PUT must still fail closed).
func TestBug172ModifyVerifiesOldPassphrase(t *testing.T) {
	srv := newSecretPathServer(t)
	cli := srv.Client

	base, stop := startServerCustom(t, srv)
	defer stop()

	const seed = "secret-before"

	seedBug172Passphrase(t, base, cli, seed)

	body, _ := json.Marshal(map[string]string{
		"old_passphrase": "wrong-guess",
		"new_passphrase": "would-be-new",
	})

	resp := httpPut(t, base+"/v1/encryption/passphrase", body)
	defer func() { _ = resp.Body.Close() }()

	// 401 Unauthorized is what handlePassphraseModify returns today
	// (the W14 "wrong proof" surface narrowed from upstream's
	// historical 403 down to 401 so the CLI shares one mismatch
	// string with handlePassphraseEnter — see encryption.go line 309).
	if resp.StatusCode != http.StatusUnauthorized {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("wrong-old PUT: got %d, want 401 (body=%q)", resp.StatusCode, string(raw))
	}

	// Auth-failure path MUST leave the Secret untouched.
	assertBug172SecretIntact(t, cli, seed)
}
