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
	"net/http"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// bug115LeakedPassphrase is the secret seeded onto the controller
// scope across every Bug-115 test. Spelled distinctively so a leak
// surfaces clearly in a failing diff — operators would never use
// this in production, so a substring match against the verbatim
// value reliably catches any unredacted surface.
const bug115LeakedPassphrase = "hunter2-leak-canary"

// seedControllerSensitiveProps installs the controller-scope
// passphrase + adjacent deny-listed keys onto the fake REST client.
// All Bug-115 tests share this seed so the assertions converge on
// the same denominators.
func seedControllerSensitiveProps(t *testing.T, srv *Server) {
	t.Helper()

	cfg := &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{
				"DrbdOptions/EncryptPassphrase": bug115LeakedPassphrase,
				"DrbdOptions/Net/cram-hmac-alg": bug115LeakedPassphrase,
				"DrbdOptions/Net/shared-secret": bug115LeakedPassphrase,
				"Aux/secret-token":              bug115LeakedPassphrase,
				"Aux/Password":                  bug115LeakedPassphrase,
				"Aux/encrypt-key":               bug115LeakedPassphrase,
				"DrbdOptions/PingTimeout":       "200",  // happy-path: NOT sensitive
				"Aux/team":                      "blue", // happy-path: NOT sensitive
			},
		},
	}

	if err := srv.Client.Create(t.Context(), cfg); err != nil {
		t.Fatalf("seed ControllerConfig: %v", err)
	}
}

// assertBodyNoLeak asserts the response body does NOT contain the
// canary passphrase verbatim. Substring match is the operationally
// meaningful predicate — if any string in the JSON payload carries
// the secret, an attacker can grep it out regardless of which key
// or which scope-level surfaces it.
func assertBodyNoLeak(t *testing.T, label string, body []byte) {
	t.Helper()

	if strings.Contains(string(body), bug115LeakedPassphrase) {
		t.Errorf("Bug 115: %s leaked passphrase %q in body: %s",
			label, bug115LeakedPassphrase, body)
	}
}

// htmlEscapedRedactedMarker is the wire form of the redaction
// marker after Go's stdlib `json.Encoder` HTML-escapes `<` and `>`
// (the package-wide default). Spelled as a Go-string literal so
// the substring match is byte-exact against the response body.
const htmlEscapedRedactedMarker = "\\u003credacted\\u003e"

// assertBodyHasRedactionMarker confirms the redaction marker is
// present (preserving the key's existence on the wire). Bug 115's
// spec preserves key presence so the operator can still see that
// the prop IS set — they just can't read the value.
//
// The marker is `<redacted>` server-side; on the wire Go's
// json.Encoder HTML-escapes it to `<redacted>`. Either
// form is acceptable — both decode to the same UTF-8 string for
// the operator's CLI.
func assertBodyHasRedactionMarker(t *testing.T, label string, body []byte) {
	t.Helper()

	raw := string(body)
	if !strings.Contains(raw, redactedPropValue) &&
		!strings.Contains(raw, htmlEscapedRedactedMarker) {
		t.Errorf("Bug 115: %s did not carry redaction marker %q (or escaped %q) in body: %s",
			label, redactedPropValue, htmlEscapedRedactedMarker, body)
	}
}

// TestBug115ControllerPropsListRedactsPassphrase covers the
// `linstor c lp` surface: `GET /v1/controller/properties` returns
// the controller-scope props map verbatim. Before this fix the
// passphrase rendered as plaintext in `c lp`'s table.
func TestBug115ControllerPropsListRedactsPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	cli := newFakeRESTClient(t)
	srv := &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    cli,
		Namespace: testRESTNamespace,
	}

	seedControllerSensitiveProps(t, srv)

	base, stop := startServerCustom(t, srv)
	defer stop()

	resp := httpGet(t, base+"/v1/controller/properties")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBodyNoLeak(t, "GET /v1/controller/properties", body)
	assertBodyHasRedactionMarker(t, "GET /v1/controller/properties", body)
}

// TestBug115RDListRedactsInheritedPassphrase covers `linstor rd lp`
// via the list path. `rd lp <rd>` calls `GET /v1/resource-
// definitions` with a name filter; the Bug-105 inheritance walk
// inlines controller-scope keys into the RD's `props` map.
// Without redaction the passphrase is surfaced verbatim under
// EVERY RD that doesn't locally shadow the key.
func TestBug115RDListRedactsInheritedPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	cli := newFakeRESTClient(t)
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "bug115-rd",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	srv := &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    cli,
		Namespace: testRESTNamespace,
	}
	seedControllerSensitiveProps(t, srv)

	base, stop := startServerCustom(t, srv)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBodyNoLeak(t, "GET /v1/resource-definitions", body)
}

// TestBug115RDGetRedactsInheritedPassphrase covers the single-RD
// fetch: `GET /v1/resource-definitions/{rd}`. Both `props` (the
// Bug-105 inlined inherited keys) and `effective_props` (the
// scope-annotated bag) must scrub sensitive values.
func TestBug115RDGetRedactsInheritedPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	cli := newFakeRESTClient(t)
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "bug115-rd",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	srv := &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    cli,
		Namespace: testRESTNamespace,
	}
	seedControllerSensitiveProps(t, srv)

	base, stop := startServerCustom(t, srv)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/bug115-rd")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBodyNoLeak(t, "GET /v1/resource-definitions/{rd}", body)

	// Inheritance still functional: non-sensitive controller keys
	// must continue to surface so Bug 105's happy-path is intact.
	if !strings.Contains(string(body), "DrbdOptions/PingTimeout") {
		t.Errorf("Bug 105 regression: PingTimeout inheritance missing from RD-get body: %s", body)
	}
}

// TestBug115RedactsAllDenyListedKeys is the unit-level check that
// every documented-as-sensitive key in the deny-list is caught.
// Bypasses the HTTP boundary so it can iterate the matrix without
// re-spinning the server per key.
func TestBug115RedactsAllDenyListedKeys(t *testing.T) {
	t.Parallel()

	cases := []struct {
		key       string
		sensitive bool
	}{
		// Sensitive: deny-list substring hits, any casing.
		{"DrbdOptions/EncryptPassphrase", true},
		{"drbdoptions/encryptpassphrase", true},
		{"DrbdOptions/Net/cram-hmac-alg", true},
		{"DrbdOptions/Net/shared-secret", true},
		{"Aux/secret-token", true},
		{"Aux/SECRET-anything", true},
		{"Aux/Password", true},
		{"Aux/password123", true},
		{"Aux/encrypt-key", true},
		{"Aux/EncryptKey", true},
		// Non-sensitive: ordinary props the deny list MUST NOT
		// touch (the inheritance happy-path).
		{"DrbdOptions/PingTimeout", false},
		{"DrbdOptions/auto-promote-timeout", false},
		{"Aux/team", false},
		{"Aux/owner", false},
		{"", false},
	}

	for _, c := range cases {
		t.Run(c.key, func(t *testing.T) {
			t.Parallel()
			got := isSensitivePropKey(c.key)
			if got != c.sensitive {
				t.Errorf("isSensitivePropKey(%q): got %v, want %v",
					c.key, got, c.sensitive)
			}
		})
	}
}

// TestBug115RedactSensitivePropsRewritesValues confirms the
// in-place mutation: deny-listed keys lose their values, non-
// sensitive keys keep theirs.
func TestBug115RedactSensitivePropsRewritesValues(t *testing.T) {
	t.Parallel()

	in := map[string]string{
		"DrbdOptions/EncryptPassphrase": bug115LeakedPassphrase,
		"Aux/Password":                  bug115LeakedPassphrase,
		"DrbdOptions/PingTimeout":       "200",
	}

	redactSensitiveProps(in)

	if in["DrbdOptions/EncryptPassphrase"] != redactedPropValue {
		t.Errorf("EncryptPassphrase: got %q, want %q",
			in["DrbdOptions/EncryptPassphrase"], redactedPropValue)
	}

	if in["Aux/Password"] != redactedPropValue {
		t.Errorf("Aux/Password: got %q, want %q",
			in["Aux/Password"], redactedPropValue)
	}

	if in["DrbdOptions/PingTimeout"] != "200" {
		t.Errorf("PingTimeout: got %q, want unchanged %q",
			in["DrbdOptions/PingTimeout"], "200")
	}
}

// TestBug115RedactSensitiveEffectivePropsRewritesEntries is the
// scope-annotated sibling check: deny-listed keys lose their
// values but keep their scope tag.
func TestBug115RedactSensitiveEffectivePropsRewritesEntries(t *testing.T) {
	t.Parallel()

	in := apiv1.EffectiveProperties{
		"DrbdOptions/EncryptPassphrase": {
			Value: bug115LeakedPassphrase,
			Scope: apiv1.EffectivePropScopeController,
		},
		"DrbdOptions/PingTimeout": {
			Value: "200",
			Scope: apiv1.EffectivePropScopeController,
		},
	}

	redactSensitiveEffectiveProps(in)

	pass := in["DrbdOptions/EncryptPassphrase"]
	if pass.Value != redactedPropValue {
		t.Errorf("EncryptPassphrase value: got %q, want %q",
			pass.Value, redactedPropValue)
	}

	if pass.Scope != apiv1.EffectivePropScopeController {
		t.Errorf("EncryptPassphrase scope: got %q, want %q",
			pass.Scope, apiv1.EffectivePropScopeController)
	}

	pt := in["DrbdOptions/PingTimeout"]
	if pt.Value != "200" {
		t.Errorf("PingTimeout value: got %q, want unchanged %q",
			pt.Value, "200")
	}
}
