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
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/passphrase"
	"github.com/cozystack/blockstor/pkg/store"
)

// guardCanary is the passphrase every refused write in this file tries to
// store; finding it anywhere afterwards means the guard leaked.
const guardCanary = "plaintext-passphrase-canary"

// startGuardServer boots a server over an in-memory store with one plain
// definition and a fake client, and returns both so a test can check that a
// refused write stored nothing.
func startGuardServer(t *testing.T, objs ...client.Object) (string, store.Store, client.Client, func()) {
	t.Helper()

	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name:  "rd-guard",
		Props: map[string]string{"Aux/keep": "yes"},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	cli := newFakeRESTClient(t)
	for _, obj := range objs {
		if err := cli.Create(t.Context(), obj); err != nil {
			t.Fatalf("seed %T: %v", obj, err)
		}
	}

	base, stop := startServerCustom(t, &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    cli,
		Namespace: testRESTNamespace,
	})

	return base, st, cli, stop
}

// controllerExtraProps reads the controller properties the way they sit on
// the CR, empty when the ControllerConfig does not exist.
func controllerExtraProps(t *testing.T, cli client.Client) map[string]string {
	t.Helper()

	var cfg blockstoriov1alpha1.ControllerConfig

	err := cli.Get(t.Context(), client.ObjectKey{Name: blockstoriov1alpha1.ControllerConfigName}, &cfg)
	if err != nil {
		return map[string]string{}
	}

	return cfg.Spec.ExtraProps
}

// Every door that would store the master passphrase in a property answers
// 400 and stores nothing: the controller set-property, a definition create
// carrying the early per-RD alias, a definition modify carrying the wire key,
// and a spelling that differs only in case.
func TestPlaintextPassphrasePropIsRefusedOnEveryDoor(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, method, path, body string
		stored                   func(t *testing.T, st store.Store, cli client.Client) bool
	}{
		{
			name: "controller set-property", method: http.MethodPost, path: "/v1/controller/properties",
			body: `{"override_props":{"DrbdOptions/EncryptPassphrase":"` + guardCanary + `"}}`,
			stored: func(t *testing.T, _ store.Store, cli client.Client) bool {
				t.Helper()

				return controllerExtraProps(t, cli)[passphrase.PropKeyCanonical] != ""
			},
		},
		{
			name: "rd create with the alias", method: http.MethodPost, path: "/v1/resource-definitions",
			body: `{"resource_definition":{"name":"rd-alias","props":{"DrbdOptions/Encryption/passphrase":"` +
				guardCanary + `"}}}`,
			stored: func(t *testing.T, st store.Store, _ client.Client) bool {
				t.Helper()

				_, err := st.ResourceDefinitions().Get(t.Context(), "rd-alias")

				return err == nil
			},
		},
		{
			name: "rd modify with the wire key", method: http.MethodPut, path: "/v1/resource-definitions/rd-guard",
			body: `{"override_props":{"LuksPassphrase":"` + guardCanary + `"}}`,
			stored: func(t *testing.T, st store.Store, _ client.Client) bool {
				t.Helper()

				rd, err := st.ResourceDefinitions().Get(t.Context(), "rd-guard")
				if err != nil {
					t.Fatalf("get RD: %v", err)
				}

				return rd.Props[passphrase.WirePropKey] != ""
			},
		},
		{
			name: "case variant", method: http.MethodPost, path: "/v1/controller/properties",
			body: `{"override_props":{"drbdoptions/ENCRYPTPASSPHRASE":"` + guardCanary + `"}}`,
			stored: func(t *testing.T, _ store.Store, cli client.Client) bool {
				t.Helper()

				for key := range controllerExtraProps(t, cli) {
					if passphrase.IsPlaintextPropKey(key) {
						return true
					}
				}

				return false
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			base, st, cli, stop := startGuardServer(t)
			defer stop()

			resp := sendGuardRequest(t, tc.method, base+tc.path, tc.body)
			body, _ := readAllBody(resp)
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body: %s", resp.StatusCode, body)
			}

			if !strings.Contains(string(body), "encryption create-passphrase") {
				t.Errorf("the refusal does not point at `encryption create-passphrase`: %s", body)
			}

			if strings.Contains(string(body), guardCanary) {
				t.Errorf("the refusal echoes the passphrase: %s", body)
			}

			if tc.stored(t, st, cli) {
				t.Error("the refused write stored the passphrase property")
			}
		})
	}
}

// Deleting a passphrase property stays allowed, through a delete list or
// through an override to the empty string (the set-property delete):
// removing one put there with kubectl is exactly what an operator moving to
// the Secret has to do.
func TestPlaintextPassphrasePropDeleteIsAllowed(t *testing.T) {
	t.Parallel()

	base, _, cli, stop := startGuardServer(t, &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{
				passphrase.PropKeyCanonical: guardCanary,
				passphrase.PropKeyLegacy:    guardCanary,
				"Aux/keep":                  "yes",
			},
		},
	})
	defer stop()

	for _, tc := range []struct{ name, key, body string }{
		{
			name: "delete list", key: passphrase.PropKeyCanonical,
			body: `{"delete_props":["DrbdOptions/EncryptPassphrase"]}`,
		},
		{
			name: "empty override", key: passphrase.PropKeyLegacy,
			body: `{"override_props":{"DrbdOptions/Encryption/passphrase":""}}`,
		},
	} {
		resp := sendGuardRequest(t, http.MethodPost, base+"/v1/controller/properties", tc.body)
		body, _ := readAllBody(resp)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("%s naming the passphrase key: status = %d, want 201; body: %s",
				tc.name, resp.StatusCode, body)
		}

		props := controllerExtraProps(t, cli)
		if _, still := props[tc.key]; still {
			t.Errorf("%s: the property survived its delete: %v", tc.name, props)
		}

		if props["Aux/keep"] != "yes" {
			t.Errorf("%s: the delete took a sibling with it: %v", tc.name, props)
		}
	}
}

// A body that is not JSON carries no property to refuse; it reaches the
// handler byte for byte, so the handler answers it exactly as before.
func TestPlaintextPassphraseGuardPassesANonJSONBody(t *testing.T) {
	t.Parallel()

	const raw = `not json, though it names "DrbdOptions/EncryptPassphrase"`

	var (
		reached bool
		got     []byte
	)

	guarded := withPlaintextPassphraseRefusal(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		got, _ = io.ReadAll(r.Body)

		w.WriteHeader(http.StatusTeapot)
	}))

	rec := httptest.NewRecorder()
	guarded.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost,
		"/v1/controller/properties", strings.NewReader(raw)))

	if !reached {
		t.Fatalf("a non-JSON body did not reach the handler; status %d, body: %s", rec.Code, rec.Body)
	}

	if string(got) != raw {
		t.Errorf("the handler read %q, want the body unchanged %q", got, raw)
	}

	if rec.Code != http.StatusTeapot {
		t.Errorf("status = %d, want the handler's own 418", rec.Code)
	}
}

func sendGuardRequest(t *testing.T, method, url, body string) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, url, bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}

	return resp
}
