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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/passphrase"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 023: `linstor encryption create-passphrase` stores the master
// passphrase in a Secret (pkg/rest/encryption.go), but the LUKS
// RD-create gate only consulted the `DrbdOptions/EncryptPassphrase`
// controller prop — so the upstream-standard flow
//
//	linstor encryption create-passphrase --passphrase ...
//	linstor rd create <rd> --layer-list drbd,luks,storage
//
// failed with 400, and the error hint told operators to put a
// PLAINTEXT passphrase into a controller prop. These tests pin the
// fixed gate: the Secret is the only thing that unlocks LUKS RD
// creation, a passphrase property never does, and the refusal sends
// the operator to `encryption create-passphrase` alone.

// startLUKSGateServer boots a server over an in-memory store + fake
// controller-runtime client seeded with objs.
func startLUKSGateServer(t *testing.T, objs ...client.Object) (string, store.Store, func()) {
	t.Helper()

	st := store.NewInMemory()
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

	return base, st, stop
}

// postLUKSRD posts an RD-create with a LUKS-bearing layer stack.
func postLUKSRD(t *testing.T, base, name string) *http.Response {
	t.Helper()

	body, err := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{
			Name:       name,
			LayerStack: []string{"DRBD", "LUKS", "STORAGE"},
		},
	})
	if err != nil {
		t.Fatalf("marshal RD create: %v", err)
	}

	return httpPost(t, base+"/v1/resource-definitions", body)
}

// TestBug023CreatePassphraseThenLUKSRDCreate is the operator-flow
// pin: POST /v1/encryption/passphrase (what `linstor encryption
// create-passphrase` sends) followed by a LUKS RD create must answer
// 201. Pre-fix the gate ignored the Secret and 400'd.
func TestBug023CreatePassphraseThenLUKSRDCreate(t *testing.T) {
	t.Parallel()

	base, st, stop := startLUKSGateServer(t)
	defer stop()

	passBody, _ := json.Marshal(map[string]string{"new_passphrase": "supersecret-passphrase-1"})

	passResp := httpPost(t, base+"/v1/encryption/passphrase", passBody)
	_ = passResp.Body.Close()

	if passResp.StatusCode != http.StatusCreated {
		t.Fatalf("encryption create-passphrase: status got %d, want 201", passResp.StatusCode)
	}

	resp := postLUKSRD(t, base, "wf-luks")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := readAllBody(resp)
		t.Fatalf("LUKS RD create after create-passphrase: status got %d, want 201. Body: %s",
			resp.StatusCode, bodyBytes)
	}

	rd, err := st.ResourceDefinitions().Get(t.Context(), "wf-luks")
	if err != nil {
		t.Fatalf("RD not persisted: %v", err)
	}

	if !apiv1.LayerInStack(rd.LayerStack, apiv1.LayerKindLUKS) {
		t.Errorf("LayerStack lost LUKS: %v", rd.LayerStack)
	}
}

// TestBug023SecretSeededDirectlyUnlocksGate covers the GitOps shape:
// the passphrase Secret applied as YAML (no REST call in this
// process) must satisfy the gate too — presence in the Secret is the
// contract, not the in-process create-passphrase side effects.
func TestBug023SecretSeededDirectlyUnlocksGate(t *testing.T) {
	t.Parallel()

	base, _, stop := startLUKSGateServer(t, testPassphraseSecret("gitops-passphrase"))
	defer stop()

	resp := postLUKSRD(t, base, "wf-luks-gitops")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := readAllBody(resp)
		t.Fatalf("LUKS RD create with seeded Secret: status got %d, want 201. Body: %s",
			resp.StatusCode, bodyBytes)
	}
}

// TestBug023ControllerPropDoesNotUnlockGate: a passphrase that sits in
// a controller property (plaintext on ControllerConfig.ExtraProps, put
// there with kubectl since REST refuses to store it) is never read, so
// it does not satisfy the gate; the refusal names the Secret flow, not
// the property.
func TestBug023ControllerPropDoesNotUnlockGate(t *testing.T) {
	t.Parallel()

	cfg := &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{
				passphrase.PropKeyCanonical: "legacy-plaintext-pass",
			},
		},
	}

	base, _, stop := startLUKSGateServer(t, cfg)
	defer stop()

	resp := postLUKSRD(t, base, "wf-luks-legacy")
	defer func() { _ = resp.Body.Close() }()

	bodyBytes, _ := readAllBody(resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("LUKS RD create with only a controller prop: status got %d, want 400. Body: %s",
			resp.StatusCode, bodyBytes)
	}

	assertLUKSRefusalHint(t, bodyBytes)
}

// TestBug023RefusalHintsAtCreatePassphrase: with NO passphrase
// anywhere the gate still refuses (Bug 95 contract) — but the
// remediation hint must lead with the upstream-standard
// `encryption create-passphrase`, not instruct the operator to stamp
// a plaintext prop as the primary path.
func TestBug023RefusalHintsAtCreatePassphrase(t *testing.T) {
	t.Parallel()

	base, st, stop := startLUKSGateServer(t)
	defer stop()

	resp := postLUKSRD(t, base, "wf-luks-refused")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("LUKS RD create without passphrase: status got %d, want 400", resp.StatusCode)
	}

	bodyBytes, _ := readAllBody(resp)
	assertLUKSRefusalHint(t, bodyBytes)

	// The RD must not be persisted on the refusal path.
	if _, err := st.ResourceDefinitions().Get(t.Context(), "wf-luks-refused"); err == nil {
		t.Errorf("RD persisted despite 400 — gate is leaky")
	}
}
