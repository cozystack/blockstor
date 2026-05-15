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
	"github.com/cozystack/blockstor/pkg/store"
)

// TestBug116RawLayerStackLUKSRefusedWithoutPassphrase pins the
// raw-REST sibling of the Bug 95 CLI path. The python-linstor CLI
// posts `--layer-list ...` via `layer_data` (the Bug 95 fix gates
// that); a custom HTTP client that POSTs `layer_stack` directly
// must hit the same gate. Without this, any non-python LINSTOR
// client (terraform, go-linstor, raw curl) gets to land an
// encrypted-on-paper / plaintext-on-disk RD.
func TestBug116RawLayerStackLUKSRefusedWithoutPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	cli := newFakeRESTClient(t)

	base, stop := startServerCustom(t, &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    cli,
		Namespace: testRESTNamespace,
	})
	defer stop()

	// Raw POST body: layer_stack carried inside resource_definition
	// (matches the OpenAPI ResourceDefinition.layer_stack field
	// blockstor's apiv1 surface accepts). NO passphrase on the
	// controller scope.
	body := []byte(`{"resource_definition":{"name":"bug116-stack",` +
		`"layer_stack":["DRBD","LUKS","STORAGE"]}}`)

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		bodyBytes, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (raw layer_stack with LUKS + no passphrase must be refused). Body: %s",
			resp.StatusCode, bodyBytes)
	}

	bodyBytes, _ := readAllBody(resp)
	if !strings.Contains(string(bodyBytes), "DrbdOptions/EncryptPassphrase") {
		t.Errorf("response body missing prop name: %s", bodyBytes)
	}

	_, err := st.ResourceDefinitions().Get(t.Context(), "bug116-stack")
	if err == nil {
		t.Errorf("RD bug116-stack persisted despite 400 — gate is leaky on the raw layer_stack wire shape")
	}
}

// TestBug116RawTopLevelLayerListLUKSRefusedWithoutPassphrase is the
// live-cluster reproducer's wire shape: the v2 operator-poke report
// hit the apiserver with `layer_list` AT THE TOP LEVEL of the create
// envelope (golinstor-derived shape). The previous handler had no
// `LayerList` field on `ResourceDefinitionCreate`, so the JSON
// decoder silently DROPPED that key, the merged RD ended up with no
// layers, the LUKS gate saw an empty stack, and the RD landed with
// the default DRBD+STORAGE composition — operator thought they had
// encryption, they had plaintext. This test is the regression guard.
func TestBug116RawTopLevelLayerListLUKSRefusedWithoutPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	cli := newFakeRESTClient(t)

	base, stop := startServerCustom(t, &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    cli,
		Namespace: testRESTNamespace,
	})
	defer stop()

	// Byte-exact wire shape from the v2 operator-poke report (Bug 116).
	body := []byte(`{"resource_definition":{"name":"bug116-toplevel"},` +
		`"drbd_resource_definition":{},` +
		`"layer_list":["DRBD","LUKS","STORAGE"]}`)

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		bodyBytes, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (top-level layer_list with LUKS + no passphrase must be refused). Body: %s",
			resp.StatusCode, bodyBytes)
	}

	bodyBytes, _ := readAllBody(resp)
	if !strings.Contains(string(bodyBytes), "DrbdOptions/EncryptPassphrase") {
		t.Errorf("response body missing prop name: %s", bodyBytes)
	}

	_, err := st.ResourceDefinitions().Get(t.Context(), "bug116-toplevel")
	if err == nil {
		t.Errorf("RD bug116-toplevel persisted despite 400 — top-level layer_list gate is leaky")
	}
}

// TestBug116RawTopLevelLayerListPersistsOnCRD is the happy-path
// counterpart: a top-level `layer_list` MUST round-trip onto the
// stored RD's LayerStack so the dispatcher / satellite read sees
// the operator-requested composition. Pre-Bug-116 the decoder
// dropped the key entirely and the RD landed with an empty stack
// (the default DRBD+STORAGE projection kicked in downstream).
func TestBug116RawTopLevelLayerListPersistsOnCRD(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	cli := newFakeRESTClient(t)
	ctx := t.Context()

	if err := cli.Create(ctx, &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{
				"DrbdOptions/EncryptPassphrase": "bug116pass",
			},
		},
	}); err != nil {
		t.Fatalf("seed ControllerConfig: %v", err)
	}

	base, stop := startServerCustom(t, &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    cli,
		Namespace: testRESTNamespace,
	})
	defer stop()

	body := []byte(`{"resource_definition":{"name":"bug116-toplevel-ok"},` +
		`"layer_list":["DRBD","LUKS","STORAGE"]}`)

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201. Body: %s",
			resp.StatusCode, bodyBytes)
	}

	rd, err := st.ResourceDefinitions().Get(ctx, "bug116-toplevel-ok")
	if err != nil {
		t.Fatalf("Get RD: %v", err)
	}

	wantStack := []string{"DRBD", "LUKS", "STORAGE"}
	if len(rd.LayerStack) != len(wantStack) {
		t.Fatalf("RD LayerStack: got %v, want %v (top-level layer_list must round-trip)",
			rd.LayerStack, wantStack)
	}

	for i, want := range wantStack {
		if rd.LayerStack[i] != want {
			t.Errorf("RD LayerStack[%d]: got %q, want %q",
				i, rd.LayerStack[i], want)
		}
	}
}

// TestBug116RawLayerDataLUKSRefusedWithoutPassphrase is the
// regression guard pinned to the Bug 95 fix wire shape. The
// python-linstor CLI posts `--layer-list` via `layer_data`; the
// existing Bug 95 fix (commit 98fc525d6) gates that path. This
// test keeps the gate honest after the Bug 116 reshuffle.
func TestBug116RawLayerDataLUKSRefusedWithoutPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	cli := newFakeRESTClient(t)

	base, stop := startServerCustom(t, &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    cli,
		Namespace: testRESTNamespace,
	})
	defer stop()

	body := []byte(`{"resource_definition":{"name":"bug116-data",` +
		`"layer_data":[{"type":"DRBD"},{"type":"LUKS"},{"type":"STORAGE"}]}}`)

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		bodyBytes, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (Bug 95 regression guard). Body: %s",
			resp.StatusCode, bodyBytes)
	}

	_, err := st.ResourceDefinitions().Get(t.Context(), "bug116-data")
	if err == nil {
		t.Errorf("RD bug116-data persisted despite 400 — Bug 95 gate is leaky")
	}
}

// TestBug116RawLayerStackLUKSAcceptedWithPassphrase is the happy-
// path counterpart: with `DrbdOptions/EncryptPassphrase` set on
// the controller scope, the raw `layer_stack` shape must land 201
// + LUKS in the persisted CRD spec.
func TestBug116RawLayerStackLUKSAcceptedWithPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	cli := newFakeRESTClient(t)
	ctx := t.Context()

	if err := cli.Create(ctx, &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{
				"DrbdOptions/EncryptPassphrase": "bug116pass",
			},
		},
	}); err != nil {
		t.Fatalf("seed ControllerConfig: %v", err)
	}

	base, stop := startServerCustom(t, &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    cli,
		Namespace: testRESTNamespace,
	})
	defer stop()

	body := []byte(`{"resource_definition":{"name":"bug116-stack-ok",` +
		`"layer_stack":["DRBD","LUKS","STORAGE"]}}`)

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201. Body: %s",
			resp.StatusCode, bodyBytes)
	}

	rd, err := st.ResourceDefinitions().Get(ctx, "bug116-stack-ok")
	if err != nil {
		t.Fatalf("Get RD: %v", err)
	}

	wantStack := []string{"DRBD", "LUKS", "STORAGE"}
	if len(rd.LayerStack) != len(wantStack) {
		t.Fatalf("RD LayerStack: got %v, want %v", rd.LayerStack, wantStack)
	}

	for i, want := range wantStack {
		if rd.LayerStack[i] != want {
			t.Errorf("RD LayerStack[%d]: got %q, want %q",
				i, rd.LayerStack[i], want)
		}
	}
}

// TestBug116BothFieldsConsistent: posting an RD-create with BOTH
// `layer_stack` AND `layer_data` carrying DIFFERENT compositions
// is ambiguous — the operator's intent isn't expressible by
// silently picking one. The gate returns 400 instead of guessing.
//
// Consistent (same composition) bodies remain accepted: see
// TestBug116BothFieldsAgreeing for that path.
func TestBug116BothFieldsConsistent(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	cli := newFakeRESTClient(t)
	ctx := t.Context()

	if err := cli.Create(ctx, &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{
				"DrbdOptions/EncryptPassphrase": "bug116pass",
			},
		},
	}); err != nil {
		t.Fatalf("seed ControllerConfig: %v", err)
	}

	base, stop := startServerCustom(t, &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    cli,
		Namespace: testRESTNamespace,
	})
	defer stop()

	// `layer_stack` says DRBD,LUKS,STORAGE; `layer_data` says
	// DRBD,STORAGE — the operator's intent is unclear, and silently
	// picking one would hide a real misconfiguration.
	body := []byte(`{"resource_definition":{"name":"bug116-ambig",` +
		`"layer_stack":["DRBD","LUKS","STORAGE"],` +
		`"layer_data":[{"type":"DRBD"},{"type":"STORAGE"}]}}`)

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		bodyBytes, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (ambiguous layer_stack vs layer_data). Body: %s",
			resp.StatusCode, bodyBytes)
	}

	bodyBytes, _ := readAllBody(resp)
	if !strings.Contains(strings.ToLower(string(bodyBytes)), "ambiguous") &&
		!strings.Contains(strings.ToLower(string(bodyBytes)), "conflict") {
		t.Errorf("response body should explain the ambiguity: %s", bodyBytes)
	}

	_, err := st.ResourceDefinitions().Get(ctx, "bug116-ambig")
	if err == nil {
		t.Errorf("RD bug116-ambig persisted despite 400 — gate is leaky on ambiguous bodies")
	}
}

// TestBug116BothFieldsAgreeing: when `layer_stack` and
// `layer_data` carry the SAME composition, the request is
// accepted (the redundancy is not a bug — golinstor clients that
// populate both fields belt-and-braces shouldn't be rejected
// just because they're verbose).
func TestBug116BothFieldsAgreeing(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	cli := newFakeRESTClient(t)
	ctx := t.Context()

	if err := cli.Create(ctx, &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{
				"DrbdOptions/EncryptPassphrase": "bug116pass",
			},
		},
	}); err != nil {
		t.Fatalf("seed ControllerConfig: %v", err)
	}

	base, stop := startServerCustom(t, &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    cli,
		Namespace: testRESTNamespace,
	})
	defer stop()

	body := []byte(`{"resource_definition":{"name":"bug116-agree",` +
		`"layer_stack":["DRBD","LUKS","STORAGE"],` +
		`"layer_data":[{"type":"DRBD"},{"type":"LUKS"},{"type":"STORAGE"}]}}`)

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201 (agreeing fields must be accepted). Body: %s",
			resp.StatusCode, bodyBytes)
	}

	rd, err := st.ResourceDefinitions().Get(ctx, "bug116-agree")
	if err != nil {
		t.Fatalf("Get RD: %v", err)
	}

	if len(rd.LayerStack) != 3 || rd.LayerStack[1] != "LUKS" {
		t.Errorf("RD LayerStack: got %v, want [DRBD LUKS STORAGE]", rd.LayerStack)
	}
}
