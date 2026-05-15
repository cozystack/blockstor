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
	"io"
	"net/http"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestBug95LUKSLayerNotSilentlyDropped is the read-side guard for the
// Bug 95 regression discovered on the dev-kvaps stand on 2026-05-15:
//
//	linstor rd c luktest --layer-list DRBD,LUKS,STORAGE
//	→ accepted, but /v1/view/resources surfaced layer_data_list as
//	  [DRBD, STORAGE] (LUKS silently dropped). Operators thought they
//	  had encryption — they had plaintext-on-DRBD.
//
// Root cause: the k8s store's `crdToWireResource` projection
// hardcoded `DefaultLayerStack()` for both `layer_object` and every
// volume's `layer_data_list` because the Resource CRD doesn't carry
// the parent RD's LayerStack. The REST `/v1/view/resources`
// aggregator now re-stamps both surfaces from the RD spec it already
// fetches for the effective-props walk.
//
// This test seeds an RD with `--layer-list DRBD,LUKS,STORAGE` + a
// matching diskful Resource, GETs `/v1/view/resources`, and asserts:
//   - top `layer_object.type == DRBD`
//   - middle child is LUKS (the silent-drop site)
//   - bottom child is STORAGE
//   - every volume's `layer_data_list` carries all three entries
func TestBug95LUKSLayerNotSilentlyDropped(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	// Seed RD with the encrypted-on-DRBD stack.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:       "luktest",
		LayerStack: []string{"DRBD", "LUKS", "STORAGE"},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// Volume definition so the resource has at least one volume to
	// re-stamp `layer_data_list` on.
	if err := st.VolumeDefinitions().Create(ctx, "luktest", &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      32 * 1024, // 32 MiB
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	// Seed a placed Resource. The in-memory store stores
	// `apiv1.Resource` as-is — the bug is in the k8s store's
	// CRD-projection, but the re-stamp at the REST layer applies
	// uniformly to both stores so the behaviour is testable here.
	// To exercise the re-stamp we deliberately stamp a wrong (default)
	// LayerObject + Volume.LayerDataList on the seed, then assert the
	// REST GET overrides it from the RD's LayerStack.
	seed := apiv1.Resource{
		Name:     "luktest",
		NodeName: "worker-1",
		LayerObject: &apiv1.ResourceLayer{
			Type: apiv1.LayerKindDRBD,
			Children: []apiv1.ResourceLayer{{
				Type: apiv1.LayerKindStorage,
			}},
		},
		Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			LayerDataList: []apiv1.VolumeLayerData{
				{Type: apiv1.LayerKindDRBD},
				{Type: apiv1.LayerKindStorage},
			},
		}},
	}
	if err := st.Resources().Create(ctx, &seed); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.ResourceWithVolumes
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("len: got %d, want 1", len(got))
	}

	res := got[0]

	// Walk the single-branch chain: DRBD → LUKS → STORAGE.
	if res.LayerObject == nil || res.LayerObject.Type != apiv1.LayerKindDRBD {
		t.Fatalf("top layer: got %+v, want DRBD", res.LayerObject)
	}

	if len(res.LayerObject.Children) != 1 ||
		res.LayerObject.Children[0].Type != apiv1.LayerKindLUKS {
		t.Fatalf("LUKS layer missing — Bug 95 regression. Chain: %s",
			describeLayerChain(res.LayerObject))
	}

	luks := &res.LayerObject.Children[0]
	if len(luks.Children) != 1 || luks.Children[0].Type != apiv1.LayerKindStorage {
		t.Fatalf("STORAGE layer below LUKS missing. Chain: %s",
			describeLayerChain(res.LayerObject))
	}

	// Per-volume layer_data_list: must carry all three layers, in
	// order. The Python CLI's `_walk(layer_data, type==LUKS)`
	// predicate reads this for State-column rendering / `--faulty`
	// gating — a missing LUKS entry here is what surfaced the
	// "operator thinks encrypted, actually plaintext" gap.
	if len(res.Volumes) != 1 {
		t.Fatalf("volumes: got %d, want 1", len(res.Volumes))
	}

	wantLayers := []string{
		apiv1.LayerKindDRBD,
		apiv1.LayerKindLUKS,
		apiv1.LayerKindStorage,
	}

	gotLayers := make([]string, 0, len(res.Volumes[0].LayerDataList))
	for _, l := range res.Volumes[0].LayerDataList {
		gotLayers = append(gotLayers, l.Type)
	}

	if len(gotLayers) != len(wantLayers) {
		t.Fatalf("layer_data_list len: got %v, want %v (Bug 95)",
			gotLayers, wantLayers)
	}

	for i, want := range wantLayers {
		if gotLayers[i] != want {
			t.Errorf("layer_data_list[%d]: got %q, want %q", i, gotLayers[i], want)
		}
	}
}

// describeLayerChain renders a single-branch ResourceLayer chain as
// "TYPE → TYPE → …" so a missing layer surfaces in the failure
// message without forcing the reader to JSON-pprint the whole tree.
func describeLayerChain(layer *apiv1.ResourceLayer) string {
	parts := []string{}

	for cursor := layer; cursor != nil; {
		parts = append(parts, cursor.Type)

		if len(cursor.Children) == 0 {
			break
		}

		cursor = &cursor.Children[0]
	}

	return strings.Join(parts, " → ")
}

// TestBug95LUKSCreateRefusedWithoutPassphrase is the write-side
// guard: posting an RD-create body with LUKS in the layer stack
// MUST be rejected with 400 when the controller-scope props don't
// carry `DrbdOptions/EncryptPassphrase`. Without this gate the RD
// was accepted and the encryption silently dropped (Bug 95).
//
// The 400 envelope must name the missing prop so the operator
// knows exactly which `controller set-property` invocation to make
// before retrying the RD-create.
func TestBug95LUKSCreateRefusedWithoutPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	cli := newFakeRESTClient(t)

	// No ControllerConfig seed — `DrbdOptions/EncryptPassphrase` is
	// absent. (The fake client treats a missing ControllerConfig
	// the same way as `controllerScopeProps` does: empty map.)
	base, stop := startServerCustom(t, &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    cli,
		Namespace: testRESTNamespace,
	})
	defer stop()

	body, err := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{
			Name:       "luktest",
			LayerStack: []string{"DRBD", "LUKS", "STORAGE"},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400 (LUKS without passphrase must be refused)",
			resp.StatusCode)
	}

	bodyBytes, _ := readAllBody(resp)
	if !strings.Contains(string(bodyBytes), "DrbdOptions/EncryptPassphrase") {
		t.Errorf("response body missing prop name: %s", bodyBytes)
	}

	// Confirm the RD was NOT persisted — partial commit on a 400 is
	// the silent-failure mode this gate is preventing.
	_, err = st.ResourceDefinitions().Get(t.Context(), "luktest")
	if err == nil {
		t.Errorf("RD luktest persisted despite 400 — gate is leaky")
	}
}

// TestBug95LUKSCreateAcceptedWithPassphrase is the happy-path
// counterpart: once `DrbdOptions/EncryptPassphrase` is set on the
// controller scope, `rd c --layer-list DRBD,LUKS,STORAGE` is
// accepted (201) and the resulting RD persists the requested
// LayerStack so the dispatcher's read of `rd.Spec.LayerStack`
// (pkg/dispatcher/dispatcher.go) carries LUKS through to the
// satellite's needsLUKS gate.
func TestBug95LUKSCreateAcceptedWithPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	cli := newFakeRESTClient(t)
	ctx := t.Context()

	if err := cli.Create(ctx, &blockstoriov1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: blockstoriov1alpha1.ControllerConfigName},
		Spec: blockstoriov1alpha1.ControllerConfigSpec{
			ExtraProps: map[string]string{
				"DrbdOptions/EncryptPassphrase": "blockstorpass",
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

	body, err := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{
			Name:       "luktest",
			LayerStack: []string{"DRBD", "LUKS", "STORAGE"},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201. Body: %s",
			resp.StatusCode, bodyBytes)
	}

	rd, err := st.ResourceDefinitions().Get(ctx, "luktest")
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

// readAllBody slurps an HTTP response body so the calling assertion
// can inline it into a failure message. Errors are swallowed; the
// only caller is a t.Errorf / t.Fatalf shortcut where producing a
// partial body is strictly better than masking the real assertion.
func readAllBody(resp *http.Response) ([]byte, error) {
	out, err := io.ReadAll(resp.Body)

	return out, err //nolint:wrapcheck // test helper, error is informational
}
