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

// TestBug349RDListLayersColumnPopulated pins Bug 349's contract: the
// REST GET /v1/resource-definitions response MUST carry a non-empty
// `layer_data[]` array for every RD whose `Spec.LayerStack` is set —
// the python-linstor CLI's `rd l` Layers column renders by joining
// `entry.type` across `layer_data[]`, and an empty array collapses
// the column to blank.
//
// Stand reproduction (2026-05-19): operator's `linstor rd l` showed
// every RD's Layers column empty even though `Spec.LayerStack` was
// `["DRBD","STORAGE"]` on the CRD. The k8s store's `crdToWireRD`
// projection persists only Spec.LayerStack as the canonical source
// of truth; without re-synthesising layer_data on the read path,
// the wire shape arrived with the column blank.
func TestBug349RDListLayersColumnPopulated(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	rd := apiv1.ResourceDefinition{
		Name:              "bug349-rd",
		ResourceGroupName: "DfltRscGrp",
		LayerStack:        []string{apiv1.LayerKindDRBD, apiv1.LayerKindStorage},
	}
	if err := st.ResourceDefinitions().Create(ctx, &rd); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rds []apiv1.ResourceDefinition
	if err := json.NewDecoder(resp.Body).Decode(&rds); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(rds) != 1 {
		t.Fatalf("rds: got %d, want 1 (%v)", len(rds), rds)
	}

	got := rds[0]
	if got.Name != "bug349-rd" {
		t.Errorf("Name: got %q, want %q", got.Name, "bug349-rd")
	}

	// Core invariant: layer_data is non-empty so the CLI Layers
	// column renders something.
	if len(got.LayerData) == 0 {
		t.Fatalf("layer_data: empty — Bug 349 regression "+
			"(LayerStack=%v should synthesise layer_data on the wire)",
			got.LayerStack)
	}

	// Every entry must carry a non-empty `type` discriminator.
	// Python-linstor CLI's row builder dereferences `entry.type`
	// unconditionally; a missing field collapses the column even
	// when len(layer_data) > 0.
	types := make([]string, 0, len(got.LayerData))
	for i, layer := range got.LayerData {
		if layer.Type == "" {
			t.Errorf("layer_data[%d].type empty: %+v", i, layer)
		}

		types = append(types, layer.Type)
	}

	// Both DRBD and STORAGE entries must appear (in stack order)
	// so the CLI emits `DRBD,STORAGE` for the row. Order matches
	// upstream LINSTOR — top-of-stack first.
	if len(types) != 2 || types[0] != apiv1.LayerKindDRBD || types[1] != apiv1.LayerKindStorage {
		t.Errorf("layer_data types: got %v, want [DRBD STORAGE]", types)
	}
}

// TestBug349RDGetLayersColumnPopulated mirrors the list path on the
// per-RD GET handler. `linstor rd l --resource-definitions <name>`
// fans out into a name-filtered list call (and `linstor rd s <name>`
// hits the per-RD endpoint directly); both must surface the same
// `layer_data[]` shape so the operator's CLI never sees a column
// disagreement between bulk and singleton reads.
func TestBug349RDGetLayersColumnPopulated(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	rd := apiv1.ResourceDefinition{
		Name:              "bug349-rd-get",
		ResourceGroupName: "DfltRscGrp",
		LayerStack:        []string{apiv1.LayerKindDRBD, apiv1.LayerKindStorage},
	}
	if err := st.ResourceDefinitions().Create(ctx, &rd); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/bug349-rd-get")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got apiv1.ResourceDefinition
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got.LayerData) == 0 {
		t.Fatalf("layer_data: empty on GET /{rd} — must match the list-path stamp")
	}

	sawDRBD, sawStorage := false, false
	for _, layer := range got.LayerData {
		switch layer.Type {
		case apiv1.LayerKindDRBD:
			sawDRBD = true
		case apiv1.LayerKindStorage:
			sawStorage = true
		}
	}

	if !sawDRBD || !sawStorage {
		t.Errorf("layer_data missing entries: drbd=%t storage=%t (got %+v)",
			sawDRBD, sawStorage, got.LayerData)
	}
}

// TestBug349RDListLayersFallsBackToDefaultWhenStackEmpty pins the
// pre-existing-RD branch: RDs created before the Bug 222 / Bug 54
// LayerStack-inheritance fixes carry an empty `LayerStack` on the
// CRD. Without a default, `rd l` would still render an empty Layers
// column for those rows. Upstream LINSTOR treats an empty stack as
// the canonical `DRBD,STORAGE` default — mirror that on the wire so
// every operator-visible RD has a populated column.
func TestBug349RDListLayersFallsBackToDefaultWhenStackEmpty(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	// Empty LayerStack — pre-Bug-222 wire shape on a half-migrated
	// cluster.
	rd := apiv1.ResourceDefinition{
		Name:              "bug349-rd-empty-stack",
		ResourceGroupName: "DfltRscGrp",
	}
	if err := st.ResourceDefinitions().Create(ctx, &rd); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rds []apiv1.ResourceDefinition
	if err := json.NewDecoder(resp.Body).Decode(&rds); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(rds) != 1 {
		t.Fatalf("rds: got %d, want 1", len(rds))
	}

	if len(rds[0].LayerData) == 0 {
		t.Fatalf("layer_data empty for RD with no LayerStack — must fall back to default DRBD,STORAGE")
	}

	sawDRBD, sawStorage := false, false
	for _, layer := range rds[0].LayerData {
		switch layer.Type {
		case apiv1.LayerKindDRBD:
			sawDRBD = true
		case apiv1.LayerKindStorage:
			sawStorage = true
		}
	}

	if !sawDRBD || !sawStorage {
		t.Errorf("default layer_data missing entries: drbd=%t storage=%t",
			sawDRBD, sawStorage)
	}
}
