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
	"io"
	"net/http"
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// A stack is validated case-insensitively but some readers compare the
// stored value exactly (the satellite controller's DRBD check among them),
// so every write path stores the canonical spelling.
func wantCanonicalStack() []string { return []string{apiv1.LayerKindDRBD, apiv1.LayerKindStorage} }

func TestRDCreateStoresLayerStackInCanonicalCase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	base, stop := startServerWithStore(t, st)
	defer stop()

	postOK(t, base+"/v1/resource-definitions", map[string]any{
		"resource_definition": map[string]any{"name": "rd-case"},
		"layer_list":          []string{"drbd", "Storage"},
	}, http.StatusCreated)

	rd, err := st.ResourceDefinitions().Get(t.Context(), "rd-case")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if !slices.Equal(rd.LayerStack, wantCanonicalStack()) {
		t.Fatalf("stored stack %v, want %v", rd.LayerStack, wantCanonicalStack())
	}
}

func TestRGCreateAndModifyStoreLayerStackInCanonicalCase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	base, stop := startServerWithStore(t, st)
	defer stop()

	postOK(t, base+"/v1/resource-groups", map[string]any{
		"name":          "rg-case",
		"select_filter": map[string]any{"layer_stack": []string{"drbd", "storage"}},
	}, http.StatusCreated)

	rg, err := st.ResourceGroups().Get(t.Context(), "rg-case")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if !slices.Equal(rg.SelectFilter.LayerStack, wantCanonicalStack()) {
		t.Fatalf("rg create stored %v, want %v", rg.SelectFilter.LayerStack, wantCanonicalStack())
	}

	raw, err := json.Marshal(map[string]any{
		"select_filter": map[string]any{"layer_stack": []string{"storage"}},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-groups/rg-case", raw)
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rg modify: status %d: %s", resp.StatusCode, body)
	}

	rg, err = st.ResourceGroups().Get(t.Context(), "rg-case")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if !slices.Equal(rg.SelectFilter.LayerStack, []string{apiv1.LayerKindStorage}) {
		t.Fatalf("rg modify stored %v, want [STORAGE]", rg.SelectFilter.LayerStack)
	}
}

func postOK(t *testing.T, url string, body any, want int) {
	t.Helper()

	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, url, raw)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != want {
		got, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST %s: status %d, want %d: %s", url, resp.StatusCode, want, got)
	}
}

// An autoplace that gives a definition its first stack stores it in the
// same canonical spelling as every other write path.
func TestStoredLayerStackIsCanonical(t *testing.T) {
	t.Parallel()

	if got := storedLayerStack([]string{"drbd", "storage"}); !slices.Equal(got, wantCanonicalStack()) {
		t.Fatalf("stored %v, want %v", got, wantCanonicalStack())
	}

	if got := storedLayerStack([]string{"bogus"}); !slices.Equal(got, []string{"bogus"}) {
		t.Fatalf("an invalid stack was not kept as given: %v", got)
	}
}

// Each call that hands a definition its first stack through a CSI-style
// layer_list (autoplace, explicit resource create, make-available) stores
// that stack in canonical case, so a lowercase list reads back the way
// every other write path stores it.
func TestCSILayerListIsStoredInCanonicalCase(t *testing.T) {
	t.Parallel()

	lower := []string{"drbd", "storage"}

	for _, tc := range []struct {
		name string
		path string
		body any
		want int
	}{
		{
			name: "autoplace",
			path: "/v1/resource-definitions/rd-csi/autoplace",
			body: apiv1.AutoPlaceRequest{
				SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 1, StoragePool: "pool"},
				LayerList:    lower,
			},
			want: http.StatusOK,
		},
		{
			name: "resource create",
			path: "/v1/resource-definitions/rd-csi/resources",
			body: apiv1.ResourceCreate{
				Resource:  apiv1.Resource{Name: "rd-csi", NodeName: "n1"},
				LayerList: lower,
			},
			want: http.StatusCreated,
		},
		{
			name: "make-available",
			path: "/v1/resource-definitions/rd-csi/resources/n1/make-available",
			body: apiv1.ResourceMakeAvailable{LayerList: lower},
			want: http.StatusOK,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			ctx := t.Context()

			if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd-csi"}); err != nil {
				t.Fatalf("seed rd: %v", err)
			}

			if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
				t.Fatalf("seed node: %v", err)
			}

			if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
				StoragePoolName: "pool", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindLVMThin,
			}); err != nil {
				t.Fatalf("seed pool: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			postOK(t, base+tc.path, tc.body, tc.want)

			rd, err := st.ResourceDefinitions().Get(ctx, "rd-csi")
			if err != nil {
				t.Fatalf("get: %v", err)
			}

			if !slices.Equal(rd.LayerStack, wantCanonicalStack()) {
				t.Fatalf("stored stack %v, want %v", rd.LayerStack, wantCanonicalStack())
			}
		})
	}
}

// A group stored with a lowercase stack before writes were canonicalised is
// not rebalanced when a modify resends the same stack in canonical case.
func TestRGNeedsRebalanceIgnoresTheStackCase(t *testing.T) {
	t.Parallel()

	prev := &apiv1.AutoSelectFilter{LayerStack: []string{"drbd", "storage"}}

	if rgNeedsRebalance(prev, &apiv1.AutoSelectFilter{LayerStack: wantCanonicalStack()}) {
		t.Error("the same stack in another case asked for a rebalance")
	}

	if !rgNeedsRebalance(prev, &apiv1.AutoSelectFilter{LayerStack: []string{"DRBD", "LUKS", "STORAGE"}}) {
		t.Error("a different stack asked for no rebalance")
	}
}
