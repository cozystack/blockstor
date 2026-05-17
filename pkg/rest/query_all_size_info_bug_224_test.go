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

// TestQueryAllSizeInfoCanonicalURL pins Bug 224: the upstream LINSTOR
// REST API mounts query-all-size-info under the `/v1/queries/...`
// namespace —
// `POST /v1/queries/resource-groups/query-all-size-info` — but the Go
// apiserver registered the handler at the bare `/v1/query-all-size-info`
// path. python-linstor / golinstor / piraeus tooling that already
// speaks the canonical URL got a 404 envelope instead of the per-RG
// capacity rollup.
//
// The fix re-registers the handler under the canonical path; the legacy
// path remains as an alias (see TestQueryAllSizeInfoLegacyAlias below)
// for one release so anything still pointing at the old URL keeps
// working.
func TestQueryAllSizeInfoCanonicalURL(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg-1",
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 1, StoragePool: "pool"},
	}); err != nil {
		t.Fatalf("seed rg: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVM,
		FreeCapacity:    2048,
		TotalCapacity:   4096,
	}); err != nil {
		t.Fatalf("seed sp: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t,
		base+"/v1/queries/resource-groups/query-all-size-info",
		[]byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 "+
			"(Bug 224: canonical upstream URL is "+
			"/v1/queries/resource-groups/query-all-size-info)",
			resp.StatusCode)
	}

	var got queryAllSizeInfoResponse

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got.Result) != 1 {
		t.Fatalf("RG count: got %d, want 1; got=%+v", len(got.Result), got)
	}

	if _, ok := got.Result["rg-1"]; !ok {
		t.Errorf("rg-1 missing from result; got=%+v", got.Result)
	}
}

// TestQueryAllSizeInfoLegacyAlias keeps the pre-Bug-224 URL working for
// one release so callers pinning the legacy path don't break in lockstep
// with the canonical-URL fix. The alias is intentionally kept short-
// lived — once tooling has rolled forward it can be removed.
func TestQueryAllSizeInfoLegacyAlias(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg-1",
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 1},
	}); err != nil {
		t.Fatalf("seed rg: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/query-all-size-info", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 "+
			"(Bug 224: legacy URL must remain as a one-release alias)",
			resp.StatusCode)
	}

	var got queryAllSizeInfoResponse

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got.Result) != 1 {
		t.Errorf("RG count: got %d, want 1", len(got.Result))
	}
}
