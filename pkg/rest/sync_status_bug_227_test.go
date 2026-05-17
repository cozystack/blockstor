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

// Bug 227 (P3) — `GET /v1/resource-definitions/{rd}/sync-status` was
// missing. Upstream LINSTOR exposes the endpoint to answer
// "is every replica of <rd> done resyncing?" with a single boolean
// (`synced_on_all`) — Java
// `controller/.../ResourceDefinitions.java:getSyncStatus` and
// `JsonGenTypes.ResourceDefinitionSyncStatus`. Driven by the python
// CLI's `linstor rd sync-status` and used by the snapshot-shipping
// flow to gate on "every peer is UpToDate before we take the snap".
//
// blockstor derives `synced_on_all` from per-replica `DrbdState`:
// every replica reporting `UpToDate` (or no replica reporting a
// non-UpToDate state) → true; any replica still in `SyncTarget` /
// `SyncSource` / `Outdated` / `Inconsistent` → false.

// TestBug227SyncStatusAllUpToDate: every replica's DrbdState is
// `UpToDate` → `synced_on_all` is true. Pre-fix 404s.
func TestBug227SyncStatusAllUpToDate(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd-227"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, node := range []string{"n1", "n2", "n3"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     "rd-227",
			NodeName: node,
			State:    apiv1.ResourceState{DrbdState: "UpToDate"},
		}); err != nil {
			t.Fatalf("seed resource %s: %v", node, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/rd-227/sync-status")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got struct {
		SyncedOnAll bool `json:"synced_on_all"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	if !got.SyncedOnAll {
		t.Errorf("synced_on_all: got false, want true (every replica is UpToDate)")
	}
}

// TestBug227SyncStatusOneSyncTarget: one replica still in
// `SyncTarget` → `synced_on_all` must be false. The python CLI's
// snapshot pre-check relies on this to refuse a snapshot of a
// resync-in-progress RD.
func TestBug227SyncStatusOneSyncTarget(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd-227"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "rd-227", NodeName: "n1",
		State: apiv1.ResourceState{DrbdState: "UpToDate"},
	}); err != nil {
		t.Fatalf("seed n1: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "rd-227", NodeName: "n2",
		State: apiv1.ResourceState{DrbdState: "SyncTarget"},
	}); err != nil {
		t.Fatalf("seed n2: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/rd-227/sync-status")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got struct {
		SyncedOnAll bool `json:"synced_on_all"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	if got.SyncedOnAll {
		t.Errorf("synced_on_all: got true, want false (n2 is SyncTarget)")
	}
}

// TestBug227SyncStatusUnknownRD: an unknown RD must 404.
func TestBug227SyncStatusUnknownRD(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/ghost-rd/sync-status")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}
