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
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestBug93ToggleDiskDiskfulRoundTrip pins the regression for Bug 93:
// `linstor r td <node> <rd> --storage-pool <pool>` (used to re-enable
// a previously demoted replica) POSTs `PUT
// .../toggle-disk/diskful/{pool}`. Before this fix that path 404'd
// at the router and the python CLI crashed with
// `xml.etree.ElementTree.ParseError`.
func TestBug93ToggleDiskDiskfulRoundTrip(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-93"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// Replica starts diskless — i.e. the operator has just run
	// `r td --diskless` and now wants to put the disk back.
	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-93",
		NodeName: "n1",
		Flags:    []string{apiv1.ResourceFlagDiskless},
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// `/diskful/{pool}` is the canonical python-linstor 1.27.1 shape.
	resp := httpPut(t, base+"/v1/resource-definitions/pvc-93/resources/n1/toggle-disk/diskful/stand", nil)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode APICallRc envelope: %v", err)
	}

	if len(rc) == 0 || rc[0].Message == "" {
		t.Fatalf("expected non-empty APICallRc envelope, got %+v", rc)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-93", "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS flag still present after toggle-disk/diskful: %v", got.Flags)
	}

	if got.Props["StorPoolName"] != "stand" {
		t.Errorf("Props[StorPoolName]: got %q, want stand", got.Props["StorPoolName"])
	}
}

// TestBug93ToggleDiskDiskfulNoPool pins the no-pool variant
// `PUT .../toggle-disk/diskful` — the controller auto-pick path
// is expected to fill the pool on the next reconcile, but the
// REST envelope must still come back 200 with the typed envelope.
func TestBug93ToggleDiskDiskfulNoPool(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-93b"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-93b",
		NodeName: "n1",
		Flags:    []string{apiv1.ResourceFlagDiskless},
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-93b/resources/n1/toggle-disk/diskful", nil)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-93b", "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS flag still present: %v", got.Flags)
	}
}

// TestBug98SnapshotRollbackCanonicalPath pins the regression for
// Bug 98: `linstor s rollback <rd> <snap>` POSTs
// `POST /v1/resource-definitions/{rd}/snapshot-rollback/{snap}` (NOT
// the legacy `/snapshots/{snap}/rollback` shape that internal callers
// historically used). Both paths must hit the same handler.
func TestBug98SnapshotRollbackCanonicalPath(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-98"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{Name: "snap1", ResourceName: "pvc-98"}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-98/snapshot-rollback/snap1", []byte("{}"))
	defer func() { _ = resp.Body.Close() }()

	// Must NOT be a router 404 — that's the exact failure mode the
	// fix exists to remove. The handler returns 501 (with a
	// structured envelope) because blockstor refuses to expose
	// `zfs rollback` directly; either 501 or 200 is acceptable here,
	// just not 404 or 405.
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed {
		t.Fatalf("status: got %d, want a handler response (not router 404/405)", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode APICallRc envelope: %v", err)
	}

	if len(rc) == 0 {
		t.Fatalf("empty APICallRc envelope")
	}
}

// TestBug99ResourceConnectionListEmpty pins the regression for Bug
// 99: `linstor resource-connection list <rd>` calls
// `GET /v1/resource-definitions/{rd}/resource-connections` and
// expects a JSON array (possibly empty). Before this fix the router
// 404'd and the python CLI crashed.
func TestBug99ResourceConnectionListEmpty(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-99"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/pvc-99/resource-connections")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var arr []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&arr); err != nil {
		t.Fatalf("decode []ResourceConnection envelope: %v", err)
	}

	if arr == nil {
		t.Fatalf("expected `[]`, got nil — python-linstor decodes null as malformed")
	}

	if len(arr) != 0 {
		t.Errorf("expected empty list, got %d entries", len(arr))
	}
}

// TestBug99ResourceConnectionListLegacyPath pins the alias at the
// older `/v1/resource-connections/{rd}` shape — some clients have
// historically hit this URL; we now register both so neither 404s.
func TestBug99ResourceConnectionListLegacyPath(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-99b"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-connections/pvc-99b")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
}

// TestBug100ScheduleListEmpty pins the regression for Bug 100:
// `linstor schedule l` calls `GET /v1/schedules` and decodes the
// body via ScheduleListResponse, which reads `data["data"]` as an
// array. The endpoint must return `{"data": []}` for a controller
// with no schedules — a bare 404 or an array would crash the CLI.
func TestBug100ScheduleListEmpty(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/schedules")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var body struct {
		Data []map[string]any `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode ScheduleListResponse envelope: %v", err)
	}

	if body.Data == nil {
		t.Fatalf("expected `data: []`, got nil — ScheduleListResponse would crash")
	}

	if len(body.Data) != 0 {
		t.Errorf("expected empty schedule list, got %d entries", len(body.Data))
	}
}
