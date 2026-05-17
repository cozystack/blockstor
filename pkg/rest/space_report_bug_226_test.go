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
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 226 (P2) — `linstor space-reporting query` fails because
// blockstor never wired `GET /v1/space-report`. Upstream LINSTOR
// exposes a cluster-wide free/total capacity summary as a single
// `report_text` string (see Java
// `controller/.../SpaceTracking.java` and
// `JsonSpaceTracking.SpaceReport`).
//
// blockstor doesn't run upstream's SpaceTrackingService — we derive
// the report from the StoragePools the store knows about. The wire
// shape stays exact-parity with upstream (`{"report_text": "..."}`)
// so the python CLI parses it without translation. The text body
// summarises each pool's `free_capacity` / `total_capacity` plus a
// final cluster-wide total line — operators get an answer to "how
// much room is left?" without having to fan out per-node.

// TestBug226SpaceReportAggregatesPools: seed 3 storage pools with
// known capacities; GET /v1/space-report; the response body
// `report_text` must mention every pool and a non-zero aggregate
// total. Pre-fix this 404s.
func TestBug226SpaceReportAggregatesPools(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Capacities are upstream-style KiB. Picking values that produce
	// distinct MiB totals (the handler renders KiB → MiB) so the
	// `175` substring assertion below pins the aggregate exactly:
	//   free MiB: 100 + 50 + 25 = 175 MiB
	pools := []apiv1.StoragePool{
		{
			StoragePoolName: "pool-a", NodeName: "n1",
			ProviderKind:  apiv1.StoragePoolKindLVM,
			FreeCapacity:  100 * 1024, // 100 MiB
			TotalCapacity: 200 * 1024,
		},
		{
			StoragePoolName: "pool-b", NodeName: "n2",
			ProviderKind:  apiv1.StoragePoolKindZFS,
			FreeCapacity:  50 * 1024, // 50 MiB
			TotalCapacity: 150 * 1024,
		},
		{
			StoragePoolName: "pool-c", NodeName: "n3",
			ProviderKind:  apiv1.StoragePoolKindLVMThin,
			FreeCapacity:  25 * 1024, // 25 MiB
			TotalCapacity: 75 * 1024,
		},
	}
	for i := range pools {
		if err := st.StoragePools().Create(ctx, &pools[i]); err != nil {
			t.Fatalf("seed pool %s: %v", pools[i].StoragePoolName, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/space-report")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	// Bug 230: the wire field is `reportText` (Jackson default
	// camelCase, matching upstream Java); Bug 226 originally emitted
	// snake_case `report_text` and was corrected in Bug 230.
	var got struct {
		ReportText string `json:"reportText"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	if got.ReportText == "" {
		t.Fatalf("report_text is empty; want a non-empty cluster summary")
	}

	for _, name := range []string{"pool-a", "pool-b", "pool-c"} {
		if !strings.Contains(got.ReportText, name) {
			t.Errorf("report_text missing pool %q; body=%q", name, got.ReportText)
		}
	}

	// The aggregate free capacity must surface (sum = 175 MiB in
	// upstream-equivalent units). We don't pin the exact rendering
	// — operators care that the number is there, not the byte-for-
	// byte formatter — but the underlying free-byte count MUST be
	// reachable in the body somewhere as a stable substring.
	if !strings.Contains(got.ReportText, "175") {
		t.Errorf("report_text missing aggregate free total (175 MiB worth of free); body=%q", got.ReportText)
	}
}

// TestBug226SpaceReportEmptyCluster: no pools registered — the
// endpoint must still succeed and emit a well-formed envelope with
// zeroed totals. Pre-fix 404s.
func TestBug226SpaceReportEmptyCluster(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/space-report")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	// Bug 230: the wire field is `reportText` (Jackson default
	// camelCase, matching upstream Java); Bug 226 originally emitted
	// snake_case `report_text` and was corrected in Bug 230.
	var got struct {
		ReportText string `json:"reportText"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	if got.ReportText == "" {
		t.Errorf("report_text empty on empty cluster; want a non-empty placeholder summary")
	}
}

// TestBug230SpaceReportUsesCamelCaseReportText pins the upstream-
// canonical wire shape for `GET /v1/space-report`. Bug 226 wired the
// handler but emitted the field as snake_case `report_text` (see Java
// `JsonSpaceTracking.java:12`: the canonical field is `reportText`,
// Jackson default camelCase). python-linstor and golinstor's
// `SpaceTrackingService.Query()` both decode the response into a
// struct keyed on `reportText`; the snake_case form decoded as the
// zero value and `linstor space-reporting query` crashed downstream
// with a KeyError on `reportText` access.
//
// The fix is a one-line JSON tag flip on the response struct; this
// test pins that the wire body carries the camelCase key. We decode
// into a loose `map[string]any` rather than a typed struct so the
// assertion is impossible to satisfy by accident — only the actual
// JSON key name on the wire matters.
func TestBug230SpaceReportUsesCamelCaseReportText(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/space-report")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got map[string]any

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	if _, ok := got["reportText"]; !ok {
		t.Errorf("response missing canonical `reportText` key (Bug 230); body=%v", got)
	}

	if _, ok := got["report_text"]; ok {
		t.Errorf("response still emits snake_case `report_text` (Bug 230 — upstream Java uses Jackson default camelCase `reportText`); body=%v", got)
	}
}
