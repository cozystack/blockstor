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

// Bug 195 (P2 SPEC): upstream LINSTOR's OpenAPI declares
// `/v1/stats/{kind}` as a family of count endpoints — each returning
// the canonical `{"count": N}` shape (ResourceDefinitionStats /
// ResourceStats / StoragePoolStats schemas all literally pin
// `required: [count]; count: int64`). `linstor controller list-stats`
// invokes the family; pre-fix blockstor wired only the legacy
// aggregate `/v1/stats` that returns a multi-key map, and the CLI
// crashed trying to read `.count` off every reply.
//
// The TDD here pins the six sub-paths the CLI hits — three from
// upstream OpenAPI (resource-definitions, resources, storage-pools)
// plus three blockstor extensions covering objects upstream tracks
// indirectly (volume-definitions, volumes, snapshots). The aggregate
// stays as a soft-deprecated convenience for non-CLI scrapers — the
// last test pins backward compatibility.

// TestBug195StatsResourceDefinitionsCountMatches: GET
// `/v1/stats/resource-definitions` returns the upstream `{"count": N}`
// shape with N == len(RDs in store).
func TestBug195StatsResourceDefinitionsCountMatches(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, name := range []string{"rd-a", "rd-b", "rd-c", "rd-d", "rd-e"} {
		if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: name}); err != nil {
			t.Fatalf("seed RD %s: %v", name, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	got := requireStatsCount(t, base+"/v1/stats/resource-definitions")
	if got != 5 {
		t.Errorf("count: got %d, want 5", got)
	}
}

// TestBug195StatsResourcesCountMatches: GET `/v1/stats/resources`.
func TestBug195StatsResourcesCountMatches(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, node := range []string{"n1", "n2", "n3", "n4", "n5"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "rd1", NodeName: node}); err != nil {
			t.Fatalf("seed Resource %s: %v", node, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	got := requireStatsCount(t, base+"/v1/stats/resources")
	if got != 5 {
		t.Errorf("count: got %d, want 5", got)
	}
}

// TestBug195StatsStoragePoolsCountMatches: GET `/v1/stats/storage-pools`.
func TestBug195StatsStoragePoolsCountMatches(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for i, node := range []string{"n1", "n2", "n3", "n4", "n5"} {
		_ = i

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool", NodeName: node, ProviderKind: apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed SP %s: %v", node, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	got := requireStatsCount(t, base+"/v1/stats/storage-pools")
	if got != 5 {
		t.Errorf("count: got %d, want 5", got)
	}
}

// TestBug195StatsVolumeDefinitionsCountMatches: GET
// `/v1/stats/volume-definitions`. VDs are nested under RDs in the
// store; the handler aggregates across every RD.
func TestBug195StatsVolumeDefinitionsCountMatches(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Two RDs with mixed VD counts (2 + 3 = 5 total) so the
	// aggregation is exercised, not just a single-RD trivial case.
	for _, rd := range []string{"rd1", "rd2"} {
		if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rd}); err != nil {
			t.Fatalf("seed RD %s: %v", rd, err)
		}
	}

	for _, vn := range []int32{0, 1} {
		if err := st.VolumeDefinitions().Create(ctx, "rd1", &apiv1.VolumeDefinition{
			VolumeNumber: vn, SizeKib: 1048576,
		}); err != nil {
			t.Fatalf("seed VD rd1/%d: %v", vn, err)
		}
	}

	for _, vn := range []int32{0, 1, 2} {
		if err := st.VolumeDefinitions().Create(ctx, "rd2", &apiv1.VolumeDefinition{
			VolumeNumber: vn, SizeKib: 1048576,
		}); err != nil {
			t.Fatalf("seed VD rd2/%d: %v", vn, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	got := requireStatsCount(t, base+"/v1/stats/volume-definitions")
	if got != 5 {
		t.Errorf("count: got %d, want 5", got)
	}
}

// TestBug195StatsVolumesCountMatches: GET `/v1/stats/volumes`. Volumes
// live inline on Resource.Volumes; the handler sums across resources.
func TestBug195StatsVolumesCountMatches(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rdv"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// 2 resources, each carrying 2+3 volumes = 5 total.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "rdv", NodeName: "n1",
		Volumes: []apiv1.Volume{{VolumeNumber: 0}, {VolumeNumber: 1}},
	}); err != nil {
		t.Fatalf("seed R1: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "rdv", NodeName: "n2",
		Volumes: []apiv1.Volume{{VolumeNumber: 0}, {VolumeNumber: 1}, {VolumeNumber: 2}},
	}); err != nil {
		t.Fatalf("seed R2: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	got := requireStatsCount(t, base+"/v1/stats/volumes")
	if got != 5 {
		t.Errorf("count: got %d, want 5", got)
	}
}

// TestBug195StatsSnapshotsCountMatches: GET `/v1/stats/snapshots`.
func TestBug195StatsSnapshotsCountMatches(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for i, sn := range []string{"s1", "s2", "s3", "s4", "s5"} {
		_ = i

		if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{Name: sn, ResourceName: "rdx"}); err != nil {
			t.Fatalf("seed snapshot %s: %v", sn, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	got := requireStatsCount(t, base+"/v1/stats/snapshots")
	if got != 5 {
		t.Errorf("count: got %d, want 5", got)
	}
}

// TestBug195StatsAggregateBackwardCompatible: the legacy aggregate
// `/v1/stats` shape must keep working — non-CLI scrapers (Prometheus
// pre-1.21 ServiceMonitors, hand-rolled curl dashboards) consume the
// multi-key map straight; flipping the response shape would break
// them. The Bug 195 sub-paths are additive, not replacements.
func TestBug195StatsAggregateBackwardCompatible(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/stats")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("aggregate status: got %d, want 200", resp.StatusCode)
	}

	var got map[string]int
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode aggregate: %v", err)
	}

	for k, want := range map[string]int{
		"nodes":                1,
		"resource_definitions": 1,
	} {
		if got[k] != want {
			t.Errorf("aggregate[%s]: got %d, want %d", k, got[k], want)
		}
	}
}

// requireStatsCount GETs the URL and decodes the upstream
// `{"count": N}` envelope. Pins both the HTTP status and the schema
// shape — a 200 with the wrong field name (e.g. `total`) is just as
// broken to `linstor controller list-stats` as a 404.
func requireStatsCount(t *testing.T, url string) int64 {
	t.Helper()

	resp := httpGet(t, url)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status for %s: got %d, want 200", url, resp.StatusCode)
	}

	var got struct {
		Count int64 `json:"count"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}

	return got.Count
}
