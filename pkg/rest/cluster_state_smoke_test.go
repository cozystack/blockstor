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

// Wave2 scenario 1.W05 — `cluster-state` smoke against `n l` +
// `sp l` + `r l`. The three lists are the operator's quick health
// sanity check ("everything Online / UpToDate?") and CSI's bootstrap
// path (ListVolumes does a `view/resources` walk; `view/storage-pools`
// drives autoplace candidate discovery). The unit suite pins the
// shared wire-envelope shape (JSON arrays, never null, filter-narrowed
// runs are a strict subset of the unfiltered run) so a regression that
// flips any list into a wrapper object breaks loud and early instead
// of silently breaking the Python CLI's `linstor n l --machine-readable`
// JSON consumer or the Go-side decode in linstor-csi.

// TestClusterStateSmokeEnvelope hits the three list endpoints
// against a healthy 3-worker seed and pins:
//  1. Each list returns a JSON array (no wrapper object).
//  2. All rows are surfaced (no silent drops).
//  3. Filter-narrowed runs are a strict subset of the unfiltered run.
//
// Single test covers the three commands together — the contract here
// is the cross-command consistency, not per-handler correctness
// (the per-handler suites cover that already).
func TestClusterStateSmokeEnvelope(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	// Seed a 3-worker stand. Each node:
	//   - Type=SATELLITE, ConnectionStatus=ONLINE
	//   - one LVM storage pool named "pool"
	//   - one Resource (replica of RD "rd1") with DrbdState=UpToDate
	for _, name := range []string{"alpha", "bravo", "charlie"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name:             name,
			Type:             apiv1.NodeTypeSatellite,
			ConnectionStatus: "ONLINE",
		}); err != nil {
			t.Fatalf("seed node %s: %v", name, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        name,
			ProviderKind:    apiv1.StoragePoolKindLVM,
			FreeCapacity:    1024,
			TotalCapacity:   2048,
		}); err != nil {
			t.Fatalf("seed pool on %s: %v", name, err)
		}

		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     "rd1",
			NodeName: name,
			State:    apiv1.ResourceState{DrbdState: "UpToDate"},
		}); err != nil {
			t.Fatalf("seed resource on %s: %v", name, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	t.Cleanup(stop)

	t.Run("nodes_list_envelope", func(t *testing.T) {
		t.Parallel()

		nodes := decodeJSONArray[apiv1.Node](t, base+"/v1/nodes")
		if len(nodes) != 3 {
			t.Fatalf("nodes: got %d, want 3", len(nodes))
		}

		for i := range nodes {
			if nodes[i].ConnectionStatus != "ONLINE" {
				t.Errorf("node %s: ConnectionStatus=%q, want ONLINE",
					nodes[i].Name, nodes[i].ConnectionStatus)
			}
		}
	})

	t.Run("storage_pools_view_envelope", func(t *testing.T) {
		t.Parallel()

		pools := decodeJSONArray[apiv1.StoragePool](t, base+"/v1/view/storage-pools")
		if len(pools) != 3 {
			t.Fatalf("storage pools: got %d, want 3", len(pools))
		}

		// Filter-narrow check: ?nodes=alpha → strict subset (1 row).
		narrow := decodeJSONArray[apiv1.StoragePool](t, base+"/v1/view/storage-pools?nodes=alpha")
		if len(narrow) != 1 {
			t.Errorf("filtered pools: got %d, want 1 (?nodes=alpha)", len(narrow))
		}

		if len(narrow) == 1 && narrow[0].NodeName != "alpha" {
			t.Errorf("filtered pool node: got %q, want alpha", narrow[0].NodeName)
		}
	})

	t.Run("resources_view_envelope", func(t *testing.T) {
		t.Parallel()

		resources := decodeJSONArray[apiv1.ResourceWithVolumes](t, base+"/v1/view/resources")
		if len(resources) != 3 {
			t.Fatalf("resources: got %d, want 3", len(resources))
		}

		for i := range resources {
			if resources[i].State.DrbdState != "UpToDate" {
				t.Errorf("resource on %s: DrbdState=%q, want UpToDate",
					resources[i].NodeName, resources[i].State.DrbdState)
			}
		}

		// Filter-narrow: ?nodes=bravo → strict subset (1 row).
		narrow := decodeJSONArray[apiv1.ResourceWithVolumes](t, base+"/v1/view/resources?nodes=bravo")
		if len(narrow) != 1 {
			t.Errorf("filtered resources: got %d, want 1 (?nodes=bravo)", len(narrow))
		}

		if len(narrow) == 1 && narrow[0].NodeName != "bravo" {
			t.Errorf("filtered resource node: got %q, want bravo", narrow[0].NodeName)
		}
	})
}

// TestClusterStateSmokeEmptyArrays pins the wire shape against an
// empty cluster: each list MUST be `[]`, not `null`. linstor-csi's
// JSON decode treats `null` as a parse error and the Python CLI's
// machine-readable output flips into "fail" mode on a wrapper-vs-array
// shape mismatch. A regression that introduces a wrapper / drops the
// non-nil guard surfaces here.
func TestClusterStateSmokeEmptyArrays(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	for _, url := range []string{
		base + "/v1/nodes",
		base + "/v1/view/storage-pools",
		base + "/v1/view/resources",
	} {
		raw := decodeRawJSON(t, url)
		if len(raw) == 0 || raw[0] != '[' {
			t.Errorf("%s: body must start with `[` (got %q ...)", url, firstBytes(raw, 16))
		}
	}
}

// decodeJSONArray issues a GET and decodes the body as []T. Fatals
// on transport / decode errors so the smoke test focuses on contract
// invariants, not HTTP plumbing.
func decodeJSONArray[T any](t *testing.T, url string) []T {
	t.Helper()

	resp := httpGet(t, url)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d, want 200", url, resp.StatusCode)
	}

	var out []T
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}

	return out
}

// decodeRawJSON returns the raw body bytes; used to assert the
// outer wrapping shape ('[' vs '{') before any per-key decoding.
func decodeRawJSON(t *testing.T, url string) []byte {
	t.Helper()

	resp := httpGet(t, url)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d, want 200", url, resp.StatusCode)
	}

	buf := make([]byte, 1024)
	n, _ := resp.Body.Read(buf)

	return buf[:n]
}

func firstBytes(b []byte, n int) string {
	if len(b) < n {
		return string(b)
	}

	return string(b[:n])
}
