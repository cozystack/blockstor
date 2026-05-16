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

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// bug181Passphrase is the canary value the deny-list MUST scrub from
// every read-side endpoint family covered by Bugs 181-185. Distinct
// from Bug 115's canary so a regression on either fix surfaces in
// the failing-test diff without spillover.
const bug181Passphrase = "hunter2-v9-poc"

// bug181AuxSecret is a sibling canary stamped on `Aux/Some/Secret`-
// shaped keys to cover the substring path of the deny list — every
// endpoint family stamps both keys so a partial fix (only one of the
// two patterns scrubbed) fails the assertion explicitly.
const bug181AuxSecret = "super-secret-v9"

// assertBug181NoLeak asserts the response body does NOT carry either
// canary substring. Substring match is the operationally meaningful
// predicate: any cleartext occurrence is greppable.
func assertBug181NoLeak(t *testing.T, label string, body []byte) {
	t.Helper()

	raw := string(body)
	if strings.Contains(raw, bug181Passphrase) {
		t.Errorf("%s leaked passphrase %q in body: %s", label, bug181Passphrase, body)
	}

	if strings.Contains(raw, bug181AuxSecret) {
		t.Errorf("%s leaked aux secret %q in body: %s", label, bug181AuxSecret, body)
	}
}

// assertBug181HasRedaction confirms the redaction marker is present
// somewhere in the body — covers both the bare `<redacted>` and the
// HTML-escaped `<redacted>` form Go's `json.Encoder` emits
// by default.
func assertBug181HasRedaction(t *testing.T, label string, body []byte) {
	t.Helper()

	raw := string(body)
	if !strings.Contains(raw, redactedPropValue) &&
		!strings.Contains(raw, htmlEscapedRedactedMarker) {
		t.Errorf("%s missing redaction marker in body: %s", label, body)
	}
}

// -----------------------------------------------------------------------
// Bug 181 — ResourceGroup GET / LIST
// -----------------------------------------------------------------------

// TestBug181RGGetRedactsPassphrase covers
// `GET /v1/resource-groups/{rg}` — the surface `linstor rg lp <rg>`
// hits. RG.Props is emitted verbatim; without the redaction the
// passphrase rendered in cleartext on every read.
func TestBug181RGGetRedactsPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rgleak",
		Props: map[string]string{
			"DrbdOptions/EncryptPassphrase": bug181Passphrase,
			"DrbdOptions/Some/Secret":       bug181AuxSecret,
			"DrbdOptions/PingTimeout":       "200",
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-groups/rgleak")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBug181NoLeak(t, "GET /v1/resource-groups/{rg}", body)
	assertBug181HasRedaction(t, "GET /v1/resource-groups/{rg}", body)
}

// TestBug181RGListRedactsPassphrase covers the list surface
// `GET /v1/resource-groups` (`linstor rg l`). Same prop, same leak,
// applied across the slice.
func TestBug181RGListRedactsPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rgleak",
		Props: map[string]string{
			"DrbdOptions/EncryptPassphrase": bug181Passphrase,
			"DrbdOptions/Some/Secret":       bug181AuxSecret,
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-groups")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBug181NoLeak(t, "GET /v1/resource-groups", body)
	assertBug181HasRedaction(t, "GET /v1/resource-groups", body)
}

// -----------------------------------------------------------------------
// Bug 182 — Snapshot GET / LIST / view
// -----------------------------------------------------------------------

// seedSnapshotWithSensitiveProps stages a snapshot whose parent RD
// carries the canary props on its Props map. `hydrateSnapshotFromRD`
// copies the parent-RD props into THREE bags on the snapshot
// (`Props`, `SnapshotDefinitionProps`, `ResourceDefinitionProps`) —
// the test seeds the snapshot directly with all three bags populated
// so the assertion exercises every copy site.
func seedSnapshotWithSensitiveProps(t *testing.T, st store.Store, rd, snap string) {
	t.Helper()

	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: rd,
		Props: map[string]string{
			"DrbdOptions/EncryptPassphrase": bug181Passphrase,
			"DrbdOptions/Some/Secret":       bug181AuxSecret,
		},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         snap,
		ResourceName: rd,
		Props: map[string]string{
			"DrbdOptions/EncryptPassphrase": bug181Passphrase,
			"DrbdOptions/Some/Secret":       bug181AuxSecret,
		},
		SnapshotDefinitionProps: map[string]string{
			"DrbdOptions/EncryptPassphrase": bug181Passphrase,
		},
		ResourceDefinitionProps: map[string]string{
			"DrbdOptions/EncryptPassphrase": bug181Passphrase,
			"DrbdOptions/Some/Secret":       bug181AuxSecret,
		},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{
			VolumeNumber: 0,
			SizeKib:      1024,
			VolumeDefinitionProps: map[string]string{
				"DrbdOptions/EncryptPassphrase": bug181Passphrase,
			},
		}},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}
}

// TestBug182SnapshotGetRedactsPassphrase covers
// `GET /v1/resource-definitions/{rd}/snapshots/{snap}`. The snapshot
// carries the passphrase in three different prop bags + the per-VD
// `volume_definition_props` slot; all four sites must scrub.
func TestBug182SnapshotGetRedactsPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedSnapshotWithSensitiveProps(t, st, "rd182", "snap182")

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/rd182/snapshots/snap182")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBug181NoLeak(t, "GET snapshot", body)
	assertBug181HasRedaction(t, "GET snapshot", body)
}

// TestBug182SnapshotListRedactsPassphrase covers
// `GET /v1/resource-definitions/{rd}/snapshots`.
func TestBug182SnapshotListRedactsPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedSnapshotWithSensitiveProps(t, st, "rd182", "snap182")

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/rd182/snapshots")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBug181NoLeak(t, "LIST snapshots", body)
	assertBug181HasRedaction(t, "LIST snapshots", body)
}

// TestBug182SnapshotViewRedactsPassphrase covers the aggregated
// `GET /v1/view/snapshots` surface (`linstor s l`). All three prop
// bags + the per-VD slot must scrub before the wire emit.
func TestBug182SnapshotViewRedactsPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedSnapshotWithSensitiveProps(t, st, "rd182", "snap182")

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/snapshots")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBug181NoLeak(t, "GET /v1/view/snapshots", body)
	assertBug181HasRedaction(t, "GET /v1/view/snapshots", body)
}

// -----------------------------------------------------------------------
// Bug 183 — Node GET / LIST
// -----------------------------------------------------------------------

// seedNodeWithSensitiveProps stages a Node whose Props bag carries
// both canary values.
func seedNodeWithSensitiveProps(t *testing.T, st store.Store, name string) {
	t.Helper()

	if err := st.Nodes().Create(t.Context(), &apiv1.Node{
		Name: name,
		Type: apiv1.NodeTypeSatellite,
		Props: map[string]string{
			"DrbdOptions/Encryption/Foo": bug181Passphrase,
			"Aux/Some/Secret":            bug181AuxSecret,
			"Aux/team":                   "blue",
		},
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}
}

// TestBug183NodeGetRedactsPassphrase covers `GET /v1/nodes/{node}`.
func TestBug183NodeGetRedactsPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedNodeWithSensitiveProps(t, st, "alpha")

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/nodes/alpha")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBug181NoLeak(t, "GET /v1/nodes/{node}", body)
	assertBug181HasRedaction(t, "GET /v1/nodes/{node}", body)
}

// TestBug183NodeListRedactsPassphrase covers `GET /v1/nodes`.
func TestBug183NodeListRedactsPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedNodeWithSensitiveProps(t, st, "alpha")

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/nodes")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBug181NoLeak(t, "GET /v1/nodes", body)
	assertBug181HasRedaction(t, "GET /v1/nodes", body)
}

// -----------------------------------------------------------------------
// Bug 184 — StoragePool GET / LIST (per-node + cluster view)
// -----------------------------------------------------------------------

// seedStoragePoolWithSensitiveProps stages a StoragePool whose Props
// bag carries both canary values.
func seedStoragePoolWithSensitiveProps(t *testing.T, st store.Store, node, pool string) {
	t.Helper()

	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: node,
		Type: apiv1.NodeTypeSatellite,
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: pool,
		NodeName:        node,
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props: map[string]string{
			"StorDriver/EncryptPassphrase": bug181Passphrase,
			"Aux/Some/Secret":              bug181AuxSecret,
		},
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}
}

// TestBug184StoragePoolGetRedactsPassphrase covers
// `GET /v1/nodes/{node}/storage-pools/{pool}`.
func TestBug184StoragePoolGetRedactsPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedStoragePoolWithSensitiveProps(t, st, "alpha", "thinpool")

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/nodes/alpha/storage-pools/thinpool")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBug181NoLeak(t, "GET storage-pool", body)
	assertBug181HasRedaction(t, "GET storage-pool", body)
}

// TestBug184StoragePoolListRedactsPassphrase covers the per-node
// list `GET /v1/nodes/{node}/storage-pools` plus the cluster-wide
// `GET /v1/view/storage-pools` view (`linstor sp l`).
func TestBug184StoragePoolListRedactsPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedStoragePoolWithSensitiveProps(t, st, "alpha", "thinpool")

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Per-node list path.
	resp := httpGet(t, base+"/v1/nodes/alpha/storage-pools")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBug181NoLeak(t, "GET per-node storage-pools", body)
	assertBug181HasRedaction(t, "GET per-node storage-pools", body)

	// Cluster-wide view path.
	viewResp := httpGet(t, base+"/v1/view/storage-pools")
	defer func() { _ = viewResp.Body.Close() }()

	if viewResp.StatusCode != http.StatusOK {
		t.Fatalf("view status: got %d, want 200", viewResp.StatusCode)
	}

	viewBody, _ := readAllBody(viewResp)
	assertBug181NoLeak(t, "GET /v1/view/storage-pools", viewBody)
	assertBug181HasRedaction(t, "GET /v1/view/storage-pools", viewBody)
}

// -----------------------------------------------------------------------
// Bug 185 — VolumeDefinition GET / LIST / view
// -----------------------------------------------------------------------

// seedVDWithSensitiveProps stages a VolumeDefinition whose Props bag
// carries both canary values, under a parent RD.
func seedVDWithSensitiveProps(t *testing.T, st store.Store, rd string, vn int32) {
	t.Helper()

	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: rd,
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, rd, &apiv1.VolumeDefinition{
		VolumeNumber: vn,
		SizeKib:      1024 * 1024,
		Props: map[string]string{
			"DrbdOptions/EncryptPassphrase": bug181Passphrase,
			"Aux/Some/Secret":               bug181AuxSecret,
			"DrbdOptions/PingTimeout":       "200",
		},
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}
}

// TestBug185VDGetRedactsPassphrase covers
// `GET /v1/resource-definitions/{rd}/volume-definitions/{vn}`.
func TestBug185VDGetRedactsPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedVDWithSensitiveProps(t, st, "rd185", 0)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/rd185/volume-definitions/0")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBug181NoLeak(t, "GET VD", body)
	assertBug181HasRedaction(t, "GET VD", body)
}

// TestBug185VDListRedactsPassphrase covers
// `GET /v1/resource-definitions/{rd}/volume-definitions` plus the
// cluster-wide `GET /v1/view/volume-definitions` aggregate.
func TestBug185VDListRedactsPassphrase(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedVDWithSensitiveProps(t, st, "rd185", 0)

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Per-RD list.
	resp := httpGet(t, base+"/v1/resource-definitions/rd185/volume-definitions")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, _ := readAllBody(resp)
	assertBug181NoLeak(t, "LIST per-RD VDs", body)
	assertBug181HasRedaction(t, "LIST per-RD VDs", body)

	// Cluster-wide aggregate.
	viewResp := httpGet(t, base+"/v1/view/volume-definitions")
	defer func() { _ = viewResp.Body.Close() }()

	if viewResp.StatusCode != http.StatusOK {
		t.Fatalf("view status: got %d, want 200", viewResp.StatusCode)
	}

	viewBody, _ := readAllBody(viewResp)
	assertBug181NoLeak(t, "GET /v1/view/volume-definitions", viewBody)
	assertBug181HasRedaction(t, "GET /v1/view/volume-definitions", viewBody)
}
