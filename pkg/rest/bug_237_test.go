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
	"io"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 237 is the wave-2 continuation of Bug 232's wire-decode work.
// Bug 222 bumped the advertised `rest_api_version` from 1.23.0 to
// 1.27.0 → python-linstor's `_require_version()` client-side gates
// opened and the CLI now puts fields on the wire that
// DisallowUnknownFields decoders refuse. v26 audit found three
// remaining endpoints still 400'ing for the same class of new
// 1.27-era fields after Bug 232 patched node-evacuate, RD-modify and
// RD-clone:
//
//   - POST /v1/resource-definitions/{rd}/resources                       (ResourceCreate)
//   - POST /v1/resource-definitions/{rd}/resources/{node}/make-available (ResourceMakeAvailable)
//   - POST /v1/resource-definitions/{rd}/autoplace                       (AutoPlaceRequest)
//
// New fields, sourced from `third_party/linstor-openapi/rest_v1_openapi.yaml`:
//
//   | endpoint          | new fields                                                 |
//   |-------------------|------------------------------------------------------------|
//   | resources POST    | drbd_tcp_ports, drbd_tcp_port_count, copy_all_snaps, snap_names |
//   | make-available    | drbd_tcp_ports, copy_all_snaps, snap_names                 |
//   | autoplace         | copy_all_snaps, snap_names                                 |
//
// The contract these tests pin is "the decoder accepts these fields
// without 400". Wire-through semantics for drbd_tcp_ports / port_count /
// copy_all_snaps / snap_names are deeper than the wire shape (they
// require placer-side allocator hooks and snapshot-clone data-plane);
// for now we mirror Bug 232's accept-and-no-op pattern with a TODO,
// so the CLI stops crashing on otherwise-valid commands.

// TestBug237ResourceCreateAcceptsDrbdTcpPortsAndSnapFields pins that
// the `resources` POST handler accepts the four 1.27 fields python-
// linstor `r c --drbd-port`, `r c --drbd-port-count`, the snapshot-
// based clone-resource lowering (`copy_all_snaps`/`snap_names`) put
// on the wire. Pre-fix the DisallowUnknownFields decoder in
// `decodeResourceCreateBody` returns 400 + `json: unknown field
// "drbd_tcp_ports"` and the CLI's create flow crashes.
//
// The test seeds a node + pool so the handler reaches the create
// path, expects HTTP 201 (the upstream success envelope), and
// verifies the Resource landed — proving the decode happened AND the
// rest of the create pipeline ran with the new fields absorbed as
// no-ops.
func TestBug237ResourceCreateAcceptsDrbdTcpPortsAndSnapFields(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// All four 1.27 fields plus the canonical existing fields. The
	// embedded Resource carries the node + pool reference so the
	// handler's existence gates pass; the new fields ride alongside.
	body := []byte(`{
		"resource": {"node_name": "n1", "props": {"StorPoolName": "pool"}},
		"drbd_tcp_ports": [7000, 7001],
		"drbd_tcp_port_count": 2,
		"copy_all_snaps": true,
		"snap_names": ["snap-a", "snap-b"]
	}`)

	resp := httpPost(t, base+"/v1/resource-definitions/rd1/resources", body)
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusBadRequest {
		t.Fatalf("resource create with 1.27 fields should not 400; got 400 body=%s", respBody)
	}

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201 (body=%s)", resp.StatusCode, respBody)
	}

	got, err := st.Resources().Get(ctx, "rd1", "n1")
	if err != nil {
		t.Fatalf("expected Resource to be persisted after decode accepted 1.27 fields: %v", err)
	}

	if got.NodeName != "n1" {
		t.Errorf("NodeName: got %q, want %q", got.NodeName, "n1")
	}
}

// TestBug237ResourceCreateArrayShapeAcceptsNewFields covers the
// `[]ResourceCreate` array body shape that the python CLI's
// multi-node `linstor r c n1 n2 n3 rd` lowers to. The single-object
// shape above and the array shape go through the same
// `decodeResourceCreateBody` helper but via separate branches; an
// over-narrow fix that only patches one branch would silently leave
// the array path 400'ing.
func TestBug237ResourceCreateArrayShapeAcceptsNewFields(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`[
		{
			"resource": {"node_name": "n1", "props": {"StorPoolName": "pool"}},
			"drbd_tcp_ports": [7000],
			"drbd_tcp_port_count": 1,
			"copy_all_snaps": false,
			"snap_names": ["snap-a"]
		}
	]`)

	resp := httpPost(t, base+"/v1/resource-definitions/rd1/resources", body)
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusBadRequest {
		t.Fatalf("array-shape resource create with 1.27 fields should not 400; got 400 body=%s", respBody)
	}

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201 (body=%s)", resp.StatusCode, respBody)
	}
}

// TestBug237MakeAvailableAcceptsDrbdTcpPortsAndSnapFields pins the
// `.../resources/{node}/make-available` handler accepts the
// 1.27 fields. linstor-csi's `Attach` may now ride along with
// `drbd_tcp_ports` / `copy_all_snaps` / `snap_names` on the same
// POST (golinstor's MakeAvailable struct carries them since
// upstream 1.27.0). Pre-fix `decodeMakeAvailableBody`'s
// DisallowUnknownFields returns 400 + "unknown field" and CSI
// can never publish the volume.
func TestBug237MakeAvailableAcceptsDrbdTcpPortsAndSnapFields(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{
		"diskful": false,
		"drbd_tcp_ports": [7000, 7001],
		"copy_all_snaps": true,
		"snap_names": ["snap-1"]
	}`)

	resp := httpPost(t, base+"/v1/resource-definitions/rd1/resources/n1/make-available", body)
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusBadRequest {
		t.Fatalf("make-available with 1.27 fields should not 400; got 400 body=%s", respBody)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body=%s)", resp.StatusCode, respBody)
	}
}

// TestBug237AutoplaceAcceptsCopyAllSnapsAndSnapNames pins the
// autoplace POST handler accepts the snapshot-restore companion
// fields python-linstor sends on
// `linstor rd ap --resource-group rg ... rd-from-snap`. Pre-fix
// `decodeAutoplaceBody`'s DisallowUnknownFields returns 400 +
// `json: unknown field "snap_names"` and the snapshot-restore-
// then-autoplace flow crashes before any placement runs.
func TestBug237AutoplaceAcceptsCopyAllSnapsAndSnapNames(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"n1", "n2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool on %s: %v", n, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{
		"select_filter": {"place_count": 2, "storage_pool": "pool"},
		"copy_all_snaps": true,
		"snap_names": ["snap-a"]
	}`)

	resp := httpPost(t, base+"/v1/resource-definitions/rd1/autoplace", body)
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusBadRequest {
		t.Fatalf("autoplace with copy_all_snaps/snap_names should not 400; got 400 body=%s", respBody)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body=%s)", resp.StatusCode, respBody)
	}
}
