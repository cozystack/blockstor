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

// This file is the preventive-hardening backport (audit item v25 #236)
// of the v23 anti-tautology pattern (the map[string]any decode witness
// introduced for the space-report camelCase fix). The fix-time tests
// for items #220 through #229 all decode into typed Go structs whose
// JSON tags mirror what the handler emits — meaning a JSON-tag drift
// landing in both producer and test struct in the same patch would
// pass the typed-decode assertion despite breaking python-linstor /
// golinstor on the wire.
//
// Each witness here decodes the same endpoint response into a loose
// `map[string]any` and asserts the EXACT upstream Java field names
// (mirroring the JsonGenTypes / per-handler model in
// linstor-server). If a future patch flips a snake_case key to
// camelCase (or vice versa) on either side independently of the test
// struct, the witness trips.
//
// Scope: one witness per endpoint touched by items #220 / #224 / #225
// / #227 / #228 / #229. Item #226 already has a witness via the
// space-report camelCase fix; #221 shares the wire shape with #220 so
// the volume-witness covers both.

// TestWireKeysVolumesList witnesses the upstream Volume DTO wire keys
// emitted by `GET /v1/resource-definitions/{rd}/resources/{node}/volumes`
// (items #220 / #221). Upstream `controller/.../Volumes.java` exposes
// the Volume model with snake_case Jackson tags: `volume_number`,
// `storage_pool_name`, `device_path`, `allocated_size_kib`. python-
// linstor's `ResourceData.volumes[i].vlm_nr` and golinstor's
// `client.Volume.VolumeNumber` both decode against these exact keys.
//
// A typed-struct decode in the existing fix tests cannot catch a
// drift where both the producer and the test struct flip to e.g.
// camelCase `volumeNumber` — the round-trip would still pass while
// every external consumer broke. This witness pins the exact wire
// keys via the loose-map decode pattern.
func TestWireKeysVolumesList(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "wk-vol"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "wk-vol", NodeName: "n1",
		Volumes: []apiv1.Volume{
			{
				VolumeNumber: 0, StoragePool: "pool0",
				DevicePath: "/dev/drbd1000", AllocatedKib: 1024,
			},
		},
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/wk-vol/resources/n1/volumes")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []map[string]any

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	if len(got) == 0 {
		t.Fatalf("response: empty list (seed leaked); body=%v", got)
	}

	// Required upstream-canonical wire keys per Java Volume DTO. Every
	// one of these MUST be present on every emitted Volume; missing
	// any one breaks the python CLI's `linstor v l` column rendering
	// AND golinstor's ResourceService.GetVolumes decoder.
	required := []string{
		"volume_number",
		"storage_pool_name",
		"device_path",
		"allocated_size_kib",
	}

	for _, key := range required {
		if _, ok := got[0][key]; !ok {
			t.Errorf("Volume[0] missing canonical wire key %q (upstream Java Volume DTO uses Jackson snake_case); body=%v",
				key, got[0])
		}
	}

	// Camel-case drift sentinels: if any of these appear, the JSON
	// tags flipped to Jackson default camelCase and external consumers
	// break. Pin the inverse so a future drift trips immediately.
	forbidden := []string{
		"volumeNumber",
		"storagePoolName",
		"devicePath",
		"allocatedSizeKib",
	}

	for _, key := range forbidden {
		if _, ok := got[0][key]; ok {
			t.Errorf("Volume[0] emits camelCase key %q; upstream Java DTO uses snake_case — flipping breaks python-linstor / golinstor; body=%v",
				key, got[0])
		}
	}
}

// TestWireKeysVolumeSingleGet witnesses the same DTO on the single-GET
// path. Upstream serves the per-vlmNr endpoint with the identical wire
// shape; a future patch that adds wrapping or flips tags on only one
// of the two routes would slip past the list-only witness above.
func TestWireKeysVolumeSingleGet(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "wk-vol1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "wk-vol1", NodeName: "n1",
		Volumes: []apiv1.Volume{
			{VolumeNumber: 0, StoragePool: "p", DevicePath: "/dev/x", AllocatedKib: 1024},
		},
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/wk-vol1/resources/n1/volumes/0")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got map[string]any

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	// Single-GET wraps nothing — the body is a bare Volume DTO.
	for _, key := range []string{"volume_number", "storage_pool_name", "device_path"} {
		if _, ok := got[key]; !ok {
			t.Errorf("single Volume missing canonical wire key %q; body=%v", key, got)
		}
	}

	for _, key := range []string{"volumeNumber", "storagePoolName", "devicePath"} {
		if _, ok := got[key]; ok {
			t.Errorf("single Volume emits camelCase key %q; upstream Java DTO uses snake_case; body=%v", key, got)
		}
	}
}

// TestWireKeysQueryAllSizeInfo witnesses the upstream
// `JsonGenTypes.QueryAllSizeInfoResponse` wire shape for
// `POST /v1/queries/resource-groups/query-all-size-info` (item #224).
// The top-level envelope is `{"result": {<rg>: {...}}}` and the
// inner per-RG payload exposes `space_info` (with nested
// `max_vlm_size_in_kib`, `capacity_in_kib`, `available_size_in_kib`).
// python-linstor's `ResourceGroupResponse.result` and golinstor's
// `QueryAllSizeInfoResponse` decode against these exact keys.
func TestWireKeysQueryAllSizeInfo(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg-wk",
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
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got map[string]any

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	result, ok := got["result"].(map[string]any)
	if !ok {
		t.Fatalf("response missing top-level `result` map (upstream Java QueryAllSizeInfoResponse); body=%v", got)
	}

	rg, ok := result["rg-wk"].(map[string]any)
	if !ok {
		t.Fatalf("result missing `rg-wk` entry; body=%v", got)
	}

	if _, ok := rg["space_info"]; !ok {
		t.Errorf("per-RG payload missing canonical wire key `space_info`; body=%v", rg)
	}

	if _, ok := rg["spaceInfo"]; ok {
		t.Errorf("per-RG payload emits camelCase `spaceInfo`; upstream uses snake_case; body=%v", rg)
	}
}

// TestWireKeysSnapshotRestoreVD witnesses the upstream `[]ApiCallRc`
// envelope emitted by
// `POST /v1/resource-definitions/{rd}/snapshot-restore-volume-definition/{snap}`
// (item #225). The envelope is the standard upstream shape with
// snake_case `ret_code` / `message` / `obj_refs` keys — python-
// linstor's `apiconsts.ApiCallResponse.ret_code` and golinstor's
// `client.ApiCallError.RetCode` both decode against these. A future
// JSON-tag flip on the shared APICallRc type would silently break
// every endpoint that returns it; this witness pins the contract on
// the post-fix path.
func TestWireKeysSnapshotRestoreVD(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "src-wk"}); err != nil {
		t.Fatalf("seed source RD: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "tgt-wk"}); err != nil {
		t.Fatalf("seed target RD: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-wk",
		ResourceName: "src-wk",
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024},
		},
	}); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{"to_resource": "tgt-wk"})

	resp := httpPost(t,
		base+"/v1/resource-definitions/src-wk/snapshot-restore-volume-definition/snap-wk",
		body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []map[string]any

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	if len(got) == 0 {
		t.Fatalf("expected non-empty []ApiCallRc envelope; body=%v", got)
	}

	if _, ok := got[0]["ret_code"]; !ok {
		t.Errorf("ApiCallRc envelope missing canonical wire key `ret_code`; body=%v", got[0])
	}

	if _, ok := got[0]["retCode"]; ok {
		t.Errorf("ApiCallRc envelope emits camelCase `retCode`; upstream uses snake_case `ret_code`; body=%v", got[0])
	}
}

// TestWireKeysSyncStatus witnesses the upstream
// `JsonGenTypes.ResourceDefinitionSyncStatus` wire shape for
// `GET /v1/resource-definitions/{rd}/sync-status` (item #227). The
// single boolean field is `synced_on_all` (snake_case); the python
// CLI's `linstor rd sync-status` and the snapshot-shipping pre-check
// both gate on this key. A flip to camelCase `syncedOnAll` decodes as
// false uniformly and silently breaks every gated workflow.
func TestWireKeysSyncStatus(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd-wk"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "rd-wk", NodeName: "n1",
		State: apiv1.ResourceState{DrbdState: "UpToDate"},
	}); err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/rd-wk/sync-status")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got map[string]any

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	if _, ok := got["synced_on_all"]; !ok {
		t.Errorf("sync-status response missing canonical wire key `synced_on_all`; body=%v", got)
	}

	if _, ok := got["syncedOnAll"]; ok {
		t.Errorf("sync-status response emits camelCase `syncedOnAll`; upstream uses snake_case; body=%v", got)
	}
}

// TestWireKeysNodeConfig witnesses the upstream
// `JsonGenTypes.SatelliteConfig` wire shape for
// `GET /v1/nodes/{node}/config` (item #228). The top-level shape
// surfaces `net` / `log` / `special_satellite`; the nested `net` block
// uses `bind_address`, `port`, `com_type`. python-linstor's
// `linstor node config` renders these column-by-column; any drift
// makes the output blank.
func TestWireKeysNodeConfig(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n-wk", Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.5", SatellitePort: 3366, SatelliteEncryptionType: "PLAIN"},
		},
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/nodes/n-wk/config")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got map[string]any

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	netBlock, ok := got["net"].(map[string]any)
	if !ok {
		t.Fatalf("response missing canonical `net` block (upstream SatelliteConfig); body=%v", got)
	}

	for _, key := range []string{"bind_address", "port", "com_type"} {
		if _, ok := netBlock[key]; !ok {
			t.Errorf("net block missing canonical wire key %q; body=%v", key, netBlock)
		}
	}

	for _, key := range []string{"bindAddress", "comType"} {
		if _, ok := netBlock[key]; ok {
			t.Errorf("net block emits camelCase key %q; upstream uses snake_case; body=%v", key, netBlock)
		}
	}
}

// TestWireKeysSPDefinitionSingle witnesses the upstream wire shape of
// the per-name `GET /v1/storage-pool-definitions/{name}` endpoint
// (item #229). Upstream returns a list-shaped envelope filtered to
// the requested name; each element exposes `storage_pool_name` plus
// the optional `props` map. The python CLI iterates the list and
// formats per-element rows; a flip to camelCase `storagePoolName`
// makes the CLI render every row blank.
func TestWireKeysSPDefinitionSingle(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "main-wk", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindLVM,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/storage-pool-definitions/main-wk")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []map[string]any

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	if len(got) == 0 {
		t.Fatalf("response: empty list (seed leaked); body=%v", got)
	}

	if _, ok := got[0]["storage_pool_name"]; !ok {
		t.Errorf("SPD element missing canonical wire key `storage_pool_name`; body=%v", got[0])
	}

	if _, ok := got[0]["storagePoolName"]; ok {
		t.Errorf("SPD element emits camelCase `storagePoolName`; upstream uses snake_case; body=%v", got[0])
	}
}
