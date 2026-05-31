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
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestBughuntP3_Finding3_ResourceGetVolumesAlwaysSlice pins that
// `GET /v1/resource-definitions/{rd}/resources/{node}` and the list
// sibling emit `"volumes":[]` instead of `"volumes":null` when the
// satellite hasn't written Status.Volumes yet. Bug-hunt v0.1.3
// Finding 3: pre-fix the wrapper `ResourceWithVolumes{Resource: r}`
// left the outer slice nil and `encoding/json` serialised it as
// `null`, breaking python-linstor's `rsc._rest_data['volumes']`
// dereference (same crash class as the historical KeyError:'size').
// Per Bug 137 contract the wire key is always present (even as
// `[]`); a fresh diskful replica with no observed Volumes is the
// most common trigger.
func TestBughuntP3_Finding3_ResourceGetVolumesAlwaysSlice(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// Fresh diskful replica — no Volumes populated by the satellite
	// observer yet. This is the wire shape the bug report captured.
	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "rd-1", NodeName: "n1"}); err != nil {
		t.Fatalf("seed res: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Per-node GET path.
	getResp := httpGet(t, base+"/v1/resource-definitions/rd-1/resources/n1")
	defer func() { _ = getResp.Body.Close() }()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("get status: got %d, want 200", getResp.StatusCode)
	}

	getBytes, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("get read: %v", err)
	}

	if !bytes.Contains(getBytes, []byte(`"volumes":[]`)) {
		t.Errorf("get response missing `volumes:[]`; got: %s", getBytes)
	}

	if bytes.Contains(getBytes, []byte(`"volumes":null`)) {
		t.Errorf("get response leaked `volumes:null`: %s", getBytes)
	}

	// Per-RD list path.
	listResp := httpGet(t, base+"/v1/resource-definitions/rd-1/resources")
	defer func() { _ = listResp.Body.Close() }()

	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("list status: got %d, want 200", listResp.StatusCode)
	}

	listBytes, err := io.ReadAll(listResp.Body)
	if err != nil {
		t.Fatalf("list read: %v", err)
	}

	if !bytes.Contains(listBytes, []byte(`"volumes":[]`)) {
		t.Errorf("list response missing `volumes:[]`; got: %s", listBytes)
	}

	if bytes.Contains(listBytes, []byte(`"volumes":null`)) {
		t.Errorf("list response leaked `volumes:null`: %s", listBytes)
	}
}

// TestBughuntP3_Finding9_NetInterfaceDeleteMissingWarnBand pins the
// warn-band envelope on `DELETE /v1/nodes/{node}/net-interfaces/{name}`
// when the named interface is absent. Bug-hunt v0.1.3 Finding 9:
// pre-fix the handler returned maskInfo + "net-interface deleted: foo"
// (indistinguishable from a real drop in audit logs). Post-fix the
// envelope carries warnNetInterfaceNotFound + "net-interface already
// absent: foo" so audit-log greppers + Prometheus alerting can
// distinguish a real teardown from an idempotent replay.
func TestBughuntP3_Finding9_NetInterfaceDeleteMissingWarnBand(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.1"},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/n1/net-interfaces/never-existed")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(rc) != 1 {
		t.Fatalf("envelope len: got %d, want 1", len(rc))
	}

	if rc[0].RetCode != warnNetInterfaceNotFound {
		t.Errorf("ret_code: got %d, want %d (warnNetInterfaceNotFound)", rc[0].RetCode, warnNetInterfaceNotFound)
	}

	if !strings.Contains(rc[0].Message, "already absent") {
		t.Errorf("message: got %q, want 'already absent' substring", rc[0].Message)
	}

	// Pin that a real-delete still surfaces maskInfo (no regression).
	realResp := httpDelete(t, base+"/v1/nodes/n1/net-interfaces/default")
	defer func() { _ = realResp.Body.Close() }()

	var realRC []apiv1.APICallRc
	if err := json.NewDecoder(realResp.Body).Decode(&realRC); err != nil {
		t.Fatalf("real-delete decode: %v", err)
	}

	if realRC[0].RetCode != maskInfo {
		t.Errorf("real-delete ret_code: got %d, want maskInfo (%d)", realRC[0].RetCode, maskInfo)
	}
}

// TestBughuntP3_Finding9_NodeLostMissingWarnBand pins the warn-band
// envelope on `DELETE /v1/nodes/{ghost}/lost` (and the POST sibling
// — both route through handleNodeLost). Bug-hunt v0.1.3 Finding 9:
// pre-fix the handler folded NotFound into maskInfo + "node lost:
// ghost" so the wire could not distinguish a real cascade from an
// idempotent replay. Post-fix it carries warnNodeAlreadyGone.
func TestBughuntP3_Finding9_NodeLostMissingWarnBand(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/ghost/lost", nil)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (idempotent fold)", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(rc) != 1 {
		t.Fatalf("envelope len: got %d, want 1", len(rc))
	}

	if rc[0].RetCode != warnNodeAlreadyGone {
		t.Errorf("ret_code: got %d, want %d (warnNodeAlreadyGone)", rc[0].RetCode, warnNodeAlreadyGone)
	}

	if !strings.Contains(rc[0].Message, "already absent") {
		t.Errorf("message: got %q, want 'already absent' substring", rc[0].Message)
	}
}

// TestBughuntP3_Finding10_QueryMaxVolumeSizeGlobalRoute pins the
// top-level OPTIONS /v1/query-max-volume-size endpoint. Bug-hunt
// v0.1.3 Finding 10: upstream LINSTOR OpenAPI registers this for
// the RG-less `linstor query-max-volume-size --place 2 --storage-
// pool <pool>` CLI form; pre-fix the apiserver only wired the RG-
// scoped path so the top-level form 404'd. The endpoint takes an
// AutoSelectFilter body and returns the same `candidates` envelope
// as the RG-scoped variant.
//
// We exercise both the body-bearing form and the no-body fallback
// (an operator running `curl -X OPTIONS .../v1/query-max-volume-
// size` with no JSON must still get a non-empty candidate list
// instead of 400).
func TestBughuntP3_Finding10_QueryMaxVolumeSizeGlobalRoute(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Seed a pool so the candidate list is non-empty — confirms the
	// handler is wired end-to-end through computeSizeInfo.
	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        "n1",
		StoragePoolName: "zfs-thin",
		ProviderKind:    apiv1.StoragePoolKindZFSThin,
		FreeCapacity:    1024 * 1024,
		TotalCapacity:   2048 * 1024,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Body-bearing form: filter pinned to the seeded pool.
	body, _ := json.Marshal(apiv1.AutoSelectFilter{
		PlaceCount:  1,
		StoragePool: "zfs-thin",
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodOptions, base+"/v1/query-max-volume-size", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new req: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (top-level OPTIONS wired)", resp.StatusCode)
	}

	var got struct {
		Candidates []struct {
			StoragePool string `json:"storage_pool"`
		} `json:"candidates"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got.Candidates) == 0 {
		t.Errorf("candidates empty; want at least one (filter pinned zfs-thin)")
	}

	for _, c := range got.Candidates {
		if c.StoragePool != "zfs-thin" {
			t.Errorf("candidate pool: got %q, want zfs-thin (filter constraint ignored)", c.StoragePool)
		}
	}

	// No-body form: must NOT 400.
	emptyReq, err := http.NewRequestWithContext(ctx, http.MethodOptions, base+"/v1/query-max-volume-size", nil)
	if err != nil {
		t.Fatalf("new empty req: %v", err)
	}

	emptyResp, err := http.DefaultClient.Do(emptyReq)
	if err != nil {
		t.Fatalf("do empty: %v", err)
	}
	defer func() { _ = emptyResp.Body.Close() }()

	if emptyResp.StatusCode != http.StatusOK {
		t.Errorf("empty-body status: got %d, want 200 (no-body fallback)", emptyResp.StatusCode)
	}
}

// TestBughuntP3_Finding13_StatsNodes pins the /v1/stats/nodes
// endpoint. Bug-hunt v0.1.3 Finding 13: pre-fix this returned 404
// while the six sibling /v1/stats/<kind> sub-paths were wired. The
// shape mirrors upstream's NodeStats schema: `count` total plus
// `online` / `offline` / `evicted` sub-counts — `linstor node
// stats` reads each key unconditionally.
func TestBughuntP3_Finding13_StatsNodes(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Three nodes: one ONLINE, one OFFLINE, one ONLINE+EVICTED.
	for _, n := range []apiv1.Node{
		{Name: "online-1", ConnectionStatus: "ONLINE"},
		{Name: "offline-1", ConnectionStatus: "OFFLINE"},
		{Name: "evicted-online", ConnectionStatus: "ONLINE", Flags: []string{apiv1.NodeFlagEvicted}},
	} {
		if err := st.Nodes().Create(ctx, &n); err != nil {
			t.Fatalf("seed %s: %v", n.Name, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/stats/nodes")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (no longer 404)", resp.StatusCode)
	}

	var got nodeStatsEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Count != 3 {
		t.Errorf("count: got %d, want 3", got.Count)
	}

	if got.Online != 2 {
		t.Errorf("online: got %d, want 2 (online-1 + evicted-online)", got.Online)
	}

	if got.Offline != 1 {
		t.Errorf("offline: got %d, want 1 (offline-1)", got.Offline)
	}

	if got.Evicted != 1 {
		t.Errorf("evicted: got %d, want 1 (evicted-online)", got.Evicted)
	}
}

// TestBughuntP3_Finding14_RDExternalFilesEmptyList pins the per-RD
// external-files attach surface returning an empty `[]`. Bug-hunt
// v0.1.3 Finding 14: pre-fix `GET /v1/resource-definitions/{rd}/
// files` returned 404, crashing the CLI's `linstor rd list-files`
// JSON decode. blockstor doesn't support arbitrary external files
// (operators ship custom DRBD opts via `Aux/` props per the report's
// note), so the empty list is the upstream-shaped truth.
func TestBughuntP3_Finding14_RDExternalFilesEmptyList(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/rd-x/files")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (no longer 404)", resp.StatusCode)
	}

	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	trimmed := strings.TrimSpace(string(got))
	if trimmed != "[]" {
		t.Errorf("body: got %q, want %q", trimmed, "[]")
	}
}

// TestBughuntP3_Finding15_ControllerConfigLogLevel pins that
// GET /v1/controller/config carries the live runtime log level and
// http.enabled. Bug-hunt v0.1.3 Finding 15: pre-fix the handler
// returned bare `{}`, so any client reading `cfg.log.level` to
// determine the current logger state (vs. PUTting + diffing) had no
// signal. Post-fix log.level mirrors the runtimeLogLevel LevelVar.
//
// We flip the level via the existing PUT /v1/controller/config
// surface (Bug 159) and re-GET — the round-trip must be the
// identity for any value `set-log-level` accepts.
func TestBughuntP3_Finding15_ControllerConfigLogLevel(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	// Flip the level via the upstream-shaped PUT.
	putBody, _ := json.Marshal(map[string]any{
		"log": map[string]string{"level": "DEBUG"},
	})

	putResp := httpPut(t, base+"/v1/controller/config", putBody)
	_ = putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", putResp.StatusCode)
	}

	// Re-GET and verify log.level == DEBUG + http.enabled == true.
	getResp := httpGet(t, base+"/v1/controller/config")
	defer func() { _ = getResp.Body.Close() }()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: got %d, want 200", getResp.StatusCode)
	}

	var got struct {
		Log struct {
			Level string `json:"level"`
		} `json:"log"`
		HTTP struct {
			Enabled bool `json:"enabled"`
		} `json:"http"`
	}

	if err := json.NewDecoder(getResp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Log.Level != "DEBUG" {
		t.Errorf("log.level: got %q, want DEBUG (PUT round-trip identity)", got.Log.Level)
	}

	if !got.HTTP.Enabled {
		t.Errorf("http.enabled: got false, want true (apiserver wired)")
	}
}
