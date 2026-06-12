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
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug-020: POST /v1/resource-definitions/{rd}/clone 400-rejected
// golinstor v0.58+'s `use_zfs_clone` field — the rdCloneRequest
// struct lacked it and the server decodes with
// DisallowUnknownFields. linstor-csi sends `UseZfsClone` on every
// CSI clone-from-source (`CreateVolume` with
// `VolumeContentSource_Volume`), so CSI volume cloning failed with
// `unknown field "use_zfs_clone" in request body`.
//
// The fix accepts the field AND materialises a real clone for
// VD-bearing sources: an internal snapshot `clone-<target>` of the
// source backs a snapshot-restore of the target RD, whose
// `BlockstorRestoreFromSnapshot` marker routes the satellite's
// storage provider to RestoreVolumeFromSnapshot — `zfs clone` on
// ZFS, which is exactly the data plane `use_zfs_clone=true`
// requests.
//
// These tests pin:
//
//  1. the wire surface accepts `use_zfs_clone` (was: 400);
//  2. a deployed VD-bearing source clones for real — target RD
//     persisted with hydrated VDs + restore marker, internal
//     snapshot recorded, CloneStatus answers COMPLETE;
//  3. the clone POST is idempotent for linstor-csi's retry loop;
//  4. a name collision with a non-clone RD answers 409 in the
//     CloneStarted object shape (python-linstor decodes the clone
//     response body into CloneStarted unconditionally);
//  5. Bug 232 `override_props` / `delete_props` apply on the
//     data-plane path too.

// seedDeployedCloneSource stamps a snapshot-capable, deployed,
// VD-bearing source RD: 1 VD, one diskful replica on an ONLINE node
// whose pool reports SupportsSnapshot=true.
func seedDeployedCloneSource(t *testing.T, st store.Store, rdName string) {
	t.Helper()

	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name:             "node-a",
		ConnectionStatus: "ONLINE",
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName:  "zfs-thin",
		NodeName:         "node-a",
		ProviderKind:     "ZFS_THIN",
		SupportsSnapshot: true,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  rdName,
		Props: map[string]string{"Aux/origin": "bug020-src"},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, rdName, &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      64 * 1024,
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     rdName,
		NodeName: "node-a",
		Props:    map[string]string{"StorPoolName": "zfs-thin"},
	}); err != nil {
		t.Fatalf("seed replica: %v", err)
	}
}

// postClone POSTs the clone body and returns the response.
func postClone(t *testing.T, base, src string, body map[string]any) *http.Response {
	t.Helper()

	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal clone body: %v", err)
	}

	return httpPost(t, base+"/v1/resource-definitions/"+src+"/clone", raw)
}

// decodeCloneStarted decodes the CloneStarted object envelope.
func decodeCloneStarted(t *testing.T, resp *http.Response) cloneStartedResponse {
	t.Helper()

	var started cloneStartedResponse
	if err := json.NewDecoder(resp.Body).Decode(&started); err != nil {
		t.Fatalf("decode CloneStarted envelope: %v", err)
	}

	return started
}

// TestBug020CloneAcceptsUseZfsCloneAndMaterialises is the core
// regression pin: the exact body linstor-csi puts on the wire
// (`{"name": ..., "use_zfs_clone": true}`) must answer 201 and leave
// behind a structurally complete clone.
func TestBug020CloneAcceptsUseZfsCloneAndMaterialises(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src020")

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src020", map[string]any{
		"name":          "dst020",
		"use_zfs_clone": true,
	})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("clone with use_zfs_clone: status got %d, want 201 "+
			"(pre-fix: 400 unknown field)", resp.StatusCode)
	}

	started := decodeCloneStarted(t, resp)
	if started.SourceName != "src020" || started.CloneName != "dst020" {
		t.Errorf("CloneStarted: got source=%q clone=%q", started.SourceName, started.CloneName)
	}

	if !strings.Contains(started.Location, "/clone/dst020") {
		t.Errorf("CloneStarted.Location: got %q, want clone-status path", started.Location)
	}

	assertCloneMaterialised(ctx, t, st)
	assertCloneStatusComplete(t, base)
}

// assertCloneMaterialised checks the post-clone store state: target
// RD with the restore marker, hydrated VDs, and the internal
// snapshot the satellite's RestoreVolumeFromSnapshot (`zfs clone`)
// consumes.
func assertCloneMaterialised(ctx context.Context, t *testing.T, st store.Store) {
	t.Helper()

	dst, err := st.ResourceDefinitions().Get(ctx, "dst020")
	if err != nil {
		t.Fatalf("target RD not persisted: %v", err)
	}

	if got := dst.Props["BlockstorRestoreFromSnapshot"]; got != "src020:clone-dst020" {
		t.Errorf("restore marker: got %q, want %q (routes the satellite to "+
			"RestoreVolumeFromSnapshot instead of a blank CreateVolume)",
			got, "src020:clone-dst020")
	}

	vds, err := st.VolumeDefinitions().List(ctx, "dst020")
	if err != nil {
		t.Fatalf("list target VDs: %v", err)
	}

	if len(vds) != 1 || vds[0].SizeKib != 64*1024 {
		t.Errorf("target VDs: got %+v, want one 64MiB VD copied from the source", vds)
	}

	snap, err := st.Snapshots().Get(ctx, "src020", "clone-dst020")
	if err != nil {
		t.Fatalf("internal clone snapshot not recorded: %v", err)
	}

	if len(snap.Nodes) != 1 || snap.Nodes[0] != "node-a" {
		t.Errorf("snapshot nodes: got %v, want the source's diskful node set", snap.Nodes)
	}
}

// assertCloneStatusComplete pins the golinstor CloneStatus poll:
// equal source/target VD counts must answer COMPLETE so linstor-csi
// stops waiting.
func assertCloneStatusComplete(t *testing.T, base string) {
	t.Helper()

	statusResp := httpGet(t, base+"/v1/resource-definitions/src020/clone/dst020")
	defer func() { _ = statusResp.Body.Close() }()

	if statusResp.StatusCode != http.StatusOK {
		t.Fatalf("clone status: got %d, want 200", statusResp.StatusCode)
	}

	var status struct {
		Status string `json:"status"`
	}

	if err := json.NewDecoder(statusResp.Body).Decode(&status); err != nil {
		t.Fatalf("decode clone status: %v", err)
	}

	if !strings.EqualFold(status.Status, "COMPLETE") {
		t.Errorf("clone status: got %q, want COMPLETE", status.Status)
	}
}

// TestBug020CloneIdempotentRetry pins the linstor-csi retry
// contract: a replayed clone POST whose target already materialised
// from THIS source answers 201 again instead of a conflict, so the
// driver's CreateVolume replay loop converges.
func TestBug020CloneIdempotentRetry(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src020")

	base, stop := startServerWithStore(t, st)
	defer stop()

	first := postClone(t, base, "src020", map[string]any{"name": "dst020", "use_zfs_clone": true})
	_ = first.Body.Close()

	if first.StatusCode != http.StatusCreated {
		t.Fatalf("first clone: status got %d, want 201", first.StatusCode)
	}

	retry := postClone(t, base, "src020", map[string]any{"name": "dst020", "use_zfs_clone": true})
	defer func() { _ = retry.Body.Close() }()

	if retry.StatusCode != http.StatusCreated {
		t.Fatalf("replayed clone: status got %d, want idempotent 201", retry.StatusCode)
	}

	started := decodeCloneStarted(t, retry)
	if started.CloneName != "dst020" {
		t.Errorf("replayed CloneStarted.CloneName: got %q, want dst020", started.CloneName)
	}
}

// TestBug020CloneTargetNameCollisionRefusedInCloneShape: a
// pre-existing RD under the requested clone name that is NOT a clone
// of this source must answer 409 — and the body must keep the
// CloneStarted OBJECT shape (python-linstor decodes it
// unconditionally; a bare []ApiCallRc array crashes the CLI).
func TestBug020CloneTargetNameCollisionRefusedInCloneShape(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src020")

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "dst020",
	}); err != nil {
		t.Fatalf("seed colliding RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src020", map[string]any{"name": "dst020", "use_zfs_clone": true})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("colliding clone target: status got %d, want 409", resp.StatusCode)
	}

	started := decodeCloneStarted(t, resp)
	if started.Messages == nil || len(*started.Messages) == 0 {
		t.Fatalf("refusal must carry messages[] inside the CloneStarted object")
	}

	if msg := (*started.Messages)[0].Message; !strings.Contains(msg, "already exists") {
		t.Errorf("refusal message: got %q, want an already-exists explanation", msg)
	}
}

// TestBug020ClonePropEditsApply pins Bug 232 parity on the
// data-plane path: `override_props` land on the cloned RD and
// `delete_props` drop inherited source keys.
func TestBug020ClonePropEditsApply(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src020")

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src020", map[string]any{
		"name":           "dst020",
		"use_zfs_clone":  true,
		"override_props": map[string]string{"Aux/team": "storage"},
		"delete_props":   []string{"Aux/origin"},
	})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("clone with prop edits: status got %d, want 201", resp.StatusCode)
	}

	dst, err := st.ResourceDefinitions().Get(ctx, "dst020")
	if err != nil {
		t.Fatalf("target RD not persisted: %v", err)
	}

	if dst.Props["Aux/team"] != "storage" {
		t.Errorf("override_props not applied: %v", dst.Props)
	}

	if _, leaked := dst.Props["Aux/origin"]; leaked {
		t.Errorf("delete_props not applied; inherited key survived: %v", dst.Props)
	}
}
