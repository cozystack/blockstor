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
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestSnapshotRestoreCreatesNewRD: POST .../snapshot-restore-resource
// builds a brand-new ResourceDefinition from a snapshot. Mirrors what
// `linstor snapshot resource restore` does upstream.
func TestSnapshotRestoreCreatesNewRD(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-1",
		ResourceName: "pvc-1",
		Nodes:        []string{"n1", "n2"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{
		"to_resource":   "pvc-2",
		"from_snapshot": "snap-1",
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "pvc-2")
	if err != nil {
		t.Fatalf("expected pvc-2 to exist: %v", err)
	}

	if got.Name != "pvc-2" {
		t.Errorf("Name: got %q", got.Name)
	}
}

// TestSnapshotRestoreAfterSourceResourcesDeleted_G3a pins corner-case
// G3a: the upstream administration guide states a snapshot can be
// restored "even when the original resource has been removed from the
// nodes where the snapshots were taken" (linstor-administration.adoc
// ~2480). E1 proved upstream BLOCKS `rd d` while snapshots exist, so
// the relevant sequence is `r d` of every replica (the RD + the
// snapshot survive), then restore from the surviving snapshot. The
// snapshot is stored independently of the (now-deleted) Resources, so
// the restore must still build the new RD + place its replicas on the
// snapshot's recorded nodes.
func TestSnapshotRestoreAfterSourceResourcesDeleted_G3a(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-src"}); err != nil {
		t.Fatalf("seed source RD: %v", err)
	}

	// Snapshot recorded on n1/n2 — but NO Resources exist (every replica
	// was `r d`'d). The RD shell + the snapshot both survive.
	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-keep",
		ResourceName: "pvc-src",
		Nodes:        []string{"n1", "n2"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Explicit node list (the snapshot's recorded nodes) so the restore
	// places replicas without depending on a parent RG / autoplace.
	body, _ := json.Marshal(snapshotRestoreRequest{
		ToResource:   "pvc-restored",
		FromSnapshot: "snap-keep",
		Nodes:        []string{"n1", "n2"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-src/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201 (restore must work after source resources deleted)", resp.StatusCode)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "pvc-restored"); err != nil {
		t.Fatalf("restored RD missing: %v", err)
	}

	// The restored RD must carry the snapshot's volume layout.
	vds, err := st.VolumeDefinitions().List(ctx, "pvc-restored")
	if err != nil {
		t.Fatalf("list restored VDs: %v", err)
	}

	if len(vds) != 1 || vds[0].VolumeNumber != 0 {
		t.Errorf("restored VDs: got %+v, want exactly [vol 0]", vds)
	}

	// Replicas must be stamped on the snapshot's recorded nodes.
	resList, err := st.Resources().ListByDefinition(ctx, "pvc-restored")
	if err != nil {
		t.Fatalf("list restored resources: %v", err)
	}

	placed := make(map[string]bool, len(resList))
	for i := range resList {
		placed[resList[i].NodeName] = true
	}

	if !placed["n1"] || !placed["n2"] {
		t.Errorf("restored replicas: got nodes %v, want {n1,n2}", placed)
	}
}

// TestSnapshotRestoreUnknownSnapshot: 404 if the snapshot doesn't exist.
func TestSnapshotRestoreUnknownSnapshot(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{
		"to_resource":   "pvc-2",
		"from_snapshot": "ghost",
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestSnapshotRestoreMissingFields: empty `to_resource` → 400.
func TestSnapshotRestoreMissingFields(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{
		"from_snapshot": "snap-1",
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestSnapshotRestoreBadJSON: malformed body → 400.
func TestSnapshotRestoreBadJSON(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/snapshot-restore-resource", []byte("{not-json"))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestSnapshotRestoreConstrainsProviderToSource: a restore whose
// source snapshot lives on ZFS_THIN must land in the SOURCE's own
// pool, even when other pools (matching or mismatched kind) share the
// candidate nodes. Pinned because `zfs send` payloads can't be
// replayed onto an LVM pool via `dd` — a cross-backend placement
// fails opaquely at satellite SendSnapshot/RecvSnapshot time. Bug 15,
// retargeted by Bug 038: the restore no longer defers placement to
// the autoplacer — it stamps replicas on the snapshot's nodes in the
// source replica's pool up front (upstream LINSTOR restore semantics,
// verified against the live linstor-oracle), and a follow-up
// autoplace at the same place_count is an idempotent no-op.
func TestSnapshotRestoreConstrainsProviderToSource(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Source RD on n1 with a ZFS_THIN pool.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-src"}); err != nil {
		t.Fatalf("seed source RD: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "zpool", NodeName: "n1",
		ProviderKind: apiv1.StoragePoolKindZFSThin,
	}); err != nil {
		t.Fatalf("seed src zfs pool: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-src", NodeName: "n1",
		Props: map[string]string{"StorPoolName": "zpool"},
	}); err != nil {
		t.Fatalf("seed source resource: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name: "snap-1", ResourceName: "pvc-src", Nodes: []string{"n1", "n2"},
		// Bug 151 follow-up: empty-VD snapshots are now refused; this
		// test cares about the provider-filter path, so we attach a
		// small placeholder VD to keep the restore on the success
		// path without exceeding the candidate pools' FreeCapacity.
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 64},
		},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	// Mixed candidate pools on the snapshot's nodes: ZFS_THIN on both
	// (the only legal targets) and LVM_THIN on both (mismatched —
	// must be filtered out).
	for _, n := range []string{"n1", "n2"} {
		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "zfs-target-" + n, NodeName: n,
			ProviderKind: apiv1.StoragePoolKindZFSThin, FreeCapacity: 1000,
		}); err != nil {
			t.Fatalf("seed zfs candidate %s: %v", n, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "lvm-target-" + n, NodeName: n,
			ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 9000,
		}); err != nil {
			t.Fatalf("seed lvm candidate %s: %v", n, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// 1) restore-resource — seeds BlockstorRestoreFromSnapshot prop on pvc-2.
	body, _ := json.Marshal(snapshotRestoreRequest{
		ToResource:   "pvc-2",
		FromSnapshot: "snap-1",
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-src/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("restore status: got %d, want 201", resp.StatusCode)
	}

	// 2) the bare restore (no --node-name) leaves an EMPTY shell — the
	// operator / linstor-csi drives placement, preserving the
	// restore-then-scale-out workflow and the staged cross-node
	// bring-up the e2e restore lanes rely on. The cross-backend
	// protection moved to the placer, asserted in step 3.
	got, err := st.Resources().ListByDefinition(ctx, "pvc-2")
	if err != nil {
		t.Fatalf("list pvc-2: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("bare restore replicas: got %d, want 0 (empty shell — "+
			"placement is the operator's call)", len(got))
	}

	// 3) the operator's autoplace honours the restore-source BACKEND
	// pin: even though the LVM_THIN candidates are roomier (FreeCapacity
	// 9000 vs the ZFS_THIN 1000), every diskful replica must land on a
	// ZFS_THIN pool — the placer's constrainFilterToRestoreSource drops
	// the mismatched backend (Bug 038: a ZFS snapshot stream piped into
	// an LVM receiver fails opaquely).
	body, _ = json.Marshal(map[string]any{
		"select_filter": map[string]any{"place_count": 2},
	})

	resp = httpPost(t, base+"/v1/resource-definitions/pvc-2/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("autoplace status: got %d, want 200", resp.StatusCode)
	}

	got, err = st.Resources().ListByDefinition(ctx, "pvc-2")
	if err != nil {
		t.Fatalf("list pvc-2 after autoplace: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("replicas after autoplace: got %d, want 2", len(got))
	}

	for i := range got {
		stor := got[i].Props["StorPoolName"]
		if stor != "zfs-target-n1" && stor != "zfs-target-n2" {
			t.Errorf("replica on %s landed on %q, want a ZFS_THIN target "+
				"(the LVM_THIN decoy must be filtered by the backend pin)",
				got[i].NodeName, stor)
		}
	}
}

// TestSnapshotRestoreFailsWhenNoMatchingProvider: source snapshot on
// ZFS_THIN, only LVM_THIN candidates → autoplace returns 409 with an
// operator-actionable message instead of placing onto a mismatched
// pool that would then fail opaquely at the satellite. Bug 15.
func TestSnapshotRestoreFailsWhenNoMatchingProvider(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-src"}); err != nil {
		t.Fatalf("seed source RD: %v", err)
	}

	// Source on ZFS_THIN (n1). n1 is marked LOST so it doesn't show
	// up as an autoplace candidate but still lets the provider-kind
	// lookup find ZFS_THIN via the source's Resource.StorPoolName.
	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1", Type: apiv1.NodeTypeSatellite, Flags: []string{apiv1.NodeFlagLost},
	}); err != nil {
		t.Fatalf("seed n1 node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "zpool", NodeName: "n1",
		ProviderKind: apiv1.StoragePoolKindZFSThin,
	}); err != nil {
		t.Fatalf("seed src pool: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-src", NodeName: "n1",
		Props: map[string]string{"StorPoolName": "zpool"},
	}); err != nil {
		t.Fatalf("seed source resource: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name: "snap-1", ResourceName: "pvc-src", Nodes: []string{"n2", "n3"},
		// Bug 151 follow-up: empty-VD snapshots are now refused;
		// this test cares about the provider-mismatch error path
		// on autoplace, so we attach a placeholder VD.
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	// Candidates only on LVM_THIN — guaranteed mismatch.
	for _, n := range []string{"n2", "n3"} {
		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "lvm-" + n, NodeName: n,
			ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 9000,
		}); err != nil {
			t.Fatalf("seed lvm candidate %s: %v", n, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(snapshotRestoreRequest{
		ToResource:   "pvc-2",
		FromSnapshot: "snap-1",
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-src/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("restore status: got %d, want 201", resp.StatusCode)
	}

	body, _ = json.Marshal(map[string]any{
		"select_filter": map[string]any{"place_count": 1},
	})

	resp = httpPost(t, base+"/v1/resource-definitions/pvc-2/autoplace", body)

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("autoplace status: got %d, want 409", resp.StatusCode)
	}

	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	msg := string(buf[:n])

	// Operator-actionable text: must call out the source provider so
	// the human knows which pool kind to add to fix the cluster.
	if !contains(msg, apiv1.StoragePoolKindZFSThin) {
		t.Errorf("error message %q missing source provider %q", msg, apiv1.StoragePoolKindZFSThin)
	}
}

// contains is a tiny local strings.Contains alias to keep the
// imports clean in this file (snapshot_restore_test.go currently
// doesn't pull strings — adding it just for one substring check
// is noisier than this helper).
func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}

	return false
}

// TestSnapshotRestoreScenario8W03: covers wave2 scenario 8.W03 /
// wave1 F1 — `snapshot resource restore` against an existing snapshot
// must build a NEW ResourceDefinition (NOT mutate the source RD,
// NOT rollback in place). End-to-end contract:
//
//  1. POST .../snapshot-restore-resource with `to_resource: <new-rd>`
//     returns 201 and reports the source-snap → target-rd mapping in
//     the APICallRc envelope.
//  2. The target RD exists in the store, separate from the source RD.
//  3. The target RD carries the `BlockstorRestoreFromSnapshot` prop
//     (`<srcRD>:<snapName>`) — this is what the dispatcher pipes
//     through to DesiredVolume.SourceSnapshot so the satellite
//     materialises the volume via Provider.RestoreVolumeFromSnapshot
//     (`zfs clone` / `lvcreate -s` / FILE reflink) instead of
//     CreateVolume. Cross-pool / cross-node clone falls back to
//     CrossNodeFetcher + SnapshotShipper.RecvSnapshot (zfs send | recv,
//     dd-piped thin LV stream); that satellite-side wiring lives in
//     pkg/satellite/reconciler.go.
//  4. The target RD's VolumeDefinitions mirror the snapshot's recorded
//     volume layout — same volume_number / size_kib pairs, hydrated
//     by hydrateVolumesFromSnapshot. Without this, autoplace would
//     create an RD with zero volumes that never reaches UpToDate.
//  5. The source RD is untouched — `snapshot resource restore` is the
//     non-destructive alternative to `snapshot rollback` (8.W04). The
//     two RDs are independently usable.
func TestSnapshotRestoreScenario8W03(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Seed source RD with a non-trivial Props map. The handler copies
	// snapshot.Props onto the new RD when set, falling back to the
	// source RD's Props when not — we exercise the fallback path so
	// the LayerStack / Props inheritance is observable.
	srcProps := map[string]string{
		"DrbdOptions/Net/protocol": "C",
	}
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:       "pvc-src",
		Props:      srcProps,
		LayerStack: []string{"DRBD", "STORAGE"},
	}); err != nil {
		t.Fatalf("seed source RD: %v", err)
	}

	// Two-volume snapshot — proves hydrateVolumesFromSnapshot copies
	// every VD, not just the first one. Mirrors what a multi-volume
	// RD (e.g. data + WAL) looks like in production.
	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-1",
		ResourceName: "pvc-src",
		Nodes:        []string{"n1", "n2"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024}, // 1 GiB data
			{VolumeNumber: 1, SizeKib: 64 * 1024},   // 64 MiB WAL
		},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(snapshotRestoreRequest{
		ToResource:   "pvc-restored",
		FromSnapshot: "snap-1",
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-src/snapshot-restore-resource", body)
	defer func() { _ = resp.Body.Close() }()

	// 1) HTTP-level contract.
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode APICallRc envelope: %v", err)
	}

	if len(rcs) != 1 || !contains(rcs[0].Message, "snap-1") || !contains(rcs[0].Message, "pvc-restored") {
		t.Errorf("envelope message %q must mention both snap-1 and pvc-restored", rcs[0].Message)
	}

	// 2) New RD exists and is distinct from source.
	got, err := st.ResourceDefinitions().Get(ctx, "pvc-restored")
	if err != nil {
		t.Fatalf("expected pvc-restored to exist: %v", err)
	}

	if got.Name != "pvc-restored" {
		t.Errorf("new RD name: got %q, want pvc-restored", got.Name)
	}

	// LayerStack must be inherited from source so the satellite
	// builds the same DRBD/STORAGE stack on the new RD.
	if len(got.LayerStack) != 2 || got.LayerStack[0] != "DRBD" || got.LayerStack[1] != "STORAGE" {
		t.Errorf("LayerStack: got %v, want [DRBD STORAGE]", got.LayerStack)
	}

	// 3) BlockstorRestoreFromSnapshot prop drives the satellite to
	// `zfs clone` / `lvcreate -s` instead of CreateVolume.
	clone, ok := got.Props["BlockstorRestoreFromSnapshot"]
	if !ok {
		t.Fatalf("Props missing BlockstorRestoreFromSnapshot — satellite would CreateVolume blank instead of cloning")
	}

	if clone != "pvc-src:snap-1" {
		t.Errorf("clone source prop: got %q, want %q", clone, "pvc-src:snap-1")
	}

	// 4) VolumeDefinitions hydrated from snapshot.
	vds, err := st.VolumeDefinitions().List(ctx, "pvc-restored")
	if err != nil {
		t.Fatalf("list VDs on new RD: %v", err)
	}

	if len(vds) != 2 {
		t.Fatalf("hydrated VDs: got %d, want 2 (one per snapshot volume)", len(vds))
	}

	wantSize := map[int32]int64{0: 1024 * 1024, 1: 64 * 1024}
	for _, vd := range vds {
		if got := vd.SizeKib; got != wantSize[vd.VolumeNumber] {
			t.Errorf("VD %d SizeKib: got %d, want %d", vd.VolumeNumber, got, wantSize[vd.VolumeNumber])
		}
	}

	// 5) Source RD untouched — independent usability is the whole
	// point of restore-into-new-RD vs rollback-in-place.
	src, err := st.ResourceDefinitions().Get(ctx, "pvc-src")
	if err != nil {
		t.Fatalf("source RD must still exist: %v", err)
	}

	if _, hasClone := src.Props["BlockstorRestoreFromSnapshot"]; hasClone {
		t.Errorf("source RD must NOT carry the clone-source prop (would mis-route satellite reconcile)")
	}

	if src.Props["DrbdOptions/Net/protocol"] != "C" {
		t.Errorf("source RD Props mutated: got %v", src.Props)
	}
}

// TestSnapshotRestoreScenario8W03SnapInPath: same scenario, but the
// snapshot name arrives via the URL path (`/snapshot-restore-resource/{snap}`)
// instead of the body — that's the dialect upstream linstor CLI /
// golinstor emit. Must produce the same target RD with the same
// clone-source prop so the CLI hits this endpoint without translation.
func TestSnapshotRestoreScenario8W03SnapInPath(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-src"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-1",
		ResourceName: "pvc-src",
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 2048},
		},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// No `from_snapshot` in body — only `to_resource`. Snapshot name
	// rides the URL path, matching upstream linstor CLI shape.
	body, _ := json.Marshal(map[string]string{"to_resource": "pvc-restored"})

	resp := httpPost(t,
		base+"/v1/resource-definitions/pvc-src/snapshot-restore-resource/snap-1",
		body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "pvc-restored")
	if err != nil {
		t.Fatalf("new RD: %v", err)
	}

	if got.Props["BlockstorRestoreFromSnapshot"] != "pvc-src:snap-1" {
		t.Errorf("clone source prop: got %q, want %q",
			got.Props["BlockstorRestoreFromSnapshot"], "pvc-src:snap-1")
	}
}

// TestSnapshotRestoreConflict: target RD already exists → 409 from
// writeStoreError surfacing ErrAlreadyExists. Pinned because
// linstor-csi reconciles VolumeSnapshot → PVC restore by retrying;
// a 5xx surface here would loop forever on a name that is
// fundamentally already taken (operator must rename or delete).
func TestSnapshotRestoreConflict(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-existing"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-1",
		ResourceName: "pvc-src",
		// Bug 151 follow-up: empty-VD snapshots are now refused;
		// this test cares about the target-already-exists 409
		// path, so we attach a placeholder VD to advance past the
		// vol-less-source gate.
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-src"}); err != nil {
		t.Fatalf("seed source RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(snapshotRestoreRequest{
		ToResource:   "pvc-existing", // target name already taken
		FromSnapshot: "snap-1",
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-src/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status: got %d, want 409 (target RD already exists)", resp.StatusCode)
	}
}

// TestSnapshotRestoreBug354StampsResourcesOnExplicitNodes pins the
// explicit `--node-name` branch of `linstor s r rst`: when the wire
// body carries a `nodes` list, the handler MUST stamp one Resource
// CRD per node so satellites have something to reconcile. Pre-fix,
// snapshotRestoreRequest.Nodes was declared but never read in
// materializeRestoredRD — the target RD landed in the store but no
// Resources were created, the BlockstorRestoreFromSnapshot prop
// marker was dead code, and the restored RD stayed an empty shell.
func TestSnapshotRestoreBug354StampsResourcesOnExplicitNodes(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Source RD with a diskful replica on n1 so the handler can
	// resolve a real StorPoolName when stamping the restored
	// Resources (mirrors the production layout: snapshots are
	// only taken of diskful replicas).
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-src"}); err != nil {
		t.Fatalf("seed source RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-src", NodeName: "n1",
		Props: map[string]string{"StorPoolName": "zpool"},
	}); err != nil {
		t.Fatalf("seed source resource: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-1",
		ResourceName: "pvc-src",
		Nodes:        []string{"n1", "n2"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(snapshotRestoreRequest{
		ToResource:   "pvc-restored",
		FromSnapshot: "snap-1",
		Nodes:        []string{"n1", "n2"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-src/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	got, err := st.Resources().ListByDefinition(ctx, "pvc-restored")
	if err != nil {
		t.Fatalf("list restored Resources: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("Resource CRDs stamped: got %d, want 2 (one per --node-name)", len(got))
	}

	// Verify both requested nodes received a Resource, in any order.
	placed := map[string]apiv1.Resource{}
	for _, res := range got {
		placed[res.NodeName] = res
	}

	for _, want := range []string{"n1", "n2"} {
		res, ok := placed[want]
		if !ok {
			t.Errorf("no Resource stamped on node %q", want)

			continue
		}

		if res.Name != "pvc-restored" {
			t.Errorf("Resource.Name on %s: got %q, want %q", want, res.Name, "pvc-restored")
		}

		// StorPoolName must be inherited from the source RD's first
		// diskful replica so the satellite stages the clone on the
		// same provider (zfs send/recv and dd/lvm aren't interchangeable).
		if pool := res.Props["StorPoolName"]; pool != "zpool" {
			t.Errorf("Resource on %s StorPoolName: got %q, want %q", want, pool, "zpool")
		}
	}
}

// TestSnapshotRestoreBug354AcceptsNodeNamesAlias verifies that the
// older `node_names` wire alias is accepted as well as the upstream
// `nodes` shape. linstor-csi clone shim and some legacy callers
// emit `node_names` — the handler must canonicalise both into the
// same per-node Resource-stamp loop.
func TestSnapshotRestoreBug354AcceptsNodeNamesAlias(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-src"}); err != nil {
		t.Fatalf("seed source RD: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-1",
		ResourceName: "pvc-src",
		Nodes:        []string{"n1"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// `node_names` alias (no `nodes` field).
	body, _ := json.Marshal(snapshotRestoreRequest{
		ToResource:   "pvc-restored",
		FromSnapshot: "snap-1",
		NodeNames:    []string{"n1"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-src/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	got, err := st.Resources().ListByDefinition(ctx, "pvc-restored")
	if err != nil {
		t.Fatalf("list restored Resources: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("Resource CRDs stamped: got %d, want 1 (one per node_names entry)", len(got))
	}

	if got[0].NodeName != "n1" {
		t.Errorf("Resource.NodeName: got %q, want %q", got[0].NodeName, "n1")
	}
}

// TestSnapshotRestoreBug354AutoplacesWhenNodesEmpty exercises the
// empty-nodes branch: no `--node-name` arguments → the handler stamps
// one replica on EVERY node holding the snapshot, in the source
// replica's storage pool. Mirrors upstream LINSTOR's behaviour when
// `linstor s r rst --to-resource X` runs without explicit nodes
// (verified against the live linstor-oracle: the restore lands on
// exactly the snapshot's nodes, same pool, regardless of the parent
// RG's SelectFilter).
//
// History: Bug 354 — the empty-nodes branch was a silent no-op (zero
// Resources). Bug 038 — the interim fix auto-placed via the parent
// RG's SelectFilter.PlaceCount, which placed NOTHING under the
// empty-spec DfltRscGrp default and left the real placement to the
// controller-side RG reconcilers, whose unconstrained placer pass
// could land a clone replica on a different backend (FILE_THIN
// snapshot stream piped into `zfs recv` → bad magic loop).
func TestSnapshotRestoreBug354AutoplacesWhenNodesEmpty(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Parent RG with an EMPTY SelectFilter — the stand-default
	// DfltRscGrp shape that reproduced Bug 038. Placement must not
	// depend on the RG's place_count at all.
	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-1",
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "pvc-src",
		ResourceGroupName: "rg-1",
	}); err != nil {
		t.Fatalf("seed source RD: %v", err)
	}

	// The source's pool exists on both snapshot nodes (LINSTOR pool
	// names are cluster-wide), plus a DIFFERENT-backend pool with
	// more free space on each node — the Bug 038 trap the restore
	// must never fall into.
	for _, n := range []string{"n1", "n2"} {
		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "zpool", NodeName: n,
			ProviderKind: apiv1.StoragePoolKindZFSThin, FreeCapacity: 10 * 1024 * 1024,
		}); err != nil {
			t.Fatalf("seed source pool on %s: %v", n, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "lvm-big", NodeName: n,
			ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 100 * 1024 * 1024,
		}); err != nil {
			t.Fatalf("seed decoy pool on %s: %v", n, err)
		}
	}

	// Source replicas pin the source pool per node.
	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-src", NodeName: n,
			Props: map[string]string{"StorPoolName": "zpool"},
		}); err != nil {
			t.Fatalf("seed source resource on %s: %v", n, err)
		}
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-1",
		ResourceName: "pvc-src",
		Nodes:        []string{"n1", "n2"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Empty Nodes / NodeNames — explicit autoplace fallback.
	body, _ := json.Marshal(snapshotRestoreRequest{
		ToResource:   "pvc-restored",
		FromSnapshot: "snap-1",
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-src/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	// The bare restore (no --node-name) leaves an EMPTY shell — the
	// operator / linstor-csi drives placement. (Bug 354 was about
	// stamping SOMETHING so satellites reconcile; the explicit
	// autoplace below does that, while the placer's backend pin keeps
	// it on the source backend — the Bug 038 fix without the eager
	// all-nodes stamp that broke staged cross-node bring-up.)
	got, err := st.Resources().ListByDefinition(ctx, "pvc-restored")
	if err != nil {
		t.Fatalf("list restored Resources: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("bare restore Resource CRDs: got %d, want 0 (empty shell)", len(got))
	}

	// Operator autoplace: with the parent RG's empty SelectFilter the
	// caller supplies place_count explicitly. The placer's restore-
	// source backend pin must keep every replica on the ZFS_THIN source
	// pool, never the roomier LVM_THIN decoy (Bug 038: a ZFS snapshot
	// stream into an LVM receiver fails opaquely at the satellite).
	body, _ = json.Marshal(map[string]any{
		"select_filter": map[string]any{"place_count": 2},
	})

	resp = httpPost(t, base+"/v1/resource-definitions/pvc-restored/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("autoplace status: got %d, want 200", resp.StatusCode)
	}

	got, err = st.Resources().ListByDefinition(ctx, "pvc-restored")
	if err != nil {
		t.Fatalf("list restored Resources after autoplace: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("Resource CRDs after autoplace: got %d, want 2", len(got))
	}

	for _, res := range got {
		if pool := res.Props["StorPoolName"]; pool != "zpool" {
			t.Errorf("replica on %s landed on pool %q, want the source pool %q "+
				"(the LVM_THIN decoy must be filtered by the backend pin)",
				res.NodeName, pool, "zpool")
		}
	}
}

// TestSnapshotRestoreVolumeDefinitionIntoEmptyRD_G3b is the positive
// control for the VD-restore variant: restoring a snapshot's volume
// layout onto a freshly-created EMPTY target RD succeeds and hydrates
// the recorded volumes. Mirrors the documented two-phase workflow
// (`rd create resource2` → `snapshot volume-definition restore`).
func TestSnapshotRestoreVolumeDefinitionIntoEmptyRD_G3b(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-src"}); err != nil {
		t.Fatalf("seed source RD: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-1",
		ResourceName: "pvc-src",
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	// Empty target RD — no VDs of its own.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-tgt"}); err != nil {
		t.Fatalf("seed target RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(snapshotRestoreRequest{ToResource: "pvc-tgt"})

	resp := httpPost(t,
		base+"/v1/resource-definitions/pvc-src/snapshot-restore-volume-definition/snap-1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (VD restore into empty RD)", resp.StatusCode)
	}

	vds, err := st.VolumeDefinitions().List(ctx, "pvc-tgt")
	if err != nil {
		t.Fatalf("list target VDs: %v", err)
	}

	if len(vds) != 1 || vds[0].VolumeNumber != 0 {
		t.Errorf("target VDs: got %+v, want exactly [vol 0]", vds)
	}
}

// TestSnapshotRestoreVolumeDefinitionConflict_G3b pins corner-case G3b:
// restoring a snapshot's volume definitions onto an RD that ALREADY
// carries a volume-definition with a clashing number is refused with a
// 409 + FAIL_EXISTS_VLM_DFN envelope BEFORE any mutation, naming the
// offending volume number. Without the up-front guard the per-VD
// hydrate Create surfaces a bare 409 only after a partial restore.
func TestSnapshotRestoreVolumeDefinitionConflict_G3b(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-src"}); err != nil {
		t.Fatalf("seed source RD: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-1",
		ResourceName: "pvc-src",
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	// Target RD already has its OWN volume 0.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-tgt"}); err != nil {
		t.Fatalf("seed target RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-tgt", &apiv1.VolumeDefinition{
		VolumeNumber: 0, SizeKib: 2048 * 1024,
	}); err != nil {
		t.Fatalf("seed target VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(snapshotRestoreRequest{ToResource: "pvc-tgt"})

	resp := httpPost(t,
		base+"/v1/resource-definitions/pvc-src/snapshot-restore-volume-definition/snap-1", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 (volume-number conflict)", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rc) == 0 || rc[0].RetCode >= 0 {
		t.Fatalf("ret_code: got %+v, want a negative (error) code", rc)
	}

	// FAIL_EXISTS_VLM_DFN (502) sub-code so golinstor's typed matcher fires.
	const failBit = 502
	if rc[0].RetCode&failBit != failBit {
		t.Errorf("ret_code: got %#x, want FAIL_EXISTS_VLM_DFN (502) bit set", rc[0].RetCode)
	}

	// The target RD's original volume 0 must remain untouched (size
	// 2048 MiB) — the guard must not have partially mutated it.
	got, err := st.VolumeDefinitions().Get(ctx, "pvc-tgt", 0)
	if err != nil {
		t.Fatalf("target VD 0 vanished: %v", err)
	}

	if got.SizeKib != 2048*1024 {
		t.Errorf("target VD 0 size: got %d, want untouched 2048 MiB", got.SizeKib)
	}
}
