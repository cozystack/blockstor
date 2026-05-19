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

// TestSnapshotRestoreConstrainsProviderToSource: a clone whose source
// snapshot lives on ZFS_THIN must only land on ZFS_THIN candidate
// pools, even when LVM_THIN pools share the same candidate nodes.
// Pinned because `zfs send` payloads can't be replayed onto an LVM
// pool via `dd` — without the provider filter the satellite would
// fail opaquely at SendSnapshot/RecvSnapshot time. Bug 15.
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

	// 2) autoplace — placer must filter candidates to ZFS_THIN only.
	body, _ = json.Marshal(map[string]any{
		"select_filter": map[string]any{"place_count": 2},
	})

	resp = httpPost(t, base+"/v1/resource-definitions/pvc-2/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("autoplace status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().ListByDefinition(ctx, "pvc-2")
	if err != nil {
		t.Fatalf("list pvc-2: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("placed: got %d, want 2", len(got))
	}

	for i := range got {
		stor := got[i].Props["StorPoolName"]
		// Every placed replica must be on a zfs-target-* pool.
		if stor == "" || stor[:4] != "zfs-" {
			t.Errorf("replica on %s landed on %q, want a zfs-target-* pool",
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
// empty-nodes branch: no `--node-name` arguments → the handler looks
// up the source RD's parent ResourceGroup and auto-places against the
// RG's SelectFilter.PlaceCount. Mirrors upstream LINSTOR's behaviour
// when `linstor s r rst --to-resource X` runs without explicit nodes.
//
// Pre-fix the empty-nodes branch was a silent no-op alongside the
// broken explicit-nodes branch — the new RD landed with zero
// Resources regardless of how the operator invoked the CLI.
func TestSnapshotRestoreBug354AutoplacesWhenNodesEmpty(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Parent RG with place_count=2 — the new RD inherits the RG
	// via the source RD, so the empty-nodes branch must place
	// exactly 2 Resources via the placer.
	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-1",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount: 2,
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "pvc-src",
		ResourceGroupName: "rg-1",
	}); err != nil {
		t.Fatalf("seed source RD: %v", err)
	}

	// Two candidate ZFS pools, one each on the snapshot's nodes.
	// constrainAutoplaceToSnapshotNodes restricts placement to the
	// snapshot's local nodes (no send-recv yet), so the placer
	// must find exactly these two candidates.
	for _, n := range []string{"n1", "n2"} {
		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "zpool-" + n, NodeName: n,
			ProviderKind: apiv1.StoragePoolKindZFSThin, FreeCapacity: 10 * 1024 * 1024,
		}); err != nil {
			t.Fatalf("seed candidate pool on %s: %v", n, err)
		}
	}

	// Source resource pins the source provider to ZFS_THIN so
	// resolveCloneSourceProviderKind returns it and the placer
	// only considers ZFS_THIN candidates.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-src", NodeName: "n1",
		Props: map[string]string{"StorPoolName": "zpool-n1"},
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

	got, err := st.Resources().ListByDefinition(ctx, "pvc-restored")
	if err != nil {
		t.Fatalf("list restored Resources: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("Resource CRDs stamped: got %d, want %d (inherited from RG place_count)",
			len(got), 2)
	}

	// Both replicas must land on the snapshot's nodes (no
	// cross-node clone until send-recv lands).
	wantNodes := map[string]bool{"n1": false, "n2": false}
	for _, res := range got {
		if _, ok := wantNodes[res.NodeName]; !ok {
			t.Errorf("placed off snapshot node set: got %q", res.NodeName)

			continue
		}

		wantNodes[res.NodeName] = true
	}

	for n, placed := range wantNodes {
		if !placed {
			t.Errorf("expected a replica on snapshot node %q, none placed", n)
		}
	}
}
