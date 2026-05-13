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
