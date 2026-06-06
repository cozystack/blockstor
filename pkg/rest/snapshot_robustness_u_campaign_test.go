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
	"io"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestU464_SingleReplicaZFSRestorePath pins the upstream U464 BS-side
// contract. Upstream U464 is about single-node ZFS in-place rollback;
// blockstor DELIBERATELY rejects in-place rollback (501, known-delta
// row #73). The supported single-node path is RESTORE: snapshot → a new
// resource-definition cloned from the snapshot on the same single node,
// on a zfs-thin pool. This test pins that path end-to-end at the REST
// layer: a single-replica snapshot restores to a new RD with the
// snapshot's volume layout and one replica on the snapshot's node.
func TestU464_SingleReplicaZFSRestorePath(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "ccu3-u464-src"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// Single diskful replica on n1, zfs-thin pool.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "ccu3-u464-src",
		NodeName: "n1",
		Props:    map[string]string{storPoolPropKey: "zfs-thin"},
	}); err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap",
		ResourceName: "ccu3-u464-src",
		Nodes:        []string{"n1"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(snapshotRestoreRequest{
		ToResource:   "ccu3-u464-tgt",
		FromSnapshot: "snap",
		Nodes:        []string{"n1"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/ccu3-u464-src/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201 (single-replica ZFS restore must work)", resp.StatusCode)
	}

	// Target RD exists and carries the clone-source marker so the
	// satellite materialises it via RestoreVolumeFromSnapshot (zfs clone)
	// rather than CreateVolume (empty).
	got, err := st.ResourceDefinitions().Get(ctx, "ccu3-u464-tgt")
	if err != nil {
		t.Fatalf("restored RD missing: %v", err)
	}

	if got.Props["BlockstorRestoreFromSnapshot"] != "ccu3-u464-src:snap" {
		t.Errorf("restore-from marker: got %q, want %q",
			got.Props["BlockstorRestoreFromSnapshot"], "ccu3-u464-src:snap")
	}

	// Exactly one replica, on the snapshot's single node.
	resList, err := st.Resources().ListByDefinition(ctx, "ccu3-u464-tgt")
	if err != nil {
		t.Fatalf("list restored resources: %v", err)
	}

	if len(resList) != 1 || resList[0].NodeName != "n1" {
		t.Fatalf("restored replicas: got %+v, want exactly one on n1", resList)
	}
}

// TestU282_RestoredResourcesPersistForSync pins the upstream U282
// contract at the controller-seed boundary: a snapshot-restored volume
// must NOT be garbage-collected before its initial sync finishes
// (upstream: a restored PVC was destroyed seconds after creation). The
// blockstor restore path stamps real Resource CRDs on the snapshot's
// nodes AND marks the RD with BlockstorRestoreFromSnapshot, so the
// satellite seeds from the snapshot. Because the parent RD exists, the
// orphan-GC path (which only fires for Resources whose parent RD is
// gone) never targets these replicas — they persist through sync.
//
// This test pins the invariant that the restore creates persistent
// (non-orphan) replicas with a parent RD present, and that re-listing
// after the restore call returns the same replicas (no implicit
// controller-side delete in the restore handler itself).
func TestU282_RestoredResourcesPersistForSync(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "ccu3-u282-src"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap",
		ResourceName: "ccu3-u282-src",
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
		ToResource:   "ccu3-u282-tgt",
		FromSnapshot: "snap",
		Nodes:        []string{"n1", "n2"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/ccu3-u282-src/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	// The parent RD of the restored replicas MUST exist — this is the
	// precondition that keeps the orphan-GC path from ever targeting
	// them during the initial sync.
	if _, err := st.ResourceDefinitions().Get(ctx, "ccu3-u282-tgt"); err != nil {
		t.Fatalf("restored RD missing — replicas would be orphan-GC'd mid-sync: %v", err)
	}

	resList, err := st.Resources().ListByDefinition(ctx, "ccu3-u282-tgt")
	if err != nil {
		t.Fatalf("list restored resources: %v", err)
	}

	if len(resList) != 2 {
		t.Fatalf("restored replicas: got %d, want 2 (persistent through sync)", len(resList))
	}

	// None of the restored replicas may carry the Diskless flag — a
	// diskless placeholder would not hold the snapshot's data and a GC of
	// it mid-sync would be the upstream U282 footgun. They must be real
	// diskful replicas seeded from the snapshot.
	for i := range resList {
		for _, f := range resList[i].Flags {
			if f == apiv1.ResourceFlagDiskless {
				t.Errorf("restored replica on %s is Diskless — not a real data copy",
					resList[i].NodeName)
			}
		}
	}
}

// TestU318_BackupListRejectionEnvelope pins the U318 DELIBERATE DELTA
// (known-delta row #53 `backup l`): blockstor does NOT implement the
// backups subsystem (S3 / cross-cluster shipping is out of scope). A
// `backups/ship` request returns a structured 501 envelope pointing at
// the in-cluster snapshot-restore-resource alternative, rather than a
// bare 404 that would confuse the operator into thinking the controller
// crashed. This pins the rejection envelope shape so the delta stays
// operator-actionable.
func TestU318_BackupShipRejectionEnvelope(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/remotes/some-remote/backups/ship", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("backups/ship status: got %d, want 501", resp.StatusCode)
	}

	raw, _ := io.ReadAll(resp.Body)
	if len(raw) == 0 {
		t.Fatal("backups/ship 501 must carry an actionable body, got empty")
	}

	// The body must name the supported in-cluster alternative so the
	// operator has a concrete next action rather than a dead end.
	if !strings.Contains(strings.ToLower(string(raw)), "snapshot-restore-resource") {
		t.Errorf("backups/ship 501 body should point at snapshot-restore-resource; got %q", string(raw))
	}
}
