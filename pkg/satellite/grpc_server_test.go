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

package satellite_test

import (
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// TestGRPCServerApplyResources: pass-through to Reconciler.Apply
// returns per-resource results in the gRPC envelope. Pins the
// "no per-resource error becomes a gRPC error" invariant — a single
// bad replica must not sink the whole batch's transport.
func TestGRPCServerApplyResources(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	srv := satellite.NewGRPCServer(rec)

	resp, err := srv.ApplyResources(t.Context(), &satellitepb.ApplyResourcesRequest{
		Resources: []*satellitepb.DesiredResource{
			{
				Name:     "pvc-1",
				NodeName: "n1",
				Volumes: []*satellitepb.DesiredVolume{
					{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
				},
				DrbdOptions: map[string]string{
					"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("ApplyResources: %v", err)
	}

	if len(resp.GetResults()) != 1 {
		t.Fatalf("results: got %d, want 1", len(resp.GetResults()))
	}

	if !resp.GetResults()[0].GetOk() {
		t.Errorf("Ok: got false (%s); want true", resp.GetResults()[0].GetMessage())
	}
}

// TestGRPCServerApplyStoragePoolsAcksAll pins the placeholder
// behaviour: every requested pool is ACK'd with Ok=true. The
// satellite uses a startup-flag Provider registry, so the
// controller's pool spec is informational today; the controller
// must not interpret a missing pool ACK as a failure on this
// transitional path.
func TestGRPCServerApplyStoragePoolsAcksAll(t *testing.T) {
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
		NodeName:  "n1",
	})
	srv := satellite.NewGRPCServer(rec)

	resp, err := srv.ApplyStoragePools(t.Context(), &satellitepb.ApplyStoragePoolsRequest{
		Pools: []*satellitepb.DesiredStoragePool{
			{Name: "thin1"},
			{Name: "zfs1"},
			{Name: "loopfile"},
		},
	})
	if err != nil {
		t.Fatalf("ApplyStoragePools: %v", err)
	}

	if len(resp.GetResults()) != 3 {
		t.Fatalf("results: got %d, want 3", len(resp.GetResults()))
	}

	wantNames := map[string]bool{"thin1": false, "zfs1": false, "loopfile": false}
	for _, r := range resp.GetResults() {
		if !r.GetOk() {
			t.Errorf("pool %s: Ok=false unexpectedly", r.GetName())
		}

		if _, ok := wantNames[r.GetName()]; !ok {
			t.Errorf("unexpected pool name in results: %s", r.GetName())
			continue
		}

		wantNames[r.GetName()] = true
	}

	for name, seen := range wantNames {
		if !seen {
			t.Errorf("missing pool ACK for %s", name)
		}
	}
}

// TestGRPCServerApplyStoragePoolsEmpty: no pools requested → empty
// results, no error. A controller that hasn't yet sent a pool list
// shouldn't crash the satellite.
func TestGRPCServerApplyStoragePoolsEmpty(t *testing.T) {
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
		NodeName:  "n1",
	})
	srv := satellite.NewGRPCServer(rec)

	resp, err := srv.ApplyStoragePools(t.Context(), &satellitepb.ApplyStoragePoolsRequest{})
	if err != nil {
		t.Fatalf("ApplyStoragePools: %v", err)
	}

	if len(resp.GetResults()) != 0 {
		t.Errorf("results: got %d, want 0", len(resp.GetResults()))
	}
}

// TestGRPCServerCreateSnapshotUnknownResource: snapshot of a
// resource the satellite hasn't seen yet → Ok=false in the response
// body, NOT a gRPC error. Pins the "transport stays clean even when
// per-snapshot business fails" invariant.
func TestGRPCServerCreateSnapshotUnknownResource(t *testing.T) {
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
		NodeName:  "n1",
	})
	srv := satellite.NewGRPCServer(rec)

	resp, err := srv.CreateSnapshot(t.Context(), &satellitepb.CreateSnapshotRequest{
		ResourceName: "unknown-rd",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot transport error (want body-level fail): %v", err)
	}

	if resp.GetOk() {
		t.Errorf("Ok: got true on unknown resource; want false")
	}
}

// TestGRPCServerDeleteSnapshotUnknownResource: same body-level-fail
// invariant for the delete-snapshot path. The snapshot CRD reconciler
// retries on transport errors but treats Ok=false as a per-replica
// outcome it logs and moves on from.
func TestGRPCServerDeleteSnapshotUnknownResource(t *testing.T) {
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
		NodeName:  "n1",
	})
	srv := satellite.NewGRPCServer(rec)

	resp, err := srv.DeleteSnapshot(t.Context(), &satellitepb.DeleteSnapshotRequest{
		ResourceName: "unknown-rd",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("DeleteSnapshot transport error (want body-level fail): %v", err)
	}

	if resp.GetOk() {
		t.Errorf("Ok: got true on unknown resource; want false")
	}
}

// TestGRPCServerDeleteResourceMissing: DeleteResource on a resource
// that was never applied (no .res, no LV) is idempotent — Ok=true
// with no message. Pins the cleanup-during-RD-delete contract: the
// controller fans DeleteResource at every replica regardless of
// which ones actually have storage allocated, and a missing one
// must not surface as a failure.
func TestGRPCServerDeleteResourceMissing(t *testing.T) {
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
		NodeName:  "n1",
	})
	srv := satellite.NewGRPCServer(rec)

	resp, err := srv.DeleteResource(t.Context(), &satellitepb.DeleteResourceRequest{
		Name:          "never-applied",
		StoragePool:   "thin1", // pool not registered → DeleteVolume just no-ops
		VolumeNumbers: []int32{0},
	})
	if err != nil {
		t.Fatalf("DeleteResource transport error: %v", err)
	}

	if !resp.GetOk() {
		t.Errorf("Ok=false on missing resource: %s", resp.GetMessage())
	}
}

// TestGRPCServerShipSnapshotUnknownResource: ShipSnapshot on a
// resource the satellite doesn't have a provider for → Ok=false body,
// no transport error. Pins the same contract for the snapshot-shipping
// RPC the cross-node clone path uses.
func TestGRPCServerShipSnapshotUnknownResource(t *testing.T) {
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
		NodeName:  "n1",
	})
	srv := satellite.NewGRPCServer(rec)

	resp, err := srv.ShipSnapshot(t.Context(), &satellitepb.ShipSnapshotRequest{
		ResourceName: "unknown-rd",
		SnapshotName: "snap-1",
		TargetNode:   "n2",
	})
	if err != nil {
		t.Fatalf("ShipSnapshot transport error (want body-level fail): %v", err)
	}

	if resp.GetOk() {
		t.Errorf("Ok: got true on unknown resource; want false")
	}
}
