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
	"context"
	"errors"
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

	srv := satellite.NewGRPCServer(rec, storage.NewFakeExec())

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

// TestGRPCServerApplyStoragePoolsRegistersValid pins the Phase 10.5
// dynamic-pool contract: each well-formed (kind + required props)
// DesiredStoragePool ACKs Ok=true and the matching `storage.Provider`
// becomes available in the reconciler's registry; malformed pools
// (unknown kind or missing required prop) ACK Ok=false with a
// readable Message so the controller can surface it.
func TestGRPCServerApplyStoragePoolsRegistersValid(t *testing.T) {
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
		NodeName:  "n1",
	})
	srv := satellite.NewGRPCServer(rec, storage.NewFakeExec())

	resp, err := srv.ApplyStoragePools(t.Context(), &satellitepb.ApplyStoragePoolsRequest{
		Pools: []*satellitepb.DesiredStoragePool{
			{
				Name:         "thin1",
				ProviderKind: "LVM_THIN",
				Props:        map[string]string{"StorDriver/LvmVg": "vg", "StorDriver/ThinPool": "tp"},
			},
			{
				Name:         "zfs1",
				ProviderKind: "ZFS",
				Props:        map[string]string{"StorDriver/ZPool": "rpool"},
			},
			{
				Name:         "broken",
				ProviderKind: "MADE_UP_KIND",
			},
			{
				Name:         "incomplete",
				ProviderKind: "LVM_THIN",
				// missing StorDriver/LvmVg
			},
		},
	})
	if err != nil {
		t.Fatalf("ApplyStoragePools: %v", err)
	}

	if len(resp.GetResults()) != 4 {
		t.Fatalf("results: got %d, want 4", len(resp.GetResults()))
	}

	byName := map[string]*satellitepb.StoragePoolApplyResult{}
	for _, r := range resp.GetResults() {
		byName[r.GetName()] = r
	}

	if !byName["thin1"].GetOk() {
		t.Errorf("thin1: want Ok=true, got false (%s)", byName["thin1"].GetMessage())
	}

	if !byName["zfs1"].GetOk() {
		t.Errorf("zfs1: want Ok=true, got false (%s)", byName["zfs1"].GetMessage())
	}

	if byName["broken"].GetOk() {
		t.Errorf("broken: want Ok=false (unknown kind), got true")
	}

	if byName["incomplete"].GetOk() {
		t.Errorf("incomplete: want Ok=false (missing LvmVg), got true")
	}

	if byName["incomplete"].GetMessage() == "" {
		t.Errorf("incomplete: expected non-empty Message naming the missing key")
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
	srv := satellite.NewGRPCServer(rec, storage.NewFakeExec())

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
	srv := satellite.NewGRPCServer(rec, storage.NewFakeExec())

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
	srv := satellite.NewGRPCServer(rec, storage.NewFakeExec())

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
	srv := satellite.NewGRPCServer(rec, storage.NewFakeExec())

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
	srv := satellite.NewGRPCServer(rec, storage.NewFakeExec())

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

// TestGRPCServerApplyResourcesCtxCancelBubbles pins the
// transport-error branch of GRPCServer.ApplyResources: when the
// underlying Reconciler.Apply returns an error (today: cancelled
// context), the gRPC handler must propagate it as a gRPC error
// rather than swallow it as Ok=false body-level. This is the
// signal the controller's dispatcher uses to distinguish "satellite
// rejected this batch" (per-replica) from "transport failed,
// retry the batch" (gRPC error).
func TestGRPCServerApplyResourcesCtxCancelBubbles(t *testing.T) {
	fx := storage.NewFakeExec()
	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  t.TempDir(),
		NodeName:  "n1",
	})

	srv := satellite.NewGRPCServer(rec, storage.NewFakeExec())

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := srv.ApplyResources(ctx, &satellitepb.ApplyResourcesRequest{
		Resources: []*satellitepb.DesiredResource{
			{Name: "pvc-1", NodeName: "n1"},
		},
	})
	if err == nil {
		t.Fatalf("ApplyResources with cancelled ctx: got nil, want gRPC-shaped error")
	}
}

// TestGRPCServerDeleteResourceProviderError pins the per-replica
// Ok=false body-level surface when the provider's DeleteVolume
// fails (lvremove EBUSY, zfs destroy held). DeleteResource must
// surface the error in the response Message rather than bubble it
// as a gRPC error — the dispatcher requires this distinction to
// avoid retrying the whole batch on one replica's lvremove EBUSY.
func TestGRPCServerDeleteResourceProviderError(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// First Apply succeeds (registers resource→pool map + creates LV).
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-del-busy_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// During DeleteResource: lvExists → "yes", lvremove fails (busy).
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-del-busy_00000",
		storage.FakeResponse{Stdout: []byte("pvc-del-busy_00000\n")})
	fx.Expect("lvremove --force vg/pvc-del-busy_00000",
		storage.FakeResponse{Err: errLVRemoveEBUSY})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	// Seed the resource→pool map by running Apply first.
	if _, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name: "pvc-del-busy", NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024, StoragePool: "thin1"},
			},
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
			},
		},
	}); err != nil {
		t.Fatalf("Apply (seed): %v", err)
	}

	srv := satellite.NewGRPCServer(rec, storage.NewFakeExec())

	resp, err := srv.DeleteResource(t.Context(), &satellitepb.DeleteResourceRequest{
		Name:          "pvc-del-busy",
		StoragePool:   "thin1",
		VolumeNumbers: []int32{0},
	})
	if err != nil {
		t.Fatalf("DeleteResource: got transport error %v, want Ok=false body-level", err)
	}

	if resp.GetOk() {
		t.Errorf("Ok: got true, want false on lvremove failure")
	}

	if resp.GetMessage() == "" {
		t.Errorf("expected non-empty message describing the lvremove failure")
	}
}

var errLVRemoveEBUSY = errors.New("lvremove: device or resource busy")
