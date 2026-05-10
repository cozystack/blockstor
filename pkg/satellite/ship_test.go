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
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/satellite"
	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// TestShipSnapshotZFSUsesZfsSendRecv: when the source pool is ZFS,
// ShipSnapshot dispatches `zfs send | zfs recv` over SSH.
func TestShipSnapshotZFSUsesZfsSendRecv(t *testing.T) {
	fx := storage.NewFakeExec()

	zfsPool := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"zpool1": zfsPool},
		ShipExec:  fx,
	})

	// Apply seeds the resource→pool mapping so ShipSnapshot can
	// route. We don't need an LV to exist for the routing test.
	fx.Expect("zfs list -H -o name tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	_, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name: "pvc-1", NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "zpool1"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	resp, err := rec.ShipSnapshot(t.Context(), &satellitepb.ShipSnapshotRequest{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
		TargetNode:   "n2",
	})
	if err != nil {
		t.Fatalf("ShipSnapshot: %v", err)
	}

	if !resp.GetOk() {
		t.Fatalf("expected ok; got %s", resp.GetMessage())
	}

	found := false

	for _, line := range fx.CommandLines() {
		if strings.Contains(line, "zfs send") {
			found = true
		}
	}

	if !found {
		t.Errorf("expected `zfs send` in calls; got %v", fx.CommandLines())
	}
}

// TestShipSnapshotLVMThinUsesThinSendRecv: LVM-thin source picks the
// thin-send-recv mechanism.
func TestShipSnapshotLVMThinUsesThinSendRecv(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		ShipExec:  fx,
	})

	_, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name: "pvc-1", NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	resp, err := rec.ShipSnapshot(t.Context(), &satellitepb.ShipSnapshotRequest{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
		TargetNode:   "n2",
	})
	if err != nil {
		t.Fatalf("ShipSnapshot: %v", err)
	}

	if !resp.GetOk() {
		t.Fatalf("expected ok; got %s", resp.GetMessage())
	}

	found := false

	for _, line := range fx.CommandLines() {
		if strings.Contains(line, "thin-send-recv") || strings.Contains(line, "thin_send") {
			found = true
		}
	}

	if !found {
		t.Errorf("expected thin-send-recv in calls; got %v", fx.CommandLines())
	}
}

// TestShipSnapshotUnsupportedMechanism: the dispatcher rejects an
// explicit unsupported Mechanism (e.g. controller-side typo, or a
// future mechanism the satellite hasn't implemented yet) with
// Ok=false body-level. Without this gate the satellite would
// silently swallow the request — controller would think the ship
// succeeded and expose a phantom destination snapshot to the CSI
// CreateVolume(from-snapshot) flow.
func TestShipSnapshotUnsupportedMechanism(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		ShipExec:  fx,
	})

	_, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name: "pvc-1", NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	resp, err := rec.ShipSnapshot(t.Context(), &satellitepb.ShipSnapshotRequest{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
		TargetNode:   "n2",
		Mechanism:    "rsync-over-pigeon", // not real
	})
	if err != nil {
		t.Fatalf("ShipSnapshot transport error (want body-level fail): %v", err)
	}

	if resp.GetOk() {
		t.Errorf("Ok: got true on unsupported mechanism; want false")
	}

	if !strings.Contains(strings.ToLower(resp.GetMessage()), "unsupported") {
		t.Errorf("error message must mention unsupported mechanism; got %q",
			resp.GetMessage())
	}

	for _, line := range fx.CommandLines() {
		if strings.Contains(line, "rsync-over-pigeon") ||
			strings.Contains(line, "zfs send") ||
			strings.Contains(line, "thin-send-recv") {
			t.Errorf("ship pipeline must not run on unsupported mechanism: %s", line)
		}
	}
}

// TestShipSnapshotUnknownResource → ok=false with message.
func TestShipSnapshotUnknownResource(t *testing.T) {
	fx := storage.NewFakeExec()
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": lvm.NewThin(lvm.ThinConfig{}, fx)},
	})

	resp, err := rec.ShipSnapshot(t.Context(), &satellitepb.ShipSnapshotRequest{
		ResourceName: "ghost",
		SnapshotName: "snap-1",
		TargetNode:   "n2",
	})
	if err != nil {
		t.Fatalf("ShipSnapshot: %v", err)
	}

	if resp.GetOk() {
		t.Errorf("expected !ok for unknown resource")
	}
}
