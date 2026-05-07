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
	"slices"
	"testing"

	"github.com/cozystack/blockstor/pkg/satellite"
	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// TestCreateSnapshotDispatchesToProvider: after Apply has registered
// the resource → pool mapping, CreateSnapshot dispatches `lvcreate -s`.
func TestCreateSnapshotDispatchesToProvider(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
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

	resp, err := rec.CreateSnapshot(t.Context(), &satellitepb.CreateSnapshotRequest{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	if !resp.GetOk() {
		t.Fatalf("expected ok; got %s", resp.GetMessage())
	}

	want := "lvcreate --snapshot --name pvc-1_snap-1_00000 vg/pvc-1_00000"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q; got %v", want, fx.CommandLines())
	}
}

// TestCreateSnapshotUnknownResource: snapshot of a resource the
// satellite has never seen → ok=false with a non-empty message.
func TestCreateSnapshotUnknownResource(t *testing.T) {
	fx := storage.NewFakeExec()
	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
	})

	resp, err := rec.CreateSnapshot(t.Context(), &satellitepb.CreateSnapshotRequest{
		ResourceName: "ghost",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	if resp.GetOk() {
		t.Errorf("expected !ok for unknown resource")
	}

	if resp.GetMessage() == "" {
		t.Errorf("expected non-empty message")
	}
}

// TestDeleteSnapshotDispatchesToProvider: lvremove via the recorded
// pool mapping.
func TestDeleteSnapshotDispatchesToProvider(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
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

	resp, err := rec.DeleteSnapshot(t.Context(), &satellitepb.DeleteSnapshotRequest{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	if !resp.GetOk() {
		t.Errorf("expected ok; got %s", resp.GetMessage())
	}

	want := "lvremove --force vg/pvc-1_snap-1_00000"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q; got %v", want, fx.CommandLines())
	}
}
