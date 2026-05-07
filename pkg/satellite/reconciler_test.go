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

// TestApplyCreatesVolumeViaProvider: a single DesiredResource with one
// volume on the registered LVM-thin pool ends up calling lvcreate.
func TestApplyCreatesVolumeViaProvider(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
	})

	results, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name:     "pvc-1",
			NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if len(results) != 1 || !results[0].GetOk() {
		t.Fatalf("results: %+v", results)
	}

	if !slices.Contains(fx.CommandLines(),
		"lvcreate --thin --virtualsize 1024MiB --name pvc-1_00000 vg/tp") {
		t.Errorf("expected lvcreate; got %v", fx.CommandLines())
	}
}

// TestApplyUnknownPoolFails: requesting a pool the satellite doesn't
// know about → per-resource OK=false; the batch keeps going.
func TestApplyUnknownPoolFails(t *testing.T) {
	fx := storage.NewFakeExec()
	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
	})

	results, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name:     "pvc-1",
			NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024, StoragePool: "missing"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if len(results) != 1 || results[0].GetOk() {
		t.Fatalf("expected !ok; got %+v", results[0])
	}

	if results[0].GetMessage() == "" {
		t.Errorf("expected non-empty message")
	}
}

// TestApplyDisklessSkipsStorage: a resource flagged DISKLESS has no
// local backing storage; the reconciler must not call CreateVolume.
func TestApplyDisklessSkipsStorage(t *testing.T) {
	fx := storage.NewFakeExec()
	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
	})

	results, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name:     "pvc-1",
			NodeName: "n1",
			Flags:    []string{"DISKLESS"},
			Volumes: []*satellitepb.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if len(results) != 1 || !results[0].GetOk() {
		t.Fatalf("results: %+v", results)
	}

	for _, line := range fx.CommandLines() {
		if len(line) >= 8 && line[:8] == "lvcreate" {
			t.Errorf("DISKLESS resource issued lvcreate: %s", line)
		}
	}
}

// TestApplyHandlesMultipleResources: all-or-nothing batch processing —
// every input gets a result, regardless of individual outcome.
func TestApplyHandlesMultipleResources(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
	})

	results, err := rec.Apply(t.Context(), []*satellitepb.DesiredResource{
		{
			Name: "pvc-1", NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
		},
		{
			Name: "pvc-2", NodeName: "n1",
			Volumes: []*satellitepb.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 2048 * 1024, StoragePool: "thin1"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("len(results): got %d, want 2", len(results))
	}

	for _, r := range results {
		if !r.GetOk() {
			t.Errorf("expected ok for %s; got %s", r.GetName(), r.GetMessage())
		}
	}
}
