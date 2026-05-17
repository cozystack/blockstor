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
	"sync"
	"testing"

	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// Bug 267 (HIGH, capacity leak): `linstor r td <node> <rd> --diskless`
// flipped Spec.Flags=[DISKLESS] on a previously-diskful replica but
// the satellite NEVER called provider.DeleteVolume on the backing
// LV / zvol. The Resource shows Diskless in `linstor r l`, the kernel
// reports `disk: Diskless`, but the underlying volume sits on disk
// forever — visible to `lvs` / `zfs list` and counted against the
// pool's free-space budget. Repeated demote-promote cycles compound
// the leak.
//
// Fix: when applyOne sees a diskless DesiredResource whose Volumes
// carry a non-empty StoragePool (the dispatcher stamps the
// historical pool on the toggle path so the satellite can find the
// provider), iterate the volumes and call provider.DeleteVolume on
// each so the backing storage is reclaimed.
//
// This test pins the invariant by feeding the reconciler exactly the
// shape the dispatcher emits on a toggle-to-diskless reconcile and
// asserting DeleteVolume IS observed on the registered provider.
// MUST fail before the fix lands.
func TestBug267ToggleToDisklessReclaimsBackingVolume(t *testing.T) {
	prov := &disklessCleanupFakeProvider{}

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": prov},
	})

	// Shape the dispatcher emits when target.Spec.Flags contains
	// DISKLESS but target.Spec.StoragePool is still set (the REST
	// handler keeps the historical pool on demote). The fix wires
	// the dispatcher to stamp that pool onto the DesiredVolume so
	// the satellite knows which provider's DeleteVolume to call.
	dr := &intent.DesiredResource{
		Name:     "pvc-bug267",
		NodeName: "n1",
		Flags:    []string{"DISKLESS"},
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024, StoragePool: "thin1"},
		},
	}

	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{dr})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if len(results) != 1 || !results[0].GetOk() {
		t.Fatalf("apply result not ok: %+v", results)
	}

	prov.mu.Lock()
	deleteCalls := prov.deleteCalls
	prov.mu.Unlock()

	if deleteCalls == 0 {
		t.Errorf("DeleteVolume not invoked on toggle-to-diskless path; " +
			"backing volume leaks on disk (Bug 267). " +
			"The satellite must reclaim the volume when transitioning " +
			"to DISKLESS.")
	}
}

// TestBug267DisklessSkipsDeleteWhenNoStoragePool guards the
// happy-path: a fresh DISKLESS replica that NEVER had storage
// (StoragePool empty on every DesiredVolume) must NOT trigger a
// DeleteVolume — it never owned a volume to begin with. Without
// this guard the fix would log spurious DeleteVolume errors on
// every plain diskless replica.
func TestBug267DisklessSkipsDeleteWhenNoStoragePool(t *testing.T) {
	prov := &disklessCleanupFakeProvider{}

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": prov},
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-bug267-fresh-diskless",
		NodeName: "n1",
		Flags:    []string{"DISKLESS"},
		Volumes: []*intent.DesiredVolume{
			// Empty StoragePool — the shape a fresh-from-creation
			// DISKLESS replica gets from the dispatcher.
			{VolumeNumber: 0, SizeKib: 1024, StoragePool: ""},
		},
	}

	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{dr})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if len(results) != 1 || !results[0].GetOk() {
		t.Fatalf("apply result not ok: %+v", results)
	}

	prov.mu.Lock()
	deleteCalls := prov.deleteCalls
	prov.mu.Unlock()

	if deleteCalls != 0 {
		t.Errorf("DeleteVolume invoked on fresh DISKLESS replica with no "+
			"prior storage; got %d calls", deleteCalls)
	}
}

// disklessCleanupFakeProvider tracks DeleteVolume calls so the test
// can assert the toggle-to-diskless reconciler path invokes it.
type disklessCleanupFakeProvider struct {
	mu          sync.Mutex
	deleteCalls int
}

func (*disklessCleanupFakeProvider) Kind() string { return "LVM_THIN" }

func (*disklessCleanupFakeProvider) PoolStatus(_ context.Context) (storage.PoolStatus, error) {
	return storage.PoolStatus{SupportsSnapshots: true}, nil
}

func (*disklessCleanupFakeProvider) CreateVolume(_ context.Context, _ storage.Volume) error {
	return nil
}

func (f *disklessCleanupFakeProvider) DeleteVolume(_ context.Context, _ storage.Volume) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls++

	return nil
}

func (*disklessCleanupFakeProvider) ResizeVolume(_ context.Context, _ storage.Volume) error {
	return nil
}

func (*disklessCleanupFakeProvider) VolumeStatus(_ context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	return storage.VolumeStatus{
		DevicePath:   "/dev/fake/" + vol.ResourceName,
		AllocatedKib: vol.SizeKib,
		UsableKib:    vol.SizeKib,
		State:        "PROVISIONED",
	}, nil
}

func (*disklessCleanupFakeProvider) CreateSnapshot(_ context.Context, _ storage.Snapshot) error {
	return nil
}

func (*disklessCleanupFakeProvider) DeleteSnapshot(_ context.Context, _ storage.Snapshot) error {
	return nil
}

func (*disklessCleanupFakeProvider) RestoreVolumeFromSnapshot(_ context.Context,
	_ storage.Volume, _ storage.Snapshot,
) error {
	return nil
}
