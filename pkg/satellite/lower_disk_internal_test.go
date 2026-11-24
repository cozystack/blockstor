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

package satellite

import (
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/luks"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// TestLowerDiskKibExternalMetadata: metadata on its own pool leaves the
// data volume at the definition's size, as LINSTOR sizes it.
func TestLowerDiskKibExternalMetadata(t *testing.T) {
	t.Parallel()

	vol := &intent.DesiredVolume{SizeKib: 20971520, DeviceSizeKib: 20971528, MetaPool: "meta"}

	got := lowerDiskKib(&intent.DesiredResource{LayerStack: []string{"DRBD", "STORAGE"}}, vol)
	if got != 20971520 {
		t.Fatalf("external metadata: got %d KiB, want 20971520", got)
	}
}

// TestGrowLowerDisk covers when a definition grow resizes the lower disk
// and the device, and that volumes created before lower disks were sized
// for metadata are not resized by an upgrade alone. The grow step is the
// satellite's (growBacking), with DRBD on for the volumes that have it.
func TestGrowLowerDisk(t *testing.T) {
	t.Parallel()

	const (
		gib20      = 20971520
		sizedDisk  = 20490 * 1024 // a 20 GiB volume's lower disk with metadata
		adoptedDev = 20971528
	)

	cases := []struct {
		name               string
		definition, device int64
		onDisk             int64
		wantResized        bool
		wantStorageGrow    bool
	}{
		{"unchanged volume sized for metadata", gib20, 20972116, sizedDisk, false, false},
		{"volume adopted from LINSTOR", gib20, adoptedDev, 20976048, false, false},
		{"volume created at the definition's size", gib20, 20961884, gib20, false, false},
		{"small grow the lower disk already holds", gib20 + 100, 20972116 - 1000, sizedDisk, true, false},
		{"small grow past the data the lower disk holds", gib20 + 4096, gib20, sizedDisk, true, true},
		{"grow past the lower disk", 21 * 1024 * 1024, 20972116, sizedDisk, true, true},
	}

	luksOnly := []struct {
		name            string
		definition      int64
		onDisk          int64
		wantResized     bool
		wantStorageGrow bool
	}{
		{"unchanged LUKS volume", gib20, gib20 + luks.HeaderKiB, false, false},
		{"LUKS volume created at the definition's size", gib20, gib20, false, false},
		{"small LUKS grow inside the header room", gib20 + 1024, gib20 + luks.HeaderKiB, true, true},
	}

	for _, tc := range luksOnly {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			provider := zfs.NewProvider(zfs.Config{Pool: "data", Thin: true}, fx)
			dr := &intent.DesiredResource{Name: "pvc-l", LayerStack: []string{"LUKS", "STORAGE"}}
			vol := &intent.DesiredVolume{SizeKib: tc.definition}

			r := NewReconciler(ReconcilerConfig{})

			resized, err := r.growBacking(t.Context(), provider, dr, vol, tc.onDisk)
			if err != nil {
				t.Fatalf("growBacking: %v", err)
			}

			if resized != tc.wantResized || (len(fx.CommandLines()) > 0) != tc.wantStorageGrow {
				t.Errorf("resized = %v, storage calls %v; want resized %v, grow %v",
					resized, fx.CommandLines(), tc.wantResized, tc.wantStorageGrow)
			}
		})
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			provider := zfs.NewProvider(zfs.Config{Pool: "data", Thin: true}, fx)
			dr := &intent.DesiredResource{Name: "pvc-g"}
			vol := &intent.DesiredVolume{SizeKib: tc.definition, DeviceSizeKib: tc.device}

			r := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(storage.NewFakeExec())})

			resized, err := r.growBacking(t.Context(), provider, dr, vol, tc.onDisk)
			if err != nil {
				t.Fatalf("growBacking: %v", err)
			}

			if resized != tc.wantResized {
				t.Errorf("resized = %v, want %v", resized, tc.wantResized)
			}

			grew := false

			for _, line := range fx.CommandLines() {
				if strings.HasPrefix(line, "zfs set volsize=") {
					grew = true
				}
			}

			if grew != tc.wantStorageGrow {
				t.Errorf("lower disk grown = %v, want %v (%v)", grew, tc.wantStorageGrow, fx.CommandLines())
			}
		})
	}
}

// TestLUKSUnderDRBDMatchesLinstor works LINSTOR v1.33.1's sizes for a
// 20 GiB LUKS volume with 7 peer slots on a 16 KiB-volblocksize zvol:
// the DRBD lower disk is 20976040 KiB, the LUKS layer adds its 16 MiB
// LUKS2 header (LuksLayerSizeCalculator.java:24,37), the storage layer
// rounds up to the volblocksize (StorageLayerSizeCalculator.java:78-82)
// to 20992432 KiB, and the DRBD device on the 20976048 KiB left under
// the header is 41943056 sectors, as without LUKS.
func TestLUKSUnderDRBDMatchesLinstor(t *testing.T) {
	t.Parallel()

	geometry := drbd.DefaultGeometry().WithMaxPeers(7)

	storage := drbd.InternalMetadataGrossKiB(20971520, geometry) + luks.HeaderKiB
	storage = (storage + 15) / 16 * 16

	if storage != 20992432 {
		t.Fatalf("storage volume: got %d KiB, want 20992432", storage)
	}

	if dev := drbd.InternalMetadataNetKiB(storage-luks.HeaderKiB, geometry); dev*2 != 41943056 {
		t.Fatalf("DRBD device: got %d sectors, want 41943056", dev*2)
	}

	got := lowerDiskKib(&intent.DesiredResource{LayerStack: []string{"DRBD", "LUKS", "STORAGE"}},
		&intent.DesiredVolume{SizeKib: 20971520, DeviceSizeKib: 20971528})
	if want := drbd.InternalMetadataGrossKiB(20971528, drbd.DefaultGeometry()) + luks.HeaderKiB; got != want {
		t.Fatalf("lowerDiskKib under LUKS: got %d, want %d", got, want)
	}
}
