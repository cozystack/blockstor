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

package satellite_test

import (
	"fmt"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/luks"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

const (
	adoptedVolumeKiB = 20 * 1024 * 1024
	// adoptedDeviceKiB is the DRBD device of a 20 GiB volume LINSTOR
	// v1.33.1 created on ZFS with a 16 KiB volblocksize: 41943056
	// sectors, 8 KiB more than the definition.
	adoptedDeviceKiB = 20971528
)

// createdZvolKiB applies one volume on a thin ZFS pool and returns the
// size, in KiB, the zvol was created with.
func createdZvolKiB(t *testing.T, vol *intent.DesiredVolume, layerStack []string) int64 {
	t.Helper()

	return createdZvolKiBWithPeerSlots(t, vol, layerStack, nil, "")
}

// createdZvolKiBWithPeerSlots is createdZvolKiB for a replica with the
// given peer-slot count, nil for one without a recorded count, on a pool
// whose dry-run creation reports dryRun; an empty one leaves the
// provider on whole MiB.
func createdZvolKiBWithPeerSlots(
	t *testing.T, vol *intent.DesiredVolume, layerStack []string, peerSlots *int32, dryRun string,
) int64 {
	t.Helper()

	fx := storage.NewFakeExec()
	fx.Expect("zfs list -H -o name data/pvc-adopted_00000", storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("zfs create -n -P -V 1K data/blockstor-volblocksize-probe", storage.FakeResponse{Stdout: []byte(dryRun)})

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"data": zfs.NewProvider(zfs.Config{Pool: "data", Thin: true}, fx)},
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{{
		Name:       "pvc-adopted",
		NodeName:   "n2",
		Volumes:    []*intent.DesiredVolume{vol},
		LayerStack: layerStack,
		PeerSlots:  peerSlots,
	}})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	for _, line := range fx.CommandLines() {
		var kib int64

		_, scanErr := fmt.Sscanf(line, "zfs create -s -V %dK data/pvc-adopted_00000", &kib)
		if scanErr == nil {
			return kib
		}
	}

	t.Fatalf("no zfs create in %v", fx.CommandLines())

	return 0
}

// TestNewLowerDiskHoldsAdoptedDevice reproduces a diskless replica of a
// volume adopted from LINSTOR turned diskful. The zvol used to be the
// definition's size, exactly 20 GiB; with 15 peer slots of internal
// metadata it held a 41923768-sector data area, and the kernel refused
// to attach it under the 41943056-sector device the resource exposes.
// The new zvol must hold that device.
func TestNewLowerDiskHoldsAdoptedDevice(t *testing.T) {
	t.Parallel()

	zvol := createdZvolKiB(t, &intent.DesiredVolume{
		VolumeNumber:  0,
		SizeKib:       adoptedVolumeKiB,
		StoragePool:   "data",
		DeviceSizeKib: adoptedDeviceKiB,
	}, nil)

	if zvol == adoptedVolumeKiB {
		t.Fatalf("zvol created at the definition's size, %d KiB, leaving no room for metadata", zvol)
	}

	data := drbd.InternalMetadataNetKiB(zvol, drbd.GeometryForPeerSlots(drbd.LegacyPeerSlots))
	if data < adoptedDeviceKiB {
		t.Fatalf("zvol of %d KiB holds a %d KiB data area, below the %d KiB device", zvol, data, adoptedDeviceKiB)
	}

	if zvol != 20490*1024 {
		t.Errorf("zvol: got %d KiB, want 20490 MiB", zvol)
	}
}

// TestNewLowerDiskIsSizedForTheReplicaPeerSlots: a replica with LINSTOR's
// seven peer slots gets the lower disk LINSTOR gives it: 20976040 KiB for
// a 20 GiB data area, rounded up to the provider's MiB, and its data area
// holds the adopted device. Sized for fifteen it would be 20490 MiB, a
// bitmap per missing slot larger than LINSTOR's.
func TestNewLowerDiskIsSizedForTheReplicaPeerSlots(t *testing.T) {
	t.Parallel()

	seven := int32(drbd.DefaultPeerSlots)

	zvol := createdZvolKiBWithPeerSlots(t, &intent.DesiredVolume{
		VolumeNumber:  0,
		SizeKib:       adoptedVolumeKiB,
		StoragePool:   "data",
		DeviceSizeKib: adoptedDeviceKiB,
	}, nil, &seven, "")

	if want := int64(20485 * 1024); zvol != want {
		t.Errorf("zvol for 7 peer slots: got %d KiB, want %d (20976040 KiB rounded up to MiB)", zvol, want)
	}

	data := drbd.InternalMetadataNetKiB(zvol, drbd.GeometryForPeerSlots(drbd.DefaultPeerSlots))
	if data < adoptedDeviceKiB {
		t.Fatalf("zvol of %d KiB holds a %d KiB data area with 7 slots, below the %d KiB device", zvol, data, adoptedDeviceKiB)
	}
}

// TestNewLowerDiskHoldsLargestReportedDevice: when a replica reports a
// device larger than the metadata arithmetic alone leaves room for, the
// lower disk grows to hold it.
func TestNewLowerDiskHoldsLargestReportedDevice(t *testing.T) {
	t.Parallel()

	device := int64(adoptedVolumeKiB + 2048)

	zvol := createdZvolKiB(t, &intent.DesiredVolume{
		VolumeNumber:  0,
		SizeKib:       adoptedVolumeKiB,
		StoragePool:   "data",
		DeviceSizeKib: device,
	}, nil)

	data := drbd.InternalMetadataNetKiB(zvol, drbd.GeometryForPeerSlots(drbd.LegacyPeerSlots))
	if data < device {
		t.Fatalf("zvol of %d KiB holds a %d KiB data area, below the reported %d KiB device", zvol, data, device)
	}
}

// TestNewLowerDiskWithoutInternalMetadata: a volume without DRBD keeps
// the definition's size.
func TestNewLowerDiskWithoutInternalMetadata(t *testing.T) {
	t.Parallel()

	storageOnly := createdZvolKiB(t, &intent.DesiredVolume{
		VolumeNumber: 0, SizeKib: adoptedVolumeKiB, StoragePool: "data", DeviceSizeKib: adoptedDeviceKiB,
	}, []string{"STORAGE"})
	if storageOnly != adoptedVolumeKiB {
		t.Errorf("storage-only volume: got %d KiB, want the definition's %d", storageOnly, adoptedVolumeKiB)
	}
}

// TestNewLowerDiskHoldsAdoptedLUKSDevice is the same toggle-disk on a
// LUKS volume adopted from LINSTOR. LINSTOR put a 16 MiB LUKS2 header
// under the DRBD lower disk, so its storage volume is 20992432 KiB and
// the DRBD device again 41943056 sectors. The new storage volume must
// hold the header and a DRBD lower disk holding that device; without
// the header it is 16 MiB short.
func TestNewLowerDiskHoldsAdoptedLUKSDevice(t *testing.T) {
	t.Parallel()

	zvol := createdZvolKiB(t, &intent.DesiredVolume{
		VolumeNumber:  0,
		SizeKib:       adoptedVolumeKiB,
		StoragePool:   "data",
		DeviceSizeKib: adoptedDeviceKiB,
	}, []string{"DRBD", "LUKS", "STORAGE"})

	data := drbd.InternalMetadataNetKiB(zvol-luks.HeaderKiB, drbd.GeometryForPeerSlots(drbd.LegacyPeerSlots))
	if data < adoptedDeviceKiB {
		t.Fatalf("zvol of %d KiB holds a %d KiB data area under the LUKS header, below the %d KiB device",
			zvol, data, adoptedDeviceKiB)
	}

	if zvol != 20506*1024 {
		t.Errorf("zvol: got %d KiB, want 20506 MiB", zvol)
	}
}

// TestNewLowerDiskLUKSWithoutDRBD: a LUKS volume without DRBD gets the
// header on top of the definition, as LINSTOR sizes it.
func TestNewLowerDiskLUKSWithoutDRBD(t *testing.T) {
	t.Parallel()

	zvol := createdZvolKiB(t, &intent.DesiredVolume{
		VolumeNumber: 0, SizeKib: adoptedVolumeKiB, StoragePool: "data",
	}, []string{"LUKS", "STORAGE"})
	if zvol != adoptedVolumeKiB+luks.HeaderKiB {
		t.Errorf("zvol: got %d KiB, want the definition plus the 16 MiB header", zvol)
	}
}

// TestNewLowerDiskIsRoundedToTheVolblocksize: a new lower disk is the
// data plus its metadata rounded up to the pool's volblocksize, as
// LINSTOR sizes it, not to whole MiB. On a 16 KiB volblocksize a 1 GiB
// volume with 7 peer slots needs 1048836 KiB and gets 1048848 KiB, the
// 1074020352 B zvol LINSTOR creates for it; rounded to MiB it got
// 1049600 KiB, and a LINSTOR replica added after a return to LINSTOR,
// sized the LINSTOR way, could not attach under the larger device the
// other replicas then exposed. A replica with no recorded count is
// sized for the 15 slots it was created with.
func TestNewLowerDiskIsRoundedToTheVolblocksize(t *testing.T) {
	t.Parallel()

	const gib = 1024 * 1024

	seven := int32(drbd.DefaultPeerSlots)

	cases := []struct {
		name      string
		peerSlots *int32
		want      int64
	}{
		{"7 peer slots", &seven, 1048848},
		{"no recorded count", nil, 1049104},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			zvol := createdZvolKiBWithPeerSlots(t, &intent.DesiredVolume{VolumeNumber: 0, SizeKib: gib, StoragePool: "data"},
				nil, tc.peerSlots, "create\tdata/blockstor-volblocksize-probe\nproperty\tvolsize\t16384\n")

			if zvol != tc.want {
				t.Errorf("zvol: got %d KiB, want %d KiB on a 16 KiB volblocksize", zvol, tc.want)
			}
		})
	}
}
