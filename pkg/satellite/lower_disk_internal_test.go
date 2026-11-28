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
	"fmt"
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
// for metadata are not resized by an upgrade alone, nor by a replica
// added to them later. The grow step is the satellite's (growBacking),
// with DRBD on for the volumes that have it. applied is the size the
// replica last reported applied, 0 for one that never reported.
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
		onDisk, applied    int64
		wantResized        bool
		wantStorageGrow    bool
	}{
		{"unchanged volume sized for metadata", gib20, 20972116, sizedDisk, gib20, false, false},
		{"volume adopted from LINSTOR", gib20, adoptedDev, 20976048, gib20, false, false},
		{"volume created at the definition's size", gib20, 20961884, gib20, gib20, false, false},
		{"small grow the lower disk already holds", gib20 + 100, 20972116 - 1000, sizedDisk, gib20, true, false},
		{"small grow past the data the lower disk holds", gib20 + 4096, gib20, sizedDisk, gib20, true, true},
		{"grow past the lower disk", 21 * 1024 * 1024, 20972116, sizedDisk, gib20, true, true},
		// A replica added to a volume whose other backings are exactly
		// the definition: its own backing is sized for metadata, the
		// device stays short of the definition, and nothing resized the
		// definition. LINSTOR leaves the device alone, before and after
		// the replica first reports its size.
		{"new replica of a volume created at the definition's size", gib20, 20961884, sizedDisk, 0, false, false},
		{"replica of a volume created at the definition's size", gib20, 20961884, sizedDisk, gib20, false, false},
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
			vol := &intent.DesiredVolume{SizeKib: tc.definition, AppliedSizeKib: gib20}

			r := NewReconciler(ReconcilerConfig{})

			resized, err := r.growBacking(t.Context(), provider, dr, vol, storage.VolumeStatus{UsableKib: tc.onDisk}, nil)
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
			vol := &intent.DesiredVolume{SizeKib: tc.definition, DeviceSizeKib: tc.device, AppliedSizeKib: tc.applied}

			r := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(storage.NewFakeExec())})

			resized, err := r.growBacking(t.Context(), provider, dr, vol, storage.VolumeStatus{UsableKib: tc.onDisk},
				newDisklessProbe(r.cfg.Adm, dr.GetName()))
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

	geometry := drbd.GeometryForPeerSlots(drbd.DefaultPeerSlots)

	storage := drbd.InternalMetadataGrossKiB(20971520, geometry) + luks.HeaderKiB
	storage = (storage + 15) / 16 * 16

	if storage != 20992432 {
		t.Fatalf("storage volume: got %d KiB, want 20992432", storage)
	}

	if dev := drbd.InternalMetadataNetKiB(storage-luks.HeaderKiB, geometry); dev*2 != 41943056 {
		t.Fatalf("DRBD device: got %d sectors, want 41943056", dev*2)
	}

	seven := int32(drbd.DefaultPeerSlots)

	got := lowerDiskKib(&intent.DesiredResource{LayerStack: []string{"DRBD", "LUKS", "STORAGE"}, PeerSlots: &seven},
		&intent.DesiredVolume{SizeKib: 20971520})
	if got != 20976040+luks.HeaderKiB {
		t.Fatalf("lowerDiskKib under LUKS with 7 peer slots: got %d, want LINSTOR's %d before the volblocksize",
			got, 20976040+luks.HeaderKiB)
	}

	got = lowerDiskKib(&intent.DesiredResource{LayerStack: []string{"DRBD", "LUKS", "STORAGE"}},
		&intent.DesiredVolume{SizeKib: 20971520, DeviceSizeKib: 20971528})
	if want := drbd.InternalMetadataGrossKiB(20971528, drbd.GeometryForPeerSlots(drbd.LegacyPeerSlots)) + luks.HeaderKiB; got != want {
		t.Fatalf("lowerDiskKib under LUKS without a recorded count: got %d, want %d", got, want)
	}
}

// TestGrowBackingBeforeAttach: a backing an older satellite created at
// the definition's size for a volume adopted from LINSTOR cannot hold
// the device the replicas report, and the kernel refuses to attach it.
// The satellite grows it to what a new backing gets while the kernel
// holds the volume Diskless, without resizing the device, and leaves it
// alone when the kernel holds it attached or does not hold it at all.
// The kernel is asked only when the sizes call for it, and a LUKS header
// is read off the backing rather than assumed.
func TestGrowBackingBeforeAttach(t *testing.T) {
	t.Parallel()

	const (
		gib20      = 20971520
		adoptedDev = 20971528
		oldDev     = 20961884 // device over backings of exactly the definition
		devicePath = "/dev/zvol/data/pvc-a_00000"
	)

	status := func(diskState string) []byte {
		return []byte(`[{"name":"pvc-a","node-id":1,"role":"Secondary","devices":[` +
			`{"volume":0,"minor":1000,"disk-state":"` + diskState + `"}],"connections":[]}]`)
	}

	luksDump := func(offset string) []byte {
		return []byte("Version: 2\nData segments:\n  0: crypt\n\toffset: " + offset + " [bytes]\n")
	}

	luks1Dump := []byte("Version: 1\nPayload offset:\t4096\n")

	drbdOnly := []string{"DRBD", "STORAGE"}
	withLUKS := []string{"DRBD", "LUKS", "STORAGE"}

	cases := []struct {
		name      string
		stack     []string
		device    int64
		onDisk    int64
		kernel    []byte
		dump      []byte
		wantProbe bool
		wantGrow  bool
	}{
		{"attach refused: backing at the definition under an adopted device", drbdOnly, adoptedDev, gib20, status("Diskless"), nil, true, true},
		{"backing at the definition attached", drbdOnly, adoptedDev, gib20, status("UpToDate"), nil, true, false},
		{"backing at the definition mid-attach", drbdOnly, adoptedDev, gib20, status("Attaching"), nil, true, false},
		{"resource the kernel does not hold", drbdOnly, adoptedDev, gib20, nil, nil, true, false},
		{"backing that holds the device", drbdOnly, oldDev, gib20, status("Diskless"), nil, false, false},
		{"backing sized for metadata", drbdOnly, adoptedDev, 20981760, status("Diskless"), nil, false, false},
		// LINSTOR sized it for its seven peer slots: fewer than the
		// fifteen blockstor creates metadata with, and still attachable.
		{"LINSTOR backing not attached yet", drbdOnly, adoptedDev, 20976048, status("Diskless"), nil, false, false},
		// LUKS: a backing at the definition has no room for the header
		// either; one with the header and no room for metadata still
		// cannot hold the device.
		{"LUKS backing at the definition", withLUKS, adoptedDev, gib20, status("Diskless"), luksDump("16777216"), true, true},
		{"LUKS backing with the header only", withLUKS, adoptedDev, gib20 + luks.HeaderKiB, status("Diskless"), luksDump("16777216"), true, true},
		{"LUKS backing sized for metadata", withLUKS, adoptedDev, 20981760 + luks.HeaderKiB, status("Diskless"), luksDump("16777216"), false, false},
		// LINSTOR sized it under a 2 MiB LUKS1 header: assuming the
		// 16 MiB a LUKS2 header takes would find it too small.
		{"LINSTOR backing under a LUKS1 header", withLUKS, adoptedDev, 20976048 + 2048, status("Diskless"), luks1Dump, false, false},
		{"LINSTOR backing under a 4 MiB LUKS2 header", withLUKS, adoptedDev, 20976048 + 4096, status("Diskless"), luksDump("4194304"), false, false},
		{"LUKS header that cannot be read", withLUKS, adoptedDev, 20976048 + 2048, status("Diskless"), nil, true, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			storageExec := storage.NewFakeExec()
			provider := zfs.NewProvider(zfs.Config{Pool: "data", Thin: true}, storageExec)

			drbdExec := storage.NewFakeExec()
			if tc.kernel != nil {
				drbdExec.Expect("drbdsetup status pvc-a --json", storage.FakeResponse{Stdout: tc.kernel})
			} else {
				drbdExec.Expect("drbdsetup status pvc-a --json", storage.FakeResponse{Err: storage.FakeExitError{Code: 10}})
			}

			cryptExec := storage.NewFakeExec()
			if tc.dump != nil {
				cryptExec.Expect("cryptsetup luksDump "+devicePath, storage.FakeResponse{Stdout: tc.dump})
			} else {
				cryptExec.Expect("cryptsetup luksDump "+devicePath, storage.FakeResponse{Err: storage.FakeExitError{Code: 1}})
			}

			dr := &intent.DesiredResource{Name: "pvc-a", LayerStack: tc.stack}
			vol := &intent.DesiredVolume{SizeKib: gib20, DeviceSizeKib: tc.device, AppliedSizeKib: gib20}

			r := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(drbdExec), Cryptsetup: luks.NewCryptsetup(cryptExec)})

			resized, err := r.growBacking(t.Context(), provider, dr, vol,
				storage.VolumeStatus{DevicePath: devicePath, UsableKib: tc.onDisk}, newDisklessProbe(r.cfg.Adm, dr.GetName()))
			if err != nil {
				t.Fatalf("growBacking: %v", err)
			}

			if resized {
				t.Errorf("resized = true: growing a backing to attach it must not resize the device")
			}

			if probed := len(drbdExec.CommandLines()) > 0; probed != tc.wantProbe {
				t.Errorf("kernel asked = %v, want %v (%v)", probed, tc.wantProbe, drbdExec.CommandLines())
			}

			want := ""
			if tc.wantGrow {
				want = fmt.Sprintf("zfs set volsize=%dK data/pvc-a_00000", alignUp(growBackingKib(dr, vol, true, r.grownPeerSlots(dr, vol), r.luksHeaderKib(t.Context(), dr, 0, devicePath)), 1024))
			}

			got := ""

			for _, line := range storageExec.CommandLines() {
				if strings.HasPrefix(line, "zfs set volsize=") {
					got = line
				}
			}

			if got != want {
				t.Errorf("storage grow = %q, want %q (%v)", got, want, storageExec.CommandLines())
			}
		})
	}
}

// TestDisklessProbeAsksOncePerPass: every volume of a resource is
// answered by one `drbdsetup status`, so a pass over several volumes
// with backings too small for the device asks the kernel once.
func TestDisklessProbeAsksOncePerPass(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status pvc-a --json", storage.FakeResponse{Stdout: []byte(
		`[{"name":"pvc-a","devices":[{"volume":0,"disk-state":"Diskless"},{"volume":1,"disk-state":"Diskless"}]}]`)})

	probe := newDisklessProbe(drbd.NewAdm(fx), "pvc-a")

	if !probe.holds(t.Context(), 0) || !probe.holds(t.Context(), 1) || probe.holds(t.Context(), 2) {
		t.Fatalf("volumes 0 and 1 are Diskless, 2 is not held")
	}

	if calls := fx.CommandLines(); len(calls) != 1 {
		t.Fatalf("kernel asked %d times, want once: %v", len(calls), calls)
	}
}
