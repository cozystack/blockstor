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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// The sizes of the restore that split: a 64 MiB snapshot restored into
// a 256 MiB volume definition, with the 7 peer slots LINSTOR gives a new
// definition.
const (
	restoredSnapshotKib  = 64 << 10
	restoredDefinitionKb = 256 << 10
	restoredPeerSlots    = int32(7)
)

func restoredReplica(stateDir string) (*Reconciler, *intent.DesiredResource, *intent.DesiredVolume) {
	slots := restoredPeerSlots
	dr := &intent.DesiredResource{Name: "pvc-r", PeerSlots: &slots}
	vol := &intent.DesiredVolume{
		VolumeNumber:   0,
		SizeKib:        restoredDefinitionKb,
		SourceSnapshot: "src:snap",
		StoragePool:    "data",
	}

	return NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(storage.NewFakeExec()), StateDir: stateDir}), dr, vol
}

// zfsGrowTo is the volsize a zfs grow of pvc-r's volume 0 sets for kib
// on the 16 KiB volblocksize the split was seen on: fine enough for the
// 64 KiB a grow for 15 slots adds on 256 MiB to show.
func zfsGrowTo(kib int64) string {
	return fmt.Sprintf("zfs set volsize=%dK data/pvc-r_00000", alignUp(kib, 16))
}

func lastZfsGrow(fx *storage.FakeExec) string {
	got := ""

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "zfs set volsize=") {
			got = line
		}
	}

	return got
}

// TestRestoredReplicaGrowsToItsBlankSiblingsSize: a replica restored
// from a snapshot smaller than its definition grows its clone before its
// first activation creates the metadata on it, in the replica's own
// layout. Sized for 15 slots instead of the 7 create-md then writes, its
// device came out 64 KiB larger than the definition on 256 MiB, larger
// than the blank replica beside it, and DRBD refused that replica every
// second for as long as the volume lived. Grown for its own slots it
// gets the lower disk a blank replica of the definition gets.
func TestRestoredReplicaGrowsToItsBlankSiblingsSize(t *testing.T) {
	t.Parallel()

	r, dr, vol := restoredReplica(t.TempDir())

	fx := storage.NewFakeExec()
	fx.Expect("zfs get -Hp -o value volblocksize data/pvc-r_00000", storage.FakeResponse{Stdout: []byte("16384\n")})
	provider := zfs.NewProvider(zfs.Config{Pool: "data", Thin: true}, fx)

	clone := drbdGrossKib(restoredSnapshotKib, int(restoredPeerSlots))

	_, err := r.growBacking(t.Context(), provider, dr, vol,
		storage.VolumeStatus{DevicePath: "/dev/zvol/data/pvc-r_00000", UsableKib: clone}, nil)
	if err != nil {
		t.Fatalf("growBacking: %v", err)
	}

	blank := lowerDiskKib(dr, vol)

	if got, want := lastZfsGrow(fx), zfsGrowTo(blank); got != want {
		t.Errorf("restored clone grown with %q, want %q: the lower disk a blank replica of the definition gets", got, want)
	}

	if device := drbd.InternalMetadataNetKiB(blank, drbd.GeometryForPeerSlots(int(restoredPeerSlots))); device != restoredDefinitionKb {
		t.Errorf("device on that lower disk = %d KiB, want the definition's %d", device, restoredDefinitionKb)
	}
}

// TestGrowAfterCreateMDUsesTheSlotsItWrote: a replica whose metadata this
// satellite created, grown before the count it read is published, sizes
// the grow for the slots it wrote rather than for 15.
func TestGrowAfterCreateMDUsesTheSlotsItWrote(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()

	err := os.WriteFile(filepath.Join(stateDir, "pvc-r.md-created"), nil, 0o600)
	if err != nil {
		t.Fatal(err)
	}

	r, dr, vol := restoredReplica(stateDir)
	vol.SourceSnapshot = ""

	if got := r.grownPeerSlots(dr, vol); got != drbd.LegacyPeerSlots {
		t.Fatalf("metadata neither created nor read here: %d slots, want the %d floor", got, drbd.LegacyPeerSlots)
	}

	err = r.createMDWithCollisionRecovery(t.Context(), "pvc-r", "pvc-r/0", drbd.GeometryForPeerSlots(int(restoredPeerSlots)))
	if err != nil {
		t.Fatalf("create-md: %v", err)
	}

	if got := r.grownPeerSlots(dr, vol); got != int(restoredPeerSlots) {
		t.Errorf("after create-md wrote %d slots the grow sizes for %d", restoredPeerSlots, got)
	}

	r.forgetRestoreRecord("pvc-r", 0)

	if got := r.grownPeerSlots(dr, vol); got != drbd.LegacyPeerSlots {
		t.Errorf("a deleted volume's count outlived it: %d slots", got)
	}
}

// TestSuperblockCountOfMetadataCreatedHereIsNotRead: the count of a
// superblock this satellite created is the one it wrote; reading it
// back with dump-md on every pass until the count is published is a
// read of a disk the satellite just wrote.
func TestSuperblockCountOfMetadataCreatedHereIsNotRead(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	slots := restoredPeerSlots
	dr := &intent.DesiredResource{
		Name:                  "pvc-r",
		PeerSlots:             &slots,
		ReadMetadataPeerSlots: true,
		Volumes:               []*intent.DesiredVolume{{VolumeNumber: 0, SizeKib: restoredDefinitionKb}},
	}

	r := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), StateDir: t.TempDir()})
	r.noteMetadataSlots("pvc-r/0", int(restoredPeerSlots))

	res := &intent.ResourceApplyResult{Ok: true}

	for range 2 {
		if got := r.superblockPeerSlots(t.Context(), dr, res); got != restoredPeerSlots {
			t.Fatalf("reported %d slots, want the %d create-md wrote", got, restoredPeerSlots)
		}
	}

	for _, line := range fx.CommandLines() {
		if strings.Contains(line, "dump-md") {
			t.Errorf("read the superblock it created: %q", line)
		}
	}
}

// TestGrowAfterToggleUsesTheSlotsCreateMDWrote: a replica an older
// satellite created with 15 while it was recorded with 7 publishes 15.
// After a toggle to diskless and back, create-md writes the 7 it is
// recorded with; the grow must size for those 7, not for the 15 the
// status still held, or the device comes out larger than the
// definition.
func TestGrowAfterToggleUsesTheSlotsCreateMDWrote(t *testing.T) {
	t.Parallel()

	r, dr, vol := restoredReplica(t.TempDir())
	vol.SourceSnapshot = ""

	stale := int32(drbd.LegacyPeerSlots)
	dr.MetadataPeerSlots = &stale

	r.noteMetadataSlots("pvc-r/0", int(restoredPeerSlots))

	if got := r.grownPeerSlots(dr, vol); got != int(restoredPeerSlots) {
		t.Errorf("grow sized for %d slots, want the %d create-md wrote", got, restoredPeerSlots)
	}
}
