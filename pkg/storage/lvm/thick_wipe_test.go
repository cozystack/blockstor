// SPDX-License-Identifier: Apache-2.0

package lvm_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

const (
	lvsVG      = "lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg"
	lvsFinal   = "lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_00000"
	lvsFresh   = "lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_00000.bsnew"
	wipeFresh  = "wipefs --all --force /dev/vg/pvc-1_00000.bsnew"
	wipeFinal  = "wipefs --all --force /dev/vg/pvc-1_00000"
	renameLV   = "lvrename --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } activation { udev_sync=0 udev_rules=0 } vg pvc-1_00000.bsnew pvc-1_00000"
	lvremove   = "lvremove --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --force vg/pvc-1_00000"
	lvcreateLV = "lvcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } activation { udev_sync=0 udev_rules=0 } --size 1024MiB --name pvc-1_00000.bsnew -Wn -Zn vg"
)

var (
	errWipeBusy = errors.New("wipefs: device busy")
	errLvsLock  = errors.New("lvs: lock timeout")
	errRename   = errors.New("lvrename: busy")
	errInUse    = errors.New("logical volume vg/pvc-1_00000 in use")
)

func thickVol() storage.Volume {
	return storage.Volume{ResourceName: "pvc-1", VolumeNumber: 0, SizeKib: 1024 * 1024}
}

func create(t *testing.T, fx *storage.FakeExec) error {
	t.Helper()

	return lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx).CreateVolume(t.Context(), thickVol()) //nolint:wrapcheck // test helper
}

// A thick LV is allocated without zeroing, so it starts with what its
// extents held. It is created under a temporary name, wiped there, and only
// then renamed to the volume's, so no stale LUKS header or filesystem is
// ever found under a volume's name.
func TestThickCreateVolumeWipesTheFreshLVBeforeItTakesTheVolumesName(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(lvsVG, storage.FakeResponse{Stdout: []byte("  other_00000\n")})

	if err := create(t, fx); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	lines := fx.CommandLines()
	c, w, r := slices.Index(lines, lvcreateLV), slices.Index(lines, wipeFresh), slices.Index(lines, renameLV)

	if c < 0 || w < 0 || r < 0 || c >= w || w >= r {
		t.Errorf("want lvcreate of the temporary LV, then wipefs, then lvrename; got %v", lines)
	}
}

// An LV under the volume's name is the volume. It is never wiped, whatever
// it holds.
func TestThickCreateVolumeLeavesAnLVUnderTheVolumesNameAlone(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(lvsVG, storage.FakeResponse{Stdout: []byte("  pvc-1_00000\n")})

	if err := create(t, fx); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	for _, l := range fx.CommandLines() {
		if l == wipeFinal || l == wipeFresh || l == lvcreateLV || l == renameLV {
			t.Errorf("touched an existing volume: %v", fx.CommandLines())
		}
	}
}

// A crash between the create and the rename leaves the temporary LV. It was
// never exposed under the volume's name, so the next call removes it and
// creates the LV again at the volume's current size, which the leftover
// need not have.
func TestThickCreateVolumeReplacesATemporaryLVACrashLeft(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(lvsVG, storage.FakeResponse{Stdout: []byte("  pvc-1_00000.bsnew\n")})

	if err := create(t, fx); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	lines := fx.CommandLines()
	rm := slices.Index(lines, "lvremove --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --force vg/pvc-1_00000.bsnew")
	c, w, r := slices.Index(lines, lvcreateLV), slices.Index(lines, wipeFresh), slices.Index(lines, renameLV)

	if rm < 0 || c < rm || w < c || r < w {
		t.Errorf("want the leftover removed, then a new LV created, wiped and renamed; got %v", lines)
	}
}

// A listing that fails says nothing about whether the volume exists, so the
// create stops rather than assume either way.
func TestThickCreateVolumeStopsWhenTheListingFails(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(lvsVG, storage.FakeResponse{Err: errLvsLock})

	if err := create(t, fx); err == nil {
		t.Fatal("CreateVolume succeeded over a failed listing")
	}

	if len(fx.CommandLines()) != 1 {
		t.Errorf("ran more than the listing: %v", fx.CommandLines())
	}
}

// A wipe that fails leaves the LV under its temporary name, so it is not
// handed on, and the next attempt wipes again.
func TestThickCreateVolumeDoesNotRenameOverAFailedWipe(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(lvsVG, storage.FakeResponse{Stdout: []byte("")})
	fx.Expect(wipeFresh, storage.FakeResponse{Err: errWipeBusy})

	if err := create(t, fx); err == nil {
		t.Fatal("CreateVolume succeeded over a failed wipe")
	}

	if slices.Contains(fx.CommandLines(), renameLV) {
		t.Errorf("renamed an LV whose wipe failed: %v", fx.CommandLines())
	}
}

// A rename that fails is an error: the volume does not exist under its name.
func TestThickCreateVolumeFailsOnAFailedRename(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(lvsVG, storage.FakeResponse{Stdout: []byte("")})
	fx.Expect(renameLV, storage.FakeResponse{Err: errRename})

	if err := create(t, fx); err == nil {
		t.Fatal("CreateVolume succeeded over a failed rename")
	}
}

// Delete wipes nothing. lvremove refuses an LV that is still open, and that
// LV has to keep its data; the next CreateVolume over these extents wipes
// them anyway.
func TestThickDeleteVolumeWipesNothingEvenWhenTheLVIsInUse(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(lvsFinal, storage.FakeResponse{Stdout: []byte("pvc-1_00000\n")})
	fx.Expect(lvremove, storage.FakeResponse{Err: errInUse})

	err := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx).DeleteVolume(t.Context(), thickVol())
	if err == nil {
		t.Fatal("DeleteVolume succeeded over a refused lvremove")
	}

	for _, l := range fx.CommandLines() {
		if len(l) >= 6 && l[:6] == "wipefs" {
			t.Errorf("wiped an LV on delete: %v", fx.CommandLines())
		}
	}
}

// Delete also removes a temporary LV a crashed create left for this volume,
// so its extents go back to the VG. The positive control for the test above:
// a removable LV is removed.
func TestThickDeleteVolumeRemovesALeftoverTemporaryLV(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(lvsFinal, storage.FakeResponse{Stdout: []byte("pvc-1_00000\n")})
	fx.Expect(lvsFresh, storage.FakeResponse{Stdout: []byte("pvc-1_00000.bsnew\n")})

	if err := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx).DeleteVolume(t.Context(), thickVol()); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	lines := fx.CommandLines()
	if !slices.Contains(lines, lvremove) ||
		!slices.Contains(lines, "lvremove --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --force vg/pvc-1_00000.bsnew") {
		t.Errorf("want both LVs removed; got %v", lines)
	}
}
