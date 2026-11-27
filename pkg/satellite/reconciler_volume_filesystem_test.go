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
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// volumeFileSystemDR is a fresh two-volume replica whose filesystem
// request lives on the volumes alone, the way linstor-csi v1.13.2
// stamps FileSystem/* on the volume definition: nothing in the
// resource's Props asks for a filesystem.
func volumeFileSystemDR(rd string, vol0, vol1 map[string]string) *intent.DesiredResource {
	return &intent.DesiredResource{
		Name:     rd,
		NodeName: "n1",
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 131072, StoragePool: "thin1", FileSystem: vol0},
			{VolumeNumber: 1, SizeKib: 131072, StoragePool: "thin1", FileSystem: vol1},
		},
		SkipInitialSync: skipInitTrue(),
		DrbdOptions: map[string]string{
			"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "8100",
			"auto-primary": "true",
		},
	}
}

func applyVolumeFileSystemDR(t *testing.T, dr *intent.DesiredResource, blank ...string) []string {
	t.Helper()

	fx := storage.NewFakeExec()
	expectBlankMkfsTarget(fx, blank...)

	for _, vol := range []string{"_00000", "_00001"} {
		fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/"+dr.Name+vol,
			storage.FakeResponse{Stdout: []byte("")})
	}

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		Exec:      fx,
		StateDir:  t.TempDir(),
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{dr})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	return slices.DeleteFunc(fx.CommandLines(), func(line string) bool {
		return !strings.HasPrefix(line, "mkfs.")
	})
}

// TestApplyAutoMkfsReadsTheVolumesOwnFileSystem: each volume is
// formatted with its own type and parameters, and a volume without a
// type (a block member next to a filesystem one) is left alone, as
// LINSTOR's MkfsUtils.makeFileSystemOnMarked does per volume. Before
// the fix only the resource's Props were read, so a definition whose
// volumes asked for a filesystem got none and the mount failed with
// "wrong fs type".
func TestApplyAutoMkfsReadsTheVolumesOwnFileSystem(t *testing.T) {
	t.Run("filesystem member next to a block member", func(t *testing.T) {
		dr := volumeFileSystemDR("pvc-vdfs-a",
			map[string]string{"FileSystem/Type": "ext4", "FileSystem/MkfsParams": "-b 4096"}, nil)

		got := applyVolumeFileSystemDR(t, dr, "/dev/drbd8100")

		want := []string{"mkfs.ext4 -b 4096 /dev/drbd8100"}
		if !slices.Equal(got, want) {
			t.Errorf("mkfs: got %q, want %q", got, want)
		}
	})

	t.Run("two volumes, two types", func(t *testing.T) {
		dr := volumeFileSystemDR("pvc-vdfs-b",
			map[string]string{"FileSystem/Type": "ext4", "FileSystem/MkfsParams": ""},
			map[string]string{"FileSystem/Type": "xfs"})

		got := applyVolumeFileSystemDR(t, dr, "/dev/drbd8100", "/dev/drbd8101")

		want := []string{"mkfs.ext4 /dev/drbd8100", "mkfs.xfs /dev/drbd8101"}
		if !slices.Equal(got, want) {
			t.Errorf("mkfs: got %q, want %q", got, want)
		}
	})
}
