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

package lvm_test

import (
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// Both LVM providers tell whether the volume group holds a snapshot LV, so a
// satellite can record which pool holds a copy taken before it recorded one.
func TestLVMHasSnapshot(t *testing.T) {
	t.Parallel()

	snap := storage.Snapshot{ResourceName: "pvc-1", SnapshotName: "snap-1"}
	probe := "lvs --config " + lvm.ConfigFilter + " --noheadings -o lv_name vg/pvc-1_snap-1_00000"

	for _, held := range []bool{true, false} {
		fx := storage.NewFakeExec()
		if held {
			fx.Expect(probe, storage.FakeResponse{Stdout: []byte("  pvc-1_snap-1_00000\n")})
		}

		for name, prober := range map[string]storage.SnapshotProber{
			"thin":  lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx),
			"thick": lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx),
		} {
			has, err := prober.HasSnapshot(t.Context(), snap)
			if err != nil || has != held {
				t.Errorf("%s HasSnapshot = %v, %v; want %v", name, has, err, held)
			}
		}
	}
}
