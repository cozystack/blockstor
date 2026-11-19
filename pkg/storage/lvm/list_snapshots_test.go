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
	"slices"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// A thin snapshot is a thin volume to lvs, and `pvc-a_s1_00000` parses
// as volume 0 of a resource `pvc-a_s1` no Resource is named after, so
// listed it reads as a leak of its own. The listing must leave out the
// snapshots CreateSnapshot takes and keep a volume restored from one.
func TestListVolumeNamesLeavesOutSnapshots(t *testing.T) {
	t.Parallel()

	const listing = "lvs --config " + lvm.ConfigFilter + " --noheadings -o lv_name,lv_attr,origin --separator \t vg"

	rows := "" +
		"  pvc-a_00000\tVwi-aotz--\n" +
		"  pvc-a_s1_00000\tVwi---tz-k\tpvc-a_00000\n" + // thin snapshot of pvc-a
		"  pvc-b_00000\tVwi-aotz--\tpvc-a_s1_00000\n" + // restored from it
		"  pvc-c_00000\t-wi-ao----\n" +
		"  pvc-c_s1_00000\tswi-a-s---\tpvc-c_00000\n" + // thick snapshot
		"  tp\ttwi-aotz--\n"

	want := []storage.VolumeRef{
		{ResourceName: "pvc-a", VolumeNumber: 0},
		{ResourceName: "pvc-b", VolumeNumber: 0},
		{ResourceName: "pvc-c", VolumeNumber: 0},
	}

	listers := map[string]func(storage.Exec) storage.VolumeLister{
		"thin": func(ex storage.Exec) storage.VolumeLister {
			return lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, ex)
		},
		"thick": func(ex storage.Exec) storage.VolumeLister {
			return lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, ex)
		},
	}

	for kind, newLister := range listers {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			fx.Expect(listing, storage.FakeResponse{Stdout: []byte(rows)})

			lister := newLister(fx)

			got, err := lister.ListVolumeNames(t.Context())
			if err != nil {
				t.Fatalf("ListVolumeNames: %v", err)
			}

			if !slices.Equal(got, want) {
				t.Errorf("got %+v, want %+v", got, want)
			}
		})
	}
}
