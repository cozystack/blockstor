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

	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// Issue 264: a volume linstor-migrate hands over, or one an older
// blockstor created, has no ownership marker. The first reconcile of
// the Resource that claims it must add the marker, so that if the
// Resource is later lost the volume is reapable as blockstor's own,
// and must not create anything next to it.
func TestApplyMarksAnAdoptedVolume(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		provider func(storage.Exec) storage.Provider
		wire     func(*storage.FakeExec)
		mark     string
		create   string
	}{
		"zfs": {
			provider: func(ex storage.Exec) storage.Provider {
				return zfs.NewProvider(zfs.Config{Pool: "tank", Thin: true}, ex)
			},
			wire: func(fx *storage.FakeExec) {
				fx.Expect("zfs list -H -o name tank/pvc-adopted_00000",
					storage.FakeResponse{Stdout: []byte("tank/pvc-adopted_00000\n")})
				fx.Expect("zfs get -H -o value,source blockstor.org:owner tank/pvc-adopted_00000",
					storage.FakeResponse{Stdout: []byte("-\t-\n")})
				fx.Expect("zfs list -H -p -o name,volsize,used tank/pvc-adopted_00000",
					storage.FakeResponse{Stdout: []byte("tank/pvc-adopted_00000\t1073741824\t4096\n")})
			},
			mark:   "zfs set blockstor.org:owner=blockstor tank/pvc-adopted_00000",
			create: "zfs create",
		},
		"lvm-thin": {
			provider: func(ex storage.Exec) storage.Provider {
				return lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, ex)
			},
			wire: func(fx *storage.FakeExec) {
				fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings -o lv_name vg/pvc-adopted_00000",
					storage.FakeResponse{Stdout: []byte("pvc-adopted_00000\n")})
			},
			mark:   "lvchange --config " + lvm.ConfigFilter + " --addtag blockstor-owned vg/pvc-adopted_00000",
			create: "lvcreate",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			tc.wire(fx)

			rec := satellite.NewReconciler(satellite.ReconcilerConfig{
				Providers: map[string]storage.Provider{"pool": tc.provider(fx)},
			})

			_, err := rec.Apply(t.Context(), []*intent.DesiredResource{{
				Name:     "pvc-adopted",
				NodeName: "n1",
				Volumes: []*intent.DesiredVolume{
					{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "pool"},
				},
			}})
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}

			if !slices.Contains(fx.CommandLines(), tc.mark) {
				t.Errorf("adopted volume not marked; calls=%v", fx.CommandLines())
			}

			for _, line := range fx.CommandLines() {
				if strings.HasPrefix(line, tc.create) {
					t.Errorf("adopted volume must not be re-created: %q", line)
				}
			}
		})
	}
}
