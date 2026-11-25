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
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/luks"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// errNoMetadata is drbdmeta finding no superblock where it looks: at the
// end of a backing that grew after its metadata was written.
var errNoMetadata = errors.New("drbdadm: No valid meta data found")

// A replica of a volume adopted from LINSTOR whose backing an older
// satellite created too small: the kernel holds it Diskless and refuses
// the attach. In one pass the backing grows, the metadata the grow left
// behind its end is written anew, and the disk is attached; the replica
// then syncs from its peers, as no GI is seeded for a replica joining a
// volume that holds data. The device keeps its size, so no `drbdadm
// resize` runs: it would fail while the replica syncs and later grow a
// volume nobody resized.
//
// Under LUKS the open mapper, which does not follow its backing on its
// own, grows with it, for both backings an older satellite left: one at
// the definition, with no room for the header either, whose mapper lags
// the grown backing by more than the 32 MiB the convergence path
// tolerates, so that path grew it and asked DRBD for the resize; and one
// with the header and no room for metadata, whose mapper lags it by
// less, so nothing grew it and the attach was refused for good.
func TestGrowBeforeAttachAttachesWithoutResizingTheDevice(t *testing.T) {
	t.Parallel()

	const (
		gib20      = 20971520
		adoptedDev = 20971528
		mapperPath = "/dev/mapper/pvc-key-0-luks"
	)

	diskless := storage.FakeResponse{Stdout: []byte(`[{"name":"` + keyRD + `","node-id":1,"role":"Secondary",` +
		`"devices":[{"volume":0,"minor":1000,"disk-state":"Diskless"}],"connections":[]}]`)}

	cases := []struct {
		name   string
		luks   bool
		onDisk int64
	}{
		{"backing at the definition", false, gib20},
		{"LUKS backing at the definition", true, gib20},
		{"LUKS backing with the header only", true, gib20 + luks.HeaderKiB},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			stack := []string{"DRBD", "STORAGE"}
			header := int64(0)

			if tc.luks {
				stack = []string{"DRBD", "LUKS", "STORAGE"}
				header = luks.HeaderKiB
			}

			// The size the backing grows to: what a new one is created
			// at for the adopted device, which LVM rounds to an extent.
			grownKib := drbd.InternalMetadataGrossKiB(adoptedDev, drbd.DefaultGeometry()) + header
			bytes := func(kib int64) storage.FakeResponse {
				return storage.FakeResponse{Stdout: []byte(strconv.FormatInt(kib*1024, 10) + "\n")}
			}

			fx := storage.NewFakeExec()
			fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-key_00000",
				storage.FakeResponse{Stdout: []byte("pvc-key_00000\n")})
			fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-key_00000",
				storage.FakeResponse{Stdout: []byte(keyDev + "|" + strconv.FormatInt(tc.onDisk, 10) + "\n")})
			fx.Expect("cryptsetup isLuks "+keyDev, storage.FakeResponse{})
			fx.Expect("cryptsetup luksDump "+keyDev, storage.FakeResponse{Stdout: []byte(
				"Version: 2\nData segments:\n  0: crypt\n\toffset: 16777216 [bytes]\n")})
			fx.Expect(keyStatus, activeMapper())
			fx.Expect(keyOpen, storage.FakeResponse{Err: errLUKSOpenAlready})
			fx.Expect("drbdsetup status "+keyRD+" --json", diskless)
			fx.Expect("drbdadm dump-md "+keyRD+"/0", storage.FakeResponse{Err: errNoMetadata})
			// The backing has grown by the time the LUKS layer looks;
			// the mapper is still the old backing less the header.
			fx.Expect("blockdev --getsize64 "+keyDev, bytes(grownKib))
			fx.Expect("blockdev --getsize64 "+mapperPath, bytes(tc.onDisk-luks.HeaderKiB))

			rec := satellite.NewReconciler(satellite.ReconcilerConfig{
				Providers:  map[string]storage.Provider{"thin1": lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)},
				Adm:        drbd.NewAdm(fx),
				Exec:       fx,
				StateDir:   t.TempDir(),
				NodeName:   "n1",
				Cryptsetup: luks.NewCryptsetup(fx, anyBlockDevice()),
			})
			rec.SetLUKSKeys(storedLUKSKey("stored-pw"))

			// A replica joining a volume that already holds data.
			joins := false
			peerID := int32(0)

			results, err := rec.Apply(t.Context(), []*intent.DesiredResource{{
				Name:     keyRD,
				NodeName: "n1",
				Volumes: []*intent.DesiredVolume{{
					VolumeNumber: 0, SizeKib: gib20, StoragePool: "thin1",
					DeviceSizeKib: adoptedDev, AppliedSizeKib: gib20,
				}},
				LayerStack:      stack,
				SkipInitialSync: &joins,
				Peers:           []intent.DesiredPeer{{Name: "n2", NodeID: &peerID}},
				DrbdOptions: map[string]string{
					"port": "7000", "node-id": "1", "address": "10.0.0.2", "minor": "1000",
				},
			}})
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}

			if len(results) != 1 || !results[0].GetOk() {
				t.Fatalf("apply failed: %v", results)
			}

			calls := fx.CommandLines()
			at := func(match func(string) bool) int { return slices.IndexFunc(calls, match) }

			grown := at(func(line string) bool {
				return strings.HasPrefix(line, "lvextend") && strings.Contains(line, " --size "+strconv.FormatInt(grownKib, 10)+"k ")
			})
			mapper := slices.Index(calls, keyResize)
			created := at(func(line string) bool { return strings.HasPrefix(line, "drbdadm create-md ") })
			attached := slices.Index(calls, "drbdadm adjust "+keyRD)

			if grown < 0 || created < grown || attached < created {
				t.Errorf("want the backing grown to %d KiB, then create-md, then adjust; ran %v", grownKib, calls)
			}

			if tc.luks && (mapper < grown || mapper > attached) {
				t.Errorf("want the mapper grown between the backing grow and the attach; ran %v", calls)
			}

			for _, line := range calls {
				if strings.HasPrefix(line, "drbdadm resize") {
					t.Errorf("%q ran for a device nobody resized", line)
				}

				if strings.Contains(line, "set-gi") || strings.Contains(line, "new-current-uuid") {
					t.Errorf("%q seeded a replica that has to sync from its peers", line)
				}
			}
		})
	}
}
