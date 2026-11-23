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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/luks"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// A replica adopted from LINSTOR can still be running on the dm-crypt
// mapper LINSTOR opened, under LINSTOR's name. These pin that blockstor
// keeps using that mapper rather than opening a second one over a device
// it holds, and lets go of it only once blockstor itself has taken the
// replica down.
const (
	linstorMapper     = "Linstor-Crypt-pvc-key_00000"
	linstorTable      = "dmsetup table " + linstorMapper
	keyDevNumber      = "lsblk --nodeps --noheadings --output MAJ:MIN " + keyDev
	linstorOpenResize = "cryptsetup resize " + linstorMapper + " --key-file -"
)

// cryptTableOver is `dmsetup table` for a crypt mapping over the device
// numbered number, its key hidden the way dmsetup prints it.
func cryptTableOver(number string) storage.FakeResponse {
	return storage.FakeResponse{Stdout: []byte(
		"0 2064384 crypt aes-xts-plain64 :64:logon:cryptsetup:0f0e-d0 0 " + number + " 32768\n")}
}

// adoptedMapper is the exec state of a node where LINSTOR's mapper holds
// the replica's device, numbered 253:7.
func adoptedMapper() map[string]storage.FakeResponse {
	return map[string]storage.FakeResponse{
		linstorTable: cryptTableOver("253:7"),
		keyDevNumber: {Stdout: []byte(" 253:7\n")},
	}
}

// The mapper LINSTOR left open is the replica's device: nothing is
// opened, formatted or read for it, on a locked cluster too, and the
// volume is handed on at LINSTOR's mapper path.
func TestLUKSAdoptsTheMapperLINSTOROpened(t *testing.T) {
	t.Parallel()

	run := &luksRun{
		header: true, blkid: storage.FakeResponse{Stdout: []byte("TYPE=crypto_LUKS\n")},
		keys:   &fakeLUKSKeys{err: luks.ErrMasterKeyLocked},
		expect: adoptedMapper(),
	}
	run.apply(t)

	if !run.ok {
		t.Fatalf("apply over LINSTOR's mapper failed: %s", run.message)
	}

	if len(run.volumes) != 1 || run.volumes[0].GetDevicePath() != "/dev/mapper/"+linstorMapper {
		t.Fatalf("volumes %v, want the one at LINSTOR's mapper", run.volumes)
	}

	if got := run.keys.log(); len(got) != 0 {
		t.Fatalf("provider calls %v, want none for an open mapper", got)
	}

	for _, line := range run.fx.CommandLines() {
		if strings.Contains(line, "luksOpen") || strings.Contains(line, "luksFormat") || strings.Contains(line, "luksClose") {
			t.Fatalf("%q ran over a device LINSTOR's mapper holds", line)
		}
	}
}

// A mapper under LINSTOR's name over some other device is not the
// replica's: blockstor opens its own, as for any replica.
func TestLUKSIgnoresALINSTORNamedMapperOverAnotherDevice(t *testing.T) {
	t.Parallel()

	for name, table := range map[string]storage.FakeResponse{
		"other device": cryptTableOver("253:9"),
		"not crypt":    {Stdout: []byte(strings.Replace(string(cryptTableOver("253:7").Stdout), " crypt ", " integrity ", 1))},
		"two targets":  {Stdout: append(cryptTableOver("253:7").Stdout, cryptTableOver("253:7").Stdout...)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			expect := adoptedMapper()
			expect[linstorTable] = table

			run := &luksRun{
				header: true, blkid: storage.FakeResponse{Stdout: []byte("TYPE=crypto_LUKS\n")},
				keys: storedLUKSKey("stored-pw"), expect: expect,
			}
			run.apply(t)

			if !run.ok {
				t.Fatalf("apply failed: %s", run.message)
			}

			if _, opened := run.stdinOf(keyOpen); !opened {
				t.Fatalf("blockstor's mapper was not opened; ran %v", run.fx.CommandLines())
			}

			if run.volumes[0].GetDevicePath() != "/dev/mapper/pvc-key-0-luks" {
				t.Fatalf("device %q, want blockstor's mapper", run.volumes[0].GetDevicePath())
			}
		})
	}
}

// A volume that grows while it runs on LINSTOR's mapper grows that
// mapper, under the name it is open under.
func TestLUKSAdoptedMapperGrowsUnderItsOwnName(t *testing.T) {
	t.Parallel()

	expect := adoptedMapper()
	expect["cryptsetup luksOpen "+keyDev+" "+linstorMapper+" --key-file -"] = storage.FakeResponse{
		Err: errLUKSOpenAlready,
	}

	run := &luksRun{
		header: true, blkid: storage.FakeResponse{Stdout: []byte("TYPE=crypto_LUKS\n")},
		sizeKib: 2 * 1024 * 1024,
		keys:    storedLUKSKey("stored-pw"),
		expect:  expect,
	}
	run.apply(t)

	if !run.ok {
		t.Fatalf("apply failed: %s", run.message)
	}

	if key, resized := run.stdinOf(linstorOpenResize); !resized || key != "stored-pw" {
		t.Fatalf("resize of LINSTOR's mapper %v with %q; ran %v", resized, key, run.fx.CommandLines())
	}

	if _, opened := run.stdinOf(keyOpen); opened {
		t.Fatal("opened blockstor's mapper beside LINSTOR's")
	}
}

// DRBD keeps the disk it runs on: the .res names LINSTOR's mapper, so
// adjust sees no change of backing disk on a live device.
func TestLUKSAdoptedMapperIsDRBDsDisk(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-key_00000",
		storage.FakeResponse{Stdout: []byte("pvc-key_00000\n")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-key_00000",
		storage.FakeResponse{Stdout: []byte(keyDev + "|1048576\n")})
	fx.Expect("cryptsetup isLuks "+keyDev, storage.FakeResponse{})

	for cmd, resp := range adoptedMapper() {
		fx.Expect(cmd, resp)
	}

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers:  map[string]storage.Provider{"thin1": lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)},
		Adm:        drbd.NewAdm(fx),
		StateDir:   dir,
		NodeName:   "n1",
		Cryptsetup: luks.NewCryptsetup(fx, anyBlockDevice()),
	})
	rec.SetLUKSKeys(&fakeLUKSKeys{err: luks.ErrMasterKeyLocked})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{{
		Name:     keyRD,
		NodeName: "n1",
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		LayerStack:      []string{"DRBD", "LUKS", "STORAGE"},
		SkipInitialSync: skipInitTrue(),
		DrbdOptions: map[string]string{
			"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
		},
	}})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	body, err := os.ReadFile(filepath.Join(dir, keyRD+".res"))
	if err != nil {
		t.Fatalf("read .res: %v", err)
	}

	if !strings.Contains(string(body), "disk /dev/mapper/"+linstorMapper+";") {
		t.Fatalf(".res does not keep DRBD on LINSTOR's mapper; body=%s", body)
	}
}

// Deleting the replica closes LINSTOR's mapper too, after DRBD is down:
// left open, it would hold the device the storage layer removes next.
func TestDeleteResourceClosesTheAdoptedMapper(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-key_00000",
		storage.FakeResponse{Stdout: []byte("pvc-key_00000\n")})

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers:  map[string]storage.Provider{"thin1": lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)},
		Adm:        drbd.NewAdm(fx),
		StateDir:   t.TempDir(),
		NodeName:   "n1",
		Cryptsetup: luks.NewCryptsetup(fx, anyBlockDevice()),
	})

	resp, err := rec.DeleteResource(t.Context(), &intent.DeleteResourceRequest{
		Name: keyRD, StoragePool: "thin1", VolumeNumbers: []int32{0},
	})
	if err != nil || !resp.GetOk() {
		t.Fatalf("DeleteResource: %v %s", err, resp.GetMessage())
	}

	calls := fx.CommandLines()
	down := slices.Index(calls, "drbdadm down "+keyRD)
	closed := slices.Index(calls, "cryptsetup luksClose "+linstorMapper)
	removed := slices.IndexFunc(calls, func(line string) bool { return strings.HasPrefix(line, "lvremove") })

	if down < 0 || closed < down || removed < closed {
		t.Fatalf("want drbdadm down, then luksClose %s, then lvremove; got %v", linstorMapper, calls)
	}
}

// Toggling the replica to diskless closes LINSTOR's mapper between the
// detach and the reclaim: left open, it would hold the LV the storage
// layer removes next, and the replica would keep a disk it no longer
// has a volume for.
func TestToggleToDisklessClosesTheAdoptedMapper(t *testing.T) {
	attached := storage.FakeResponse{Stdout: []byte(keyRD + " role:Secondary\n  disk:UpToDate\n")}

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status "+keyRD, attached)
	fx.Expect("drbdsetup status --verbose "+keyRD, attached)
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-key_00000",
		storage.FakeResponse{Stdout: []byte("pvc-key_00000\n")})

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers:  map[string]storage.Provider{"thin1": lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)},
		Adm:        drbd.NewAdm(fx),
		StateDir:   t.TempDir(),
		NodeName:   "n1",
		Cryptsetup: luks.NewCryptsetup(fx, anyBlockDevice()),
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{{
		Name:       keyRD,
		NodeName:   "n1",
		Flags:      []string{"DISKLESS"},
		LayerStack: []string{"DRBD", "LUKS", "STORAGE"},
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		DrbdOptions: map[string]string{
			"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
		},
	}})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	calls := fx.CommandLines()
	detached := slices.Index(calls, "drbdadm detach --force "+keyRD)
	closed := slices.Index(calls, "cryptsetup luksClose "+linstorMapper)
	removed := slices.IndexFunc(calls, func(line string) bool { return strings.HasPrefix(line, "lvremove") })

	if detached < 0 || closed < detached || removed < closed {
		t.Fatalf("want drbdadm detach, then luksClose %s, then lvremove; got %v", linstorMapper, calls)
	}
}

// Taking a replica inactive lets go of LINSTOR's mapper once DRBD is down,
// which is what moves the volume onto blockstor's own mapper when it is
// activated again. A down that is deferred closes nothing.
func TestApplyInactiveClosesTheAdoptedMapperAfterTheDown(t *testing.T) {
	for name, tc := range map[string]struct {
		probe     storage.FakeResponse
		wantClose bool
	}{
		"down": {
			probe:     storage.FakeResponse{Stdout: []byte("No such resource: " + keyRD + "\n"), Err: errInconclusiveProbe},
			wantClose: true,
		},
		"deferred": {
			probe: storage.FakeResponse{Stdout: []byte("error receiving netlink message\n"), Err: errInconclusiveProbe},
		},
	} {
		t.Run(name, func(t *testing.T) {
			fx := storage.NewFakeExec()
			fx.Expect("drbdsetup status "+keyRD+" --json", tc.probe)

			rec := satellite.NewReconciler(satellite.ReconcilerConfig{
				Providers:  map[string]storage.Provider{"thin1": lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)},
				Adm:        drbd.NewAdm(fx),
				StateDir:   t.TempDir(),
				NodeName:   "n1",
				Cryptsetup: luks.NewCryptsetup(fx, anyBlockDevice()),
			})

			_, err := rec.Apply(t.Context(), []*intent.DesiredResource{{
				Name:       keyRD,
				NodeName:   "n1",
				Flags:      []string{"INACTIVE"},
				LayerStack: []string{"DRBD", "LUKS", "STORAGE"},
				Volumes: []*intent.DesiredVolume{
					{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
				},
				DrbdOptions: map[string]string{
					"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
				},
			}})
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}

			calls := fx.CommandLines()
			down := slices.Index(calls, "drbdadm down "+keyRD)
			closed := slices.Index(calls, "cryptsetup luksClose "+linstorMapper)

			if tc.wantClose && (down < 0 || closed < down) {
				t.Fatalf("want drbdadm down, then luksClose %s; got %v", linstorMapper, calls)
			}

			if !tc.wantClose && closed >= 0 {
				t.Fatalf("closed LINSTOR's mapper without a down; got %v", calls)
			}

			if slices.Contains(calls, "cryptsetup luksClose pvc-key-0-luks") {
				t.Fatalf("an inactive replica closed blockstor's own mapper; got %v", calls)
			}
		})
	}
}
