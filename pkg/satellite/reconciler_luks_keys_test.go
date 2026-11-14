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
	"github.com/cozystack/blockstor/pkg/luks"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

const (
	keyRD  = "pvc-key"
	keyDev = "/dev/vg/pvc-key_00000"

	keyFormat = "cryptsetup luksFormat --batch-mode --pbkdf pbkdf2 --pbkdf-force-iterations 1000 " + keyDev + " --key-file -"
	keyOpen   = "cryptsetup luksOpen " + keyDev + " pvc-key-0-luks --key-file -"
	keyTest   = "cryptsetup luksOpen --test-passphrase " + keyDev + " --key-file -"
)

// luksRun is one apply of a one-volume [LUKS,STORAGE] replica.
type luksRun struct {
	header bool
	blkid  storage.FakeResponse
	source string
	keys   *fakeLUKSKeys
	expect map[string]storage.FakeResponse
	seq    map[string][]storage.FakeResponse

	fx      *storage.FakeExec
	message string
	ok      bool
}

func (run *luksRun) apply(t *testing.T) {
	t.Helper()

	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-key_00000",
		storage.FakeResponse{Stdout: []byte("pvc-key_00000\n")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-key_00000",
		storage.FakeResponse{Stdout: []byte(keyDev + "|1048576\n")})

	if run.header {
		fx.Expect("cryptsetup isLuks "+keyDev, storage.FakeResponse{})
	} else {
		fx.Expect("cryptsetup isLuks "+keyDev, storage.FakeResponse{Err: errNotALUKSDevice})
	}

	fx.Expect("blkid -p -o export "+keyDev, run.blkid)

	for cmd, resp := range run.expect {
		fx.Expect(cmd, resp)
	}

	for cmd, resps := range run.seq {
		fx.ExpectSequence(cmd, resps...)
	}

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers:  map[string]storage.Provider{"thin1": lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)},
		Adm:        drbd.NewAdm(fx),
		StateDir:   t.TempDir(),
		NodeName:   "n1",
		Cryptsetup: luks.NewCryptsetup(fx, anyBlockDevice()),
	})
	rec.SetLUKSKeys(run.keys)

	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{{
		Name:     keyRD,
		NodeName: "n1",
		Volumes: []*intent.DesiredVolume{{
			VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1", SourceSnapshot: run.source,
		}},
		LayerStack: []string{"LUKS", "STORAGE"},
	}})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("results %v", results)
	}

	run.fx, run.ok, run.message = fx, results[0].GetOk(), results[0].GetMessage()
}

// stdinOf is what the first run of cmd read on stdin, and whether it ran.
func (run *luksRun) stdinOf(cmd string) (string, bool) {
	idx := slices.Index(run.fx.CommandLines(), cmd)
	if idx < 0 {
		return "", false
	}

	return run.fx.Stdins[idx], true
}

// emptyDevice is blkid's answer for a device with no signature.
func emptyDevice() storage.FakeResponse {
	return storage.FakeResponse{Err: storage.FakeExitError{Code: 2}}
}

// No password and no header, on a device blkid finds empty: the password
// is generated and stored first, the device is formatted and opened with
// it after.
func TestLUKSNewVolumeStoresItsPasswordBeforeFormatting(t *testing.T) {
	t.Parallel()

	run := &luksRun{blkid: emptyDevice(), keys: &fakeLUKSKeys{}}
	run.apply(t)

	if !run.ok {
		t.Fatalf("apply failed: %s", run.message)
	}

	if got := run.keys.log(); !slices.Equal(got, []string{"stored", "generate"}) {
		t.Fatalf("provider calls %v, want stored then generate", got)
	}

	stored := string(run.keys.stored[luksVol{keyRD, 0}])

	formatKey, formatted := run.stdinOf(keyFormat)
	openKey, opened := run.stdinOf(keyOpen)

	if !formatted || !opened || formatKey != stored || openKey != stored {
		t.Fatalf("format %v with %q, open %v with %q; want both with the stored %q",
			formatted, formatKey, opened, openKey, stored)
	}
}

// A failed store leaves the device unformatted: a header whose password
// is nowhere would lock the data away for good.
func TestLUKSNoFormatWhenThePasswordIsNotStored(t *testing.T) {
	t.Parallel()

	run := &luksRun{blkid: emptyDevice(), keys: &fakeLUKSKeys{generateErr: errStoreRefused}}
	run.apply(t)

	if run.ok || !strings.Contains(run.message, errStoreRefused.Error()) {
		t.Fatalf("ok %v, message %q; want the store failure", run.ok, run.message)
	}

	if _, formatted := run.stdinOf(keyFormat); formatted {
		t.Fatal("formatted with a password that is not stored")
	}
}

// No password and no header on a device that holds data: neither a
// password nor a header is made.
func TestLUKSNoPasswordIsGeneratedForADeviceHoldingData(t *testing.T) {
	t.Parallel()

	run := &luksRun{blkid: storage.FakeResponse{Stdout: []byte("TYPE=xfs\n")}, keys: &fakeLUKSKeys{}}
	run.apply(t)

	if run.ok || !strings.Contains(run.message, luks.ErrDeviceHoldsData.Error()) {
		t.Fatalf("ok %v, message %q; want ErrDeviceHoldsData", run.ok, run.message)
	}

	if slices.Contains(run.keys.log(), "generate") {
		t.Fatal("a password was generated for a device that holds data")
	}

	if _, formatted := run.stdinOf(keyFormat); formatted {
		t.Fatal("luksFormat ran over data")
	}
}

// A stored password and no header: the device is formatted with the
// stored password, nothing new is generated.
func TestLUKSStoredPasswordFormatsAFreshDevice(t *testing.T) {
	t.Parallel()

	run := &luksRun{blkid: emptyDevice(), keys: storedLUKSKey("stored-pw")}
	run.apply(t)

	if !run.ok {
		t.Fatalf("apply failed: %s", run.message)
	}

	if key, formatted := run.stdinOf(keyFormat); !formatted || key != "stored-pw" {
		t.Fatalf("format %v with %q, want the stored password", formatted, key)
	}

	if slices.Contains(run.keys.log(), "generate") {
		t.Fatal("generated a password over a stored one")
	}
}

// A stored password and a header: opened, never formatted.
func TestLUKSStoredPasswordOpensItsHeader(t *testing.T) {
	t.Parallel()

	run := &luksRun{header: true, blkid: storage.FakeResponse{Stdout: []byte("TYPE=crypto_LUKS\n")}, keys: storedLUKSKey("stored-pw")}
	run.apply(t)

	if !run.ok {
		t.Fatalf("apply failed: %s", run.message)
	}

	if key, opened := run.stdinOf(keyOpen); !opened || key != "stored-pw" {
		t.Fatalf("open %v with %q, want the stored password", opened, key)
	}

	if _, formatted := run.stdinOf(keyFormat); formatted {
		t.Fatal("formatted a device that has a header")
	}
}

// A header the stored password does not open is LUKSKeyMismatch, and the
// device is left as it is.
func TestLUKSMismatchIsNeverReformatted(t *testing.T) {
	t.Parallel()

	run := &luksRun{
		header: true, blkid: storage.FakeResponse{Stdout: []byte("TYPE=crypto_LUKS\n")}, keys: storedLUKSKey("stored-pw"),
		expect: map[string]storage.FakeResponse{keyOpen: {Err: storage.FakeExitError{Code: 2}}},
	}
	run.apply(t)

	if run.ok || !strings.Contains(run.message, luks.ErrLUKSKeyMismatch.Error()) {
		t.Fatalf("ok %v, message %q; want ErrLUKSKeyMismatch", run.ok, run.message)
	}

	if _, formatted := run.stdinOf(keyFormat); formatted {
		t.Fatal("re-formatted a device its password does not open")
	}
}

// A header and no password, outside a restore: LUKSKeyMissing, neither
// opened nor formatted.
func TestLUKSHeaderWithoutAPasswordIsMissing(t *testing.T) {
	t.Parallel()

	run := &luksRun{header: true, blkid: storage.FakeResponse{Stdout: []byte("TYPE=crypto_LUKS\n")}, keys: &fakeLUKSKeys{}}
	run.apply(t)

	if run.ok || !strings.Contains(run.message, luks.ErrLUKSKeyMissing.Error()) {
		t.Fatalf("ok %v, message %q; want ErrLUKSKeyMissing", run.ok, run.message)
	}

	for _, cmd := range []string{keyFormat, keyOpen} {
		if _, ran := run.stdinOf(cmd); ran {
			t.Fatalf("%s ran", cmd)
		}
	}

	if slices.Contains(run.keys.log(), "generate") {
		t.Fatal("generated a password for a device with a header")
	}
}

// A volume restored from a snapshot carries the header of the replica the
// bytes came from: the recorded password that opens it is adopted, stored
// first, and opened with. This node's is tried first.
func TestLUKSRestoredVolumeAdoptsTheSnapshotPasswordThatOpens(t *testing.T) {
	t.Parallel()

	run := &luksRun{
		header: true, blkid: storage.FakeResponse{Stdout: []byte("TYPE=crypto_LUKS\n")},
		source: "pvc-src:snap1",
		keys:   &fakeLUKSKeys{candidates: map[string][]byte{"n1": []byte("own-pw"), "peer-a": []byte("peer-pw")}},
		seq: map[string][]storage.FakeResponse{
			keyTest: {{Err: storage.FakeExitError{Code: 2}}, {}},
		},
	}
	run.apply(t)

	if !run.ok {
		t.Fatalf("apply failed: %s", run.message)
	}

	var tried []string

	for i, line := range run.fx.CommandLines() {
		if line == keyTest {
			tried = append(tried, run.fx.Stdins[i])
		}
	}

	if !slices.Equal(tried, []string{"own-pw", "peer-pw"}) {
		t.Fatalf("tried %v, want this node's first, then the peer's", tried)
	}

	if got := run.keys.log(); !slices.Equal(got, []string{"stored", "candidates", "adopt peer-a"}) {
		t.Fatalf("provider calls %v", got)
	}

	if key, opened := run.stdinOf(keyOpen); !opened || key != "peer-pw" {
		t.Fatalf("open %v with %q, want the adopted password", opened, key)
	}
}

// A restored header none of the recorded passwords opens is
// LUKSKeyMissing; nothing is adopted, opened or formatted.
func TestLUKSRestoredVolumeWithNoMatchingPasswordIsMissing(t *testing.T) {
	t.Parallel()

	run := &luksRun{
		header: true, blkid: storage.FakeResponse{Stdout: []byte("TYPE=crypto_LUKS\n")},
		source: "pvc-src:snap1",
		keys:   &fakeLUKSKeys{candidates: map[string][]byte{"n1": []byte("own-pw")}},
		expect: map[string]storage.FakeResponse{keyTest: {Err: storage.FakeExitError{Code: 2}}},
	}
	run.apply(t)

	if run.ok || !strings.Contains(run.message, luks.ErrLUKSKeyMissing.Error()) {
		t.Fatalf("ok %v, message %q; want ErrLUKSKeyMissing", run.ok, run.message)
	}

	if slices.ContainsFunc(run.keys.log(), func(e string) bool { return strings.HasPrefix(e, "adopt") }) {
		t.Fatal("adopted a password that does not open the header")
	}

	for _, cmd := range []string{keyFormat, keyOpen} {
		if _, ran := run.stdinOf(cmd); ran {
			t.Fatalf("%s ran", cmd)
		}
	}
}

// Under a locked master key nothing is formatted or opened.
func TestLUKSLockedMasterKeyTouchesNothing(t *testing.T) {
	t.Parallel()

	run := &luksRun{blkid: emptyDevice(), keys: &fakeLUKSKeys{err: luks.ErrMasterKeyLocked}}
	run.apply(t)

	if run.ok || !strings.Contains(run.message, luks.ErrMasterKeyLocked.Error()) {
		t.Fatalf("ok %v, message %q; want ErrMasterKeyLocked", run.ok, run.message)
	}

	for _, cmd := range []string{keyFormat, keyOpen} {
		if _, ran := run.stdinOf(cmd); ran {
			t.Fatalf("%s ran", cmd)
		}
	}
}
