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
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/luks"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// Bug 215: the satellite reconciler used to detect the cryptsetup
// "already exists" error class via a raw English-only substring match
// (`strings.Contains(err.Error(), "already exists")`). On a satellite
// node whose locale was anything but C/en_US, cryptsetup would
// translate the message ("Gerät existiert bereits" on de_DE,
// "Le périphérique existe déjà" on fr_FR, etc.), the substring match
// would silently miss, and the reconciler would propagate the EEXIST
// as a hard failure — eventually retrying luksFormat against an
// already-LUKS-formatted device, which is a corruption-class hazard
// because the format wipes the existing key slots.
//
// The fix is layered: the common exec helper now forces LC_ALL=C so
// real cryptsetup output is always English, and the luks wrapper
// returns a typed sentinel (luks.ErrAlreadyOpen) that the reconciler
// matches with errors.Is — closing the gap on both the
// production-locale axis AND the future-locale-injection axis.
//
// This test injects a German "already exists" error via FakeExec
// (FakeExec doesn't enforce LC_ALL — it just returns whatever error
// the test registered, simulating a satellite that bypassed the env
// guard somehow). The reconciler MUST still treat it as EEXIST and
// continue the reconcile rather than bubbling the error up.
func TestApplyLUKSOpenAlreadyExistsNonEnglishLocale(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	fx.Expect(
		"lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } "+
			"--noheadings -o lv_name vg/pvc-luks-de_00000",
		storage.FakeResponse{Stdout: []byte("pvc-luks-de_00000\n")})
	fx.Expect(
		"lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } "+
			"--noheadings --separator | -o lv_path,lv_size --units k --nosuffix "+
			"vg/pvc-luks-de_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-luks-de_00000|1048576\n")})
	// Probe says: yes, this is a LUKS device — so luksFormat is
	// skipped and we go straight to luksOpen.
	fx.Expect("cryptsetup isLuks /dev/vg/pvc-luks-de_00000",
		storage.FakeResponse{})
	// German cryptsetup translation for the EEXIST class — the
	// dm-crypt mapper is still open from a previous reconcile, so
	// re-opening collides. This is the everyday idempotent-path
	// error that the satellite must absorb.
	fx.Expect(
		"cryptsetup luksOpen /dev/vg/pvc-luks-de_00000 pvc-luks-de-0-luks --key-file -",
		storage.FakeResponse{
			Err: errors.New("Gerät pvc-luks-de-0-luks existiert bereits."),
		})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers:  map[string]storage.Provider{"thin1": thin},
		Adm:        drbd.NewAdm(fx),
		StateDir:   dir,
		NodeName:   "n1",
		Cryptsetup: luks.NewCryptsetup(fx),
	})

	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-luks-de",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			LayerStack: []string{"LUKS", "STORAGE"},
			Props:      map[string]string{"LuksPassphrase": "topsecret"},
		},
	})
	if err != nil {
		t.Fatalf("Apply (locale-de EEXIST classification): "+
			"reconciler propagated a non-English already-exists error "+
			"instead of treating it as EEXIST: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("results: got %d, want 1", len(results))
	}

	if !results[0].GetOk() {
		t.Fatalf("expected Ok result on EEXIST idempotency path, got %+v: %s",
			results[0], results[0].GetMessage())
	}

	// Defensive guard: must NOT have re-issued luksFormat. The
	// pre-fix behaviour, after misclassifying the German error,
	// could fall into a retry that re-formats — wiping the key
	// slots. Format on an already-LUKS-formatted device is the
	// corruption-class outcome this bug ultimately enables.
	for _, line := range fx.CommandLines() {
		if strings.Contains(line, "luksFormat") {
			t.Errorf("luksFormat ran after EEXIST — corruption risk: %s", line)
		}
	}
}

// TestRealExecForcesEnglishLocale pins the defensive shield in
// pkg/storage/exec.go: RealExec runs every child process with
// LC_ALL=C set so locale-sensitive parsers (cryptsetup EEXIST,
// drbdadm output, lvm/zfs error class detection) see English
// regardless of how the satellite container was launched. The check
// uses `sh -c 'printf %s "$LC_ALL"'` because it's the smallest
// portable env probe we can express through storage.Exec.
func TestRealExecForcesEnglishLocale(t *testing.T) {
	// Sabotage the parent env so we can prove the child got the
	// override and didn't merely inherit a happy default.
	t.Setenv("LC_ALL", "ru_RU.UTF-8")

	out, err := storage.RealExec{}.Run(t.Context(),
		"sh", "-c", `printf %s "$LC_ALL"`)
	if err != nil {
		t.Fatalf("sh probe: %v", err)
	}

	got := string(out)
	if got != "C" {
		t.Errorf("RealExec must force LC_ALL=C for child processes "+
			"(defence against locale-dependent error parsers, Bug 215); "+
			"got %q", got)
	}
}
