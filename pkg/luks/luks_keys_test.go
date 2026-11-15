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

package luks_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/cozystack/blockstor/pkg/luks"
	"github.com/cozystack/blockstor/pkg/storage"
)

const keyDev = "/dev/vg/pvc-key_00000"

// A new volume's header is formatted with PBKDF2 at its minimum cost: the
// password is random, and Argon2id would cost seconds and up to a GiB on
// every open after a reboot. The password goes on stdin, never argv.
func TestFormatUsesMinimalPBKDF2(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("cryptsetup isLuks "+keyDev, storage.FakeResponse{Err: errFakeNotLuks})
	fx.Expect("blkid -p -o export "+keyDev, storage.FakeResponse{Err: storage.FakeExitError{Code: 2}})

	err := luks.NewCryptsetup(fx, anyBlockDevice()).Format(t.Context(), keyDev, []byte("pw"))
	if err != nil {
		t.Fatalf("Format: %v", err)
	}

	want := "cryptsetup luksFormat --batch-mode --type luks2 --pbkdf pbkdf2 --pbkdf-force-iterations 1000 " + keyDev + " --key-file -"

	idx := slices.Index(fx.CommandLines(), want)
	if idx < 0 {
		t.Fatalf("no %q in %v", want, fx.CommandLines())
	}

	if fx.Stdins[idx] != "pw" {
		t.Errorf("stdin %q, want the password", fx.Stdins[idx])
	}
}

func TestIsLUKS(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("cryptsetup isLuks /dev/a", storage.FakeResponse{})
	fx.Expect("cryptsetup isLuks /dev/b", storage.FakeResponse{Err: errFakeNotLuks})

	c := luks.NewCryptsetup(fx, anyBlockDevice())

	if !c.IsLUKS(t.Context(), "/dev/a") || c.IsLUKS(t.Context(), "/dev/b") {
		t.Fatal("IsLUKS does not follow the probe's exit status")
	}
}

// IsActive is true only for a status probe that succeeds and describes the
// mapping: an inactive mapping, a failed probe and a silent one all send
// the caller down the path that opens the device.
func TestIsActive(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("cryptsetup status open", storage.FakeResponse{Stdout: []byte("/dev/mapper/open is active.\n")})
	fx.Expect("cryptsetup status closed", storage.FakeResponse{
		Stdout: []byte("/dev/mapper/closed is inactive.\n"), Err: storage.FakeExitError{Code: 4},
	})

	c := luks.NewCryptsetup(fx, anyBlockDevice())

	if !c.IsActive(t.Context(), "open") {
		t.Error("an active mapping is reported inactive")
	}

	for _, name := range []string{"closed", "silent"} {
		if c.IsActive(t.Context(), name) {
			t.Errorf("%s is reported active", name)
		}
	}
}

// EnsureEmpty passes only a block device blkid finds nothing on.
func TestEnsureEmpty(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		blkid storage.FakeResponse
		want  error
	}{
		"empty":       {blkid: storage.FakeResponse{Err: storage.FakeExitError{Code: 2}}, want: nil},
		"drbd":        {blkid: storage.FakeResponse{Stdout: []byte("TYPE=drbd\n")}, want: luks.ErrDeviceHoldsData},
		"luks":        {blkid: storage.FakeResponse{Stdout: []byte("TYPE=crypto_LUKS\n")}, want: luks.ErrDeviceHoldsData},
		"probe error": {blkid: storage.FakeResponse{Err: storage.FakeExitError{Code: 4}}, want: errAny},
	}

	for name, tc := range cases {
		fx := storage.NewFakeExec()
		fx.Expect("blkid -p -o export "+keyDev, tc.blkid)

		err := luks.NewCryptsetup(fx, anyBlockDevice()).EnsureEmpty(t.Context(), keyDev)

		switch {
		case tc.want == nil && err != nil:
			t.Errorf("%s: %v", name, err)
		case errors.Is(tc.want, errAny) && err == nil:
			t.Errorf("%s: passed", name)
		case tc.want != nil && !errors.Is(tc.want, errAny) && !errors.Is(err, tc.want):
			t.Errorf("%s: err %v, want %v", name, err, tc.want)
		}
	}
}

var errAny = errors.New("any error")

// TestKey tells a wrong key (cryptsetup's exit 2) from a probe that failed.
func TestTestKey(t *testing.T) {
	t.Parallel()

	cmd := "cryptsetup luksOpen --test-passphrase " + keyDev + " --key-file -"

	cases := map[string]struct {
		resp    storage.FakeResponse
		ok, err bool
	}{
		"opens":  {resp: storage.FakeResponse{}, ok: true},
		"wrong":  {resp: storage.FakeResponse{Err: storage.FakeExitError{Code: 2}}},
		"failed": {resp: storage.FakeResponse{Err: storage.FakeExitError{Code: 4}}, err: true},
	}

	for name, tc := range cases {
		fx := storage.NewFakeExec()
		fx.Expect(cmd, tc.resp)

		ok, err := luks.NewCryptsetup(fx, anyBlockDevice()).TestKey(t.Context(), keyDev, []byte("pw"))
		if ok != tc.ok || (err != nil) != tc.err {
			t.Errorf("%s: ok %v err %v", name, ok, err)
		}

		if idx := slices.Index(fx.CommandLines(), cmd); idx < 0 || fx.Stdins[idx] != "pw" {
			t.Errorf("%s: the key did not go on stdin of %q", name, cmd)
		}
	}
}

// An open the key does not fit is ErrLUKSKeyMismatch, by exit status, and
// the message says so.
func TestOpenWithTheWrongKeyIsAMismatch(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("cryptsetup luksOpen "+keyDev+" m --key-file -", storage.FakeResponse{Err: storage.FakeExitError{Code: 2}})

	err := luks.NewCryptsetup(fx, anyBlockDevice()).Open(t.Context(), keyDev, "m", []byte("pw"))
	if !errors.Is(err, luks.ErrLUKSKeyMismatch) {
		t.Fatalf("err %v, want ErrLUKSKeyMismatch", err)
	}

	if errors.Is(err, luks.ErrAlreadyOpen) {
		t.Fatal("a wrong key is not an open mapper")
	}
}
