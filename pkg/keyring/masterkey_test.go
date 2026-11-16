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

package keyring

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"testing"

	"github.com/cockroachdb/errors"
)

// The vectors below were computed outside Go, with Python's cryptography
// package (HKDF-SHA256, AES-256-GCM), from the master key 00 01 .. 1f.
const (
	katKeyID   = "c7324c99567678f7"
	katWrapKey = "37e4800ecc9a6f117bc387eda158e22f391e6c9aedd7bc971d2e90cc349de173"
	katPass    = "correct horse battery staple"
	katVolWrap = "6465666768696a6b6c6d6e6fd2f81240b51f1ec4789758d87228a573f220543f" +
		"275c7794d3dd13c5d58bde7a82e57c143420309e1069418713f81027e75cc2bf" +
		"51fbb535f0174409049f5920694a59652e834cd014a1510f55408670"
)

func seqBytes(from, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(from + i)
	}

	return out
}

func katMasterKey(t *testing.T) *MasterKey {
	t.Helper()

	master, err := masterKeyFrom(seqBytes(0, MasterKeySize))
	if err != nil {
		t.Fatalf("masterKeyFrom: %v", err)
	}

	return master
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()

	out, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("decode %q: %v", s, err)
	}

	return out
}

// Both HKDF sub-keys match an independent implementation.
func TestSubKeysMatchKnownAnswer(t *testing.T) {
	t.Parallel()

	master := katMasterKey(t)

	if master.KeyID() != katKeyID {
		t.Errorf("key ID %s, want %s", master.KeyID(), katKeyID)
	}

	if got := hex.EncodeToString(master.wrapKey[:]); got != katWrapKey {
		t.Errorf("wrap key %s, want %s", got, katWrapKey)
	}
}

// The volume wrap with a fixed nonce produces the independently computed
// blob, and that blob unwraps to the password.
func TestVolumeWrapMatchesKnownAnswer(t *testing.T) {
	t.Parallel()

	master := katMasterKey(t)

	wrapped, err := master.wrapVolumePassword([]byte(katPass), seqBytes(100, nonceSize))
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}

	if got := hex.EncodeToString(wrapped.Wrapped); got != katVolWrap {
		t.Errorf("wrapped %s, want %s", got, katVolWrap)
	}

	if wrapped.KeyID != katKeyID {
		t.Errorf("key ID %s, want %s", wrapped.KeyID, katKeyID)
	}

	plain, err := master.UnwrapVolumePassword(WrappedKey{KeyID: katKeyID, Wrapped: mustHex(t, katVolWrap)})
	if err != nil {
		t.Fatalf("unwrap: %v", err)
	}

	if string(plain) != katPass {
		t.Errorf("unwrapped %q, want %q", plain, katPass)
	}
}

// A random wrap round-trips, uses a fresh nonce every time, and a copy of
// the blob unwraps the same way: copies need no master key.
func TestVolumeWrapRoundTripsAndCopies(t *testing.T) {
	t.Parallel()

	master, err := NewMasterKey()
	if err != nil {
		t.Fatalf("NewMasterKey: %v", err)
	}

	password, err := GeneratePassword()
	if err != nil {
		t.Fatalf("GeneratePassword: %v", err)
	}

	first, err := master.WrapVolumePassword(password)
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}

	second, err := master.WrapVolumePassword(password)
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}

	if bytes.Equal(first.Wrapped[:nonceSize], second.Wrapped[:nonceSize]) {
		t.Fatal("two wraps share a nonce")
	}

	copied := WrappedKey{KeyID: first.KeyID, Wrapped: append([]byte(nil), first.Wrapped...)}

	for _, wrapped := range []WrappedKey{first, second, copied} {
		plain, err := master.UnwrapVolumePassword(wrapped)
		if err != nil {
			t.Fatalf("unwrap: %v", err)
		}

		if !bytes.Equal(plain, password) {
			t.Fatalf("unwrapped %q, want %q", plain, password)
		}
	}
}

// Every flipped bit of the blob, a truncated blob, and a blob carried
// under a forged key ID are all refused.
func TestVolumeUnwrapRefusesTampering(t *testing.T) {
	t.Parallel()

	master := katMasterKey(t)
	blob := mustHex(t, katVolWrap)

	for i := range blob {
		tampered := append([]byte(nil), blob...)
		tampered[i] ^= 0x01

		_, err := master.UnwrapVolumePassword(WrappedKey{KeyID: katKeyID, Wrapped: tampered})
		if !errors.Is(err, ErrUnwrap) {
			t.Fatalf("byte %d flipped: err %v, want ErrUnwrap", i, err)
		}
	}

	for _, size := range []int{0, nonceSize, nonceSize + 15, len(blob) - 1} {
		_, err := master.UnwrapVolumePassword(WrappedKey{KeyID: katKeyID, Wrapped: blob[:size]})
		if !errors.Is(err, ErrUnwrap) {
			t.Fatalf("truncated to %d: err %v, want ErrUnwrap", size, err)
		}
	}

	other, err := NewMasterKey()
	if err != nil {
		t.Fatalf("NewMasterKey: %v", err)
	}

	_, err = other.UnwrapVolumePassword(WrappedKey{KeyID: katKeyID, Wrapped: blob})
	if !errors.Is(err, ErrKeyIDMismatch) {
		t.Fatalf("other key: err %v, want ErrKeyIDMismatch", err)
	}

	// The key ID is bound through the AAD as well: a blob relabelled to the
	// other key's ID fails the tag, not the ID check.
	_, err = other.UnwrapVolumePassword(WrappedKey{KeyID: other.KeyID(), Wrapped: blob})
	if !errors.Is(err, ErrUnwrap) {
		t.Fatalf("relabelled: err %v, want ErrUnwrap", err)
	}
}

// The framing pads to a multiple of 64 bytes, round-trips every length
// around a block edge, and refuses what it cannot carry.
func TestFraming(t *testing.T) {
	t.Parallel()

	for _, size := range []int{0, 1, 43, 62, 63, 64, 126, 127, frameMax} {
		password := bytes.Repeat([]byte{'x'}, size)

		framed, err := frame(password)
		if err != nil {
			t.Fatalf("frame %d: %v", size, err)
		}

		want := (2 + size + frameBlock - 1) / frameBlock * frameBlock
		if len(framed) != want {
			t.Errorf("frame %d: %d bytes, want %d", size, len(framed), want)
		}

		back, err := unframe(framed)
		if err != nil {
			t.Fatalf("unframe %d: %v", size, err)
		}

		if !bytes.Equal(back, password) {
			t.Errorf("unframe %d: got %d bytes back", size, len(back))
		}
	}

	_, err := frame(make([]byte, frameMax+1))
	if !errors.Is(err, ErrPasswordTooLong) {
		t.Errorf("oversize: err %v, want ErrPasswordTooLong", err)
	}

	good, err := frame([]byte("abc"))
	if err != nil {
		t.Fatalf("frame: %v", err)
	}

	cases := map[string][]byte{
		"empty":        nil,
		"short":        good[:1],
		"not a block":  good[:frameBlock-1],
		"length large": append([]byte{0, frameBlock - 1}, good[2:]...),
		"dirty fill":   append(append([]byte(nil), good[:frameBlock-1]...), 1),
	}

	for name, framed := range cases {
		_, err := unframe(framed)
		if !errors.Is(err, ErrUnwrap) {
			t.Errorf("%s: err %v, want ErrUnwrap", name, err)
		}
	}
}

// A generated password is 43 base64url characters carrying 32 bytes, and
// two are never equal.
func TestGeneratePassword(t *testing.T) {
	t.Parallel()

	first, err := GeneratePassword()
	if err != nil {
		t.Fatalf("GeneratePassword: %v", err)
	}

	second, err := GeneratePassword()
	if err != nil {
		t.Fatalf("GeneratePassword: %v", err)
	}

	if len(first) != 43 {
		t.Errorf("length %d, want 43", len(first))
	}

	raw, err := base64.RawURLEncoding.DecodeString(string(first))
	if err != nil || len(raw) != passwordBytes {
		t.Errorf("decode: %d bytes, err %v", len(raw), err)
	}

	if bytes.ContainsAny(first, "+/=") {
		t.Errorf("%q is not unpadded base64url", first)
	}

	if bytes.Equal(first, second) {
		t.Error("two generated passwords are equal")
	}
}

// Destroy clears both the master key and the wrap key.
func TestDestroyClearsKeyMaterial(t *testing.T) {
	t.Parallel()

	master := katMasterKey(t)
	master.Destroy()

	if master.key != [MasterKeySize]byte{} || master.wrapKey != [MasterKeySize]byte{} {
		t.Fatal("key material survives Destroy")
	}
}

func TestMasterKeyFromRefusesWrongLength(t *testing.T) {
	t.Parallel()

	_, err := masterKeyFrom(make([]byte, MasterKeySize-1))
	if !errors.Is(err, ErrMasterKeySize) {
		t.Fatalf("err %v, want ErrMasterKeySize", err)
	}
}
