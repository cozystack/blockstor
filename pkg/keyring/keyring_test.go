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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
)

// The Argon2id vector was computed with `openssl kdf ARGON2ID` (pass
// "hunter2", salt c8 c9 .. d7, t=3, m=65536 KiB, p=4, 32 bytes out), the slot
// vector with Python's AES-256-GCM under that KEK, nonce 32 33 .. 3d and the
// AAD for slot ID a41c09d2, over the master key 00 01 .. 1f.
const (
	katSlotPass   = "hunter2"
	katSlotID     = "a41c09d2"
	katKEK        = "b04f4f51f0514600e1c441f9e7cebd79f367d1b613a26e6ada543d7b52f6eb1e"
	katSlotSealed = "4a9f096f75137ea1b6a2998940482dca568e63d30a334c32" +
		"d035b5769bb5338b1e69c828b794e9a2483f3ef2f0590339"
)

const testNowUnix = 1794650400

func testNowTime() time.Time { return time.Unix(testNowUnix, 0).UTC() }

func katSlot(t *testing.T) Slot {
	t.Helper()

	slot, err := sealSlot(katMasterKey(t), []byte(katSlotPass), &slotParams{
		id:     katSlotID,
		name:   "secret",
		origin: OriginSecret,
		now:    testNowTime(),
		salt:   seqBytes(200, saltSize),
		nonce:  seqBytes(50, nonceSize),
	})
	if err != nil {
		t.Fatalf("sealSlot: %v", err)
	}

	return slot
}

func katKeyring(t *testing.T) *Keyring {
	t.Helper()

	return &Keyring{Version: Version, KeyID: katKeyID, CreatedAt: testNowTime(), Slots: []Slot{katSlot(t)}}
}

func newKeyring(t *testing.T, passphrase string) (*Keyring, *MasterKey) {
	t.Helper()

	ring, master, err := New([]byte(passphrase), "secret", OriginSecret, testNowTime())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	return ring, master
}

func TestArgon2idMatchesKnownAnswer(t *testing.T) {
	t.Parallel()

	slot := katSlot(t)

	if got := hex.EncodeToString(deriveKEK([]byte(katSlotPass), &slot.KDF)); got != katKEK {
		t.Fatalf("KEK %s, want %s", got, katKEK)
	}
}

// A slot sealed with fixed salt and nonce is the independently computed one,
// and the passphrase opens it to the master key.
func TestSlotMatchesKnownAnswer(t *testing.T) {
	t.Parallel()

	slot := katSlot(t)

	if got := hex.EncodeToString(slot.WrappedKey); got != katSlotSealed {
		t.Fatalf("sealed %s, want %s", got, katSlotSealed)
	}

	ring := katKeyring(t)

	master, slotID, err := ring.Unlock([]byte(katSlotPass))
	if err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	if slotID != katSlotID || !bytes.Equal(master.key[:], seqBytes(0, MasterKeySize)) {
		t.Fatalf("slot %s, key %x", slotID, master.key)
	}
}

// New, Marshal, Parse and Unlock round-trip: the parsed keyring opens to
// the same master key, and the JSON carries the field names of the design.
func TestKeyringRoundTrip(t *testing.T) {
	t.Parallel()

	ring, master := newKeyring(t, "first passphrase")

	data, err := ring.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var shape map[string]any

	err = json.Unmarshal(data, &shape)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	for _, key := range []string{"version", "keyID", "createdAt", "slots"} {
		if _, ok := shape[key]; !ok {
			t.Errorf("JSON lacks %q: %s", key, data)
		}
	}

	parsed, err := Parse(data)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	opened, slotID, err := parsed.Unlock([]byte("first passphrase"))
	if err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	if slotID != ring.Slots[0].ID || opened.key != master.key || opened.KeyID() != ring.KeyID {
		t.Fatal("parsed keyring opens to another key")
	}
}

// A wrong passphrase, or any byte of the salt, the nonce, the wrapped key or
// the slot ID changed, opens nothing, and the error is the same in every
// case.
func TestUnlockRefusesWrongPassphraseAndTampering(t *testing.T) {
	t.Parallel()

	_, _, err := katKeyring(t).Unlock([]byte(katSlotPass + "\n"))
	if !errors.Is(err, ErrWrongPassphrase) {
		t.Fatalf("wrong passphrase: err %v", err)
	}

	tampers := map[string]func(*Slot){
		"salt":    func(s *Slot) { s.KDF.Salt[3] ^= 1 },
		"nonce":   func(s *Slot) { s.Nonce[0] ^= 1 },
		"wrapped": func(s *Slot) { s.WrappedKey[40] ^= 1 },
		"tag":     func(s *Slot) { s.WrappedKey[len(s.WrappedKey)-1] ^= 1 },
		"slot ID": func(s *Slot) { s.ID = "a41c09d3" },
		"time":    func(s *Slot) { s.KDF.Time++ },
	}

	for name, tamper := range tampers {
		ring := katKeyring(t)
		tamper(&ring.Slots[0])

		_, _, err := ring.Unlock([]byte(katSlotPass))
		if !errors.Is(err, ErrWrongPassphrase) {
			t.Errorf("%s: err %v, want ErrWrongPassphrase", name, err)
		}
	}
}

// A slot moved over from another keyring opens with its passphrase but to
// another master key, so it does not unlock this keyring.
func TestUnlockRefusesSlotFromAnotherKeyring(t *testing.T) {
	t.Parallel()

	ring, _ := newKeyring(t, "ours")
	other, _ := newKeyring(t, "theirs")

	ring.Slots = append(ring.Slots, other.Slots[0])
	ring.Slots[1].Name = "moved"

	_, _, err := ring.Unlock([]byte("theirs"))
	if !errors.Is(err, ErrWrongPassphrase) {
		t.Fatalf("err %v, want ErrWrongPassphrase", err)
	}
}

// Slot parameters below what blockstor writes, or above the ceilings, are
// refused before Argon2id runs.
func TestUnlockRefusesKDFOutOfBounds(t *testing.T) {
	t.Parallel()

	cases := map[string]func(*KDF){
		"time low":          func(k *KDF) { k.Time = argonTime - 1 },
		"time high":         func(k *KDF) { k.Time = maxArgonTime + 1 },
		"memory low":        func(k *KDF) { k.MemoryKiB = argonMemoryKiB - 1 },
		"memory high":       func(k *KDF) { k.MemoryKiB = maxArgonMemoryKiB + 1 },
		"parallelism low":   func(k *KDF) { k.Parallelism = argonParallelism - 1 },
		"parallelism high":  func(k *KDF) { k.Parallelism = maxArgonParallelism + 1 },
		"type":              func(k *KDF) { k.Type = "pbkdf2" },
		"salt short":        func(k *KDF) { k.Salt = k.Salt[:saltSize-1] },
		"memory at ceiling": nil,
	}

	for name, mutate := range cases {
		ring := katKeyring(t)

		if mutate == nil {
			ring.Slots[0].KDF.Time = maxArgonTime
			ring.Slots[0].KDF.Parallelism = maxArgonParallelism

			err := ring.Validate()
			if err != nil {
				t.Errorf("%s: %v", name, err)
			}

			continue
		}

		mutate(&ring.Slots[0].KDF)

		derived := 0
		counting := func(passphrase []byte, kdf *KDF) []byte {
			derived++

			return deriveKEK(passphrase, kdf)
		}

		_, _, err := ring.unlockWith([]byte(katSlotPass), counting)

		if !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: err %v, want ErrMalformed", name, err)
		}

		if derived != 0 {
			t.Errorf("%s: refused after %d derivations, want none", name, derived)
		}
	}

	// The counter sees the derivation of a slot within bounds.
	derived := 0

	_, _, err := katKeyring(t).unlockWith([]byte(katSlotPass), func(passphrase []byte, kdf *KDF) []byte {
		derived++

		return deriveKEK(passphrase, kdf)
	})
	if err != nil || derived != 1 {
		t.Fatalf("in bounds: err %v after %d derivations, want one", err, derived)
	}
}

func TestValidateRefusesMalformedKeyrings(t *testing.T) {
	t.Parallel()

	cases := map[string]func(*Keyring){
		"version":    func(k *Keyring) { k.Version = 2 },
		"key ID":     func(k *Keyring) { k.KeyID = "c7324c99" },
		"key ID hex": func(k *Keyring) { k.KeyID = "zz324c99567678f7" },
		"no slots":   func(k *Keyring) { k.Slots = nil },
		"too many slots": func(k *Keyring) {
			for i := range MaxSlots {
				slot := k.Slots[0]
				slot.ID = fmt.Sprintf("%08x", i+1)
				slot.Name = fmt.Sprintf("s%d", i)
				k.Slots = append(k.Slots, slot)
			}
		},
		"slot ID": func(k *Keyring) { k.Slots[0].ID = "a41c" },
		"name":    func(k *Keyring) { k.Slots[0].Name = "" },
		"name ID": func(k *Keyring) { k.Slots[0].Name = "deadbeef" },
		"name up": func(k *Keyring) { k.Slots[0].Name = "Secret" },
		"origin":  func(k *Keyring) { k.Slots[0].Origin = "Imported" },
		"aead":    func(k *Keyring) { k.Slots[0].AEAD = "chacha20-poly1305" },
		"nonce":   func(k *Keyring) { k.Slots[0].Nonce = k.Slots[0].Nonce[:8] },
		"wrapped": func(k *Keyring) { k.Slots[0].WrappedKey = k.Slots[0].WrappedKey[:32] },
		"repeated ID": func(k *Keyring) {
			k.Slots = append(k.Slots, k.Slots[0])
			k.Slots[1].Name = "other"
		},
		"repeated name": func(k *Keyring) {
			k.Slots = append(k.Slots, k.Slots[0])
			k.Slots[1].ID = "00000000"
		},
	}

	for name, mutate := range cases {
		ring := katKeyring(t)
		mutate(ring)

		err := ring.Validate()
		if !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: err %v, want ErrMalformed", name, err)
		}

		_, err = ring.Marshal()
		if !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: Marshal err %v, want ErrMalformed", name, err)
		}
	}

	_, err := Parse([]byte("{"))
	if !errors.Is(err, ErrMalformed) {
		t.Errorf("bad JSON: err %v, want ErrMalformed", err)
	}

	_, err = Parse([]byte(`{"version":1}`))
	if !errors.Is(err, ErrMalformed) {
		t.Errorf("empty keyring: err %v, want ErrMalformed", err)
	}
}

// A second slot opens the same master key; each passphrase reports its own
// slot; names are unique; a foreign key cannot add a slot.
func TestAddSlot(t *testing.T) {
	t.Parallel()

	ring, master := newKeyring(t, "first")

	secondID, err := ring.AddSlot(master, []byte("second"), "ops", OriginAPI, testNowTime())
	if err != nil {
		t.Fatalf("AddSlot: %v", err)
	}

	if secondID == ring.Slots[0].ID {
		t.Fatal("two slots share an ID")
	}

	if bytes.Equal(ring.Slots[0].KDF.Salt, ring.Slots[1].KDF.Salt) || bytes.Equal(ring.Slots[0].Nonce, ring.Slots[1].Nonce) {
		t.Fatal("two slots share a salt or a nonce")
	}

	for pass, want := range map[string]string{"first": ring.Slots[0].ID, "second": secondID} {
		opened, slotID, err := ring.Unlock([]byte(pass))
		if err != nil {
			t.Fatalf("Unlock %s: %v", pass, err)
		}

		if slotID != want || opened.key != master.key {
			t.Fatalf("Unlock %s: slot %s, want %s", pass, slotID, want)
		}
	}

	_, err = ring.AddSlot(master, []byte("third"), "ops", OriginAPI, testNowTime())
	if !errors.Is(err, ErrDuplicateName) {
		t.Errorf("duplicate name: err %v", err)
	}

	foreign, err := NewMasterKey()
	if err != nil {
		t.Fatalf("NewMasterKey: %v", err)
	}

	_, err = ring.AddSlot(foreign, []byte("third"), "third", OriginAPI, testNowTime())
	if !errors.Is(err, ErrKeyMismatch) {
		t.Errorf("foreign key: err %v", err)
	}

	_, err = ring.AddSlot(master, []byte("third"), "third", "Imported", testNowTime())
	if !errors.Is(err, ErrMalformed) {
		t.Errorf("bad origin: err %v", err)
	}

	if len(ring.Slots) != 2 {
		t.Fatalf("%d slots after refused adds, want 2", len(ring.Slots))
	}
}

// Slot names are [a-z0-9-]{1,32} and never read as a slot ID, which slot
// references resolve first. A refused name is refused before any
// derivation, and leaves the keyring as it was.
func TestSlotNames(t *testing.T) {
	t.Parallel()

	for name, want := range map[string]bool{
		"recovery":              true,
		"secret-2":              true,
		"0":                     true,
		strings.Repeat("a", 32): true,
		"deadbeef0":             true,
		"":                      false,
		strings.Repeat("a", 33): false,
		"Recovery":              false,
		"off_site":              false,
		"two words":             false,
		"deadbeef":              false,
		"0123abcd":              false,
		"ключ":                  false,
	} {
		if got := ValidSlotName(name); got != want {
			t.Errorf("ValidSlotName(%q) = %v, want %v", name, got, want)
		}
	}

	ring := katKeyring(t)

	for _, name := range []string{"", "deadbeef", "Off_Site"} {
		_, err := ring.AddSlot(katMasterKey(t), []byte("pass"), name, OriginAPI, testNowTime())
		if !errors.Is(err, ErrInvalidName) {
			t.Errorf("AddSlot(%q): err %v, want ErrInvalidName", name, err)
		}
	}

	if len(ring.Slots) != 1 {
		t.Fatalf("%d slots after refused adds, want 1", len(ring.Slots))
	}
}

func TestAddSlotStopsAtMaxSlots(t *testing.T) {
	t.Parallel()

	ring := katKeyring(t)
	master := katMasterKey(t)

	// Copies of one slot with distinct IDs and names: AddSlot only counts.
	for i := len(ring.Slots); i < MaxSlots; i++ {
		slot := ring.Slots[0]
		slot.ID = fmt.Sprintf("%08x", i)
		slot.Name = fmt.Sprintf("s%d", i)
		ring.Slots = append(ring.Slots, slot)
	}

	_, err := ring.AddSlot(master, []byte("one more"), "more", OriginAPI, testNowTime())
	if !errors.Is(err, ErrTooManySlots) {
		t.Fatalf("err %v, want ErrTooManySlots", err)
	}
}

// RemoveSlot finds a slot by ID or by name and refuses the last slot and the
// slot the cluster unlocks with.
func TestRemoveSlot(t *testing.T) {
	t.Parallel()

	ring, master := newKeyring(t, "first")
	unlockID := ring.Slots[0].ID

	_, err := ring.AddSlot(master, []byte("second"), "ops", OriginAPI, testNowTime())
	if err != nil {
		t.Fatalf("AddSlot: %v", err)
	}

	err = ring.RemoveSlot("nope", unlockID)
	if !errors.Is(err, ErrSlotNotFound) {
		t.Errorf("unknown: err %v", err)
	}

	err = ring.RemoveSlot("secret", unlockID)
	if !errors.Is(err, ErrProtectedSlot) {
		t.Errorf("unlock slot: err %v", err)
	}

	err = ring.RemoveSlot("ops", unlockID)
	if err != nil {
		t.Fatalf("by name: %v", err)
	}

	_, _, err = ring.Unlock([]byte("second"))
	if !errors.Is(err, ErrWrongPassphrase) {
		t.Errorf("removed slot still opens: %v", err)
	}

	err = ring.RemoveSlot(unlockID, "")
	if !errors.Is(err, ErrLastSlot) {
		t.Errorf("last slot: err %v", err)
	}

	secondID, err := ring.AddSlot(master, []byte("second"), "ops", OriginAPI, testNowTime())
	if err != nil {
		t.Fatalf("AddSlot: %v", err)
	}

	err = ring.RemoveSlot(secondID, unlockID)
	if err != nil || len(ring.Slots) != 1 {
		t.Fatalf("by ID: err %v, %d slots", err, len(ring.Slots))
	}
}

// A superseded slot keeps opening the keyring until the Secret opens another
// slot; RemoveSuperseded never drops the slot the Secret opens nor empties
// the keyring.
func TestSupersede(t *testing.T) {
	t.Parallel()

	ring, master := newKeyring(t, "old")
	oldID := ring.Slots[0].ID

	newID, err := ring.AddSlot(master, []byte("new"), "secret-2", OriginSecret, testNowTime())
	if err != nil {
		t.Fatalf("AddSlot: %v", err)
	}

	err = ring.Supersede("nope")
	if !errors.Is(err, ErrSlotNotFound) {
		t.Errorf("unknown: err %v", err)
	}

	err = ring.Supersede(oldID)
	if err != nil {
		t.Fatalf("Supersede: %v", err)
	}

	if _, err := ring.Opens([]byte("old")); err != nil {
		t.Fatalf("superseded slot stopped opening: %v", err)
	}

	if ring.RemoveSuperseded(oldID) {
		t.Fatal("removed the slot the Secret still opens")
	}

	if !ring.RemoveSuperseded(newID) || len(ring.Slots) != 1 || ring.Slots[0].ID != newID {
		t.Fatalf("superseded slot not removed: %+v", ring.Slots)
	}

	err = ring.Supersede(newID)
	if err != nil {
		t.Fatalf("Supersede: %v", err)
	}

	if ring.RemoveSuperseded("") || len(ring.Slots) != 1 {
		t.Fatal("RemoveSuperseded emptied the keyring")
	}
}
