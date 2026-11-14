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
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"io"
	"slices"
	"time"

	"github.com/cockroachdb/errors"
	"golang.org/x/crypto/argon2"
)

const (
	// Version is the keyring format this package reads and writes.
	Version = 1

	// MaxSlots caps the passphrases one keyring holds, as LUKS1 does: every
	// failed unlock costs one Argon2id run per slot.
	MaxSlots = 8

	// KDFArgon2id is the only slot KDF implemented.
	KDFArgon2id = "argon2id"

	// AEADAES256GCM is the only slot cipher implemented.
	AEADAES256GCM = "aes-256-gcm"

	// OriginSecret is a slot enrolled from the passphrase Secret.
	OriginSecret = "Secret"

	// OriginAPI is a slot whose passphrase came through REST or the CLI.
	OriginAPI = "API"

	// OriginGenerated is the slot of the passphrase blockstor generated
	// and keeps in its own unlock Secret.
	OriginGenerated = "Generated"

	// Argon2id parameters of new slots: RFC 9106's second recommended set.
	argonTime        = 3
	argonMemoryKiB   = 64 * 1024
	argonParallelism = 4

	// Ceilings a stored slot may ask for. A slot above them is refused
	// before Argon2id runs, so a tampered keyring cannot make every
	// component allocate gigabytes or spin for minutes.
	maxArgonTime        = 16
	maxArgonMemoryKiB   = 1024 * 1024
	maxArgonParallelism = 16

	saltSize       = 16
	slotIDSize     = 4
	wrappedKeySize = MasterKeySize + 16

	aadSlot = "blockstor.org/master-key-slot/v1"
)

var (
	// ErrWrongPassphrase is a passphrase that opens no slot. It says
	// nothing about which slot was tried or why it did not open.
	ErrWrongPassphrase = errors.New("the passphrase opens no slot of the keyring")

	// ErrMalformed is a keyring that does not parse or does not validate.
	ErrMalformed = errors.New("the keyring is malformed")

	// ErrTooManySlots is an add that would exceed MaxSlots.
	ErrTooManySlots = errors.New("the keyring has no free slot")

	// ErrSlotNotFound is a slot reference that names no slot.
	ErrSlotNotFound = errors.New("no such slot")

	// ErrLastSlot is a removal that would leave the keyring unopenable.
	ErrLastSlot = errors.New("refusing to remove the last slot")

	// ErrProtectedSlot is a removal of the slot the cluster unlocks with.
	ErrProtectedSlot = errors.New("refusing to remove the slot the cluster unlocks with")

	// ErrDuplicateName is a slot name already taken in the keyring.
	ErrDuplicateName = errors.New("a slot with this name already exists")

	// ErrKeyMismatch is a master key that is not the one of this keyring.
	ErrKeyMismatch = errors.New("the master key does not belong to this keyring")
)

// KDF is the passphrase derivation of one slot.
type KDF struct {
	Type        string `json:"type"`
	Salt        []byte `json:"salt"`
	Time        uint32 `json:"time"`
	MemoryKiB   uint32 `json:"memoryKiB"`
	Parallelism uint8  `json:"parallelism"`
}

// Slot is the master key wrapped under one passphrase.
type Slot struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Origin     string    `json:"origin"`
	CreatedAt  time.Time `json:"createdAt"`
	Superseded bool      `json:"superseded,omitempty"`
	KDF        KDF       `json:"kdf"`
	AEAD       string    `json:"aead"`
	Nonce      []byte    `json:"nonce"`
	WrappedKey []byte    `json:"wrappedKey"`
}

// Keyring is the master key's slots plus its public key ID. It holds no key
// material in the clear.
type Keyring struct {
	Version   int       `json:"version"`
	KeyID     string    `json:"keyID"`
	CreatedAt time.Time `json:"createdAt"`
	Slots     []Slot    `json:"slots"`
}

// New creates a keyring around a fresh master key with one slot for
// passphrase. The caller destroys the returned key when done with it.
func New(passphrase []byte, name, origin string, now time.Time) (*Keyring, *MasterKey, error) {
	master, err := NewMasterKey()
	if err != nil {
		return nil, nil, err
	}

	ring, err := NewWithKey(master, passphrase, name, origin, now)
	if err != nil {
		master.Destroy()

		return nil, nil, err
	}

	return ring, master, nil
}

// NewWithKey creates a keyring around an existing master key, as the LINSTOR
// importer does, with one slot for passphrase.
func NewWithKey(master *MasterKey, passphrase []byte, name, origin string, now time.Time) (*Keyring, error) {
	ring := &Keyring{Version: Version, KeyID: master.KeyID(), CreatedAt: now.UTC(), Slots: nil}

	_, err := ring.AddSlot(master, passphrase, name, origin, now)
	if err != nil {
		return nil, err
	}

	return ring, nil
}

// Parse decodes and validates a keyring.
func Parse(data []byte) (*Keyring, error) {
	var ring Keyring

	err := json.Unmarshal(data, &ring)
	if err != nil {
		return nil, errors.Wrapf(ErrMalformed, "decode: %v", err)
	}

	err = ring.Validate()
	if err != nil {
		return nil, err
	}

	return &ring, nil
}

// Marshal validates and encodes the keyring.
func (k *Keyring) Marshal() ([]byte, error) {
	err := k.Validate()
	if err != nil {
		return nil, err
	}

	out, err := json.Marshal(k)
	if err != nil {
		return nil, errors.Wrap(err, "encode keyring")
	}

	return out, nil
}

// Validate checks the structure of the keyring and of every slot, including
// the KDF cost bounds, so that nothing reaches Argon2id or AES-GCM unchecked.
func (k *Keyring) Validate() error {
	if k.Version != Version {
		return errors.Wrapf(ErrMalformed, "version %d, want %d", k.Version, Version)
	}

	if !validHex(k.KeyID, keyIDSize) {
		return errors.Wrapf(ErrMalformed, "key ID %q", k.KeyID)
	}

	if len(k.Slots) == 0 || len(k.Slots) > MaxSlots {
		return errors.Wrapf(ErrMalformed, "%d slots, want 1 to %d", len(k.Slots), MaxSlots)
	}

	ids := map[string]bool{}
	names := map[string]bool{}

	for i := range k.Slots {
		slot := &k.Slots[i]

		err := slot.validate()
		if err != nil {
			return err
		}

		if ids[slot.ID] || names[slot.Name] {
			return errors.Wrapf(ErrMalformed, "slot %q or name %q repeats", slot.ID, slot.Name)
		}

		ids[slot.ID] = true
		names[slot.Name] = true
	}

	return nil
}

// Unlock returns the master key and the ID of the slot passphrase opened.
// Every failure to open is ErrWrongPassphrase, whatever the slot and the
// reason. The caller destroys the returned key when done with it.
func (k *Keyring) Unlock(passphrase []byte) (*MasterKey, string, error) {
	err := k.Validate()
	if err != nil {
		return nil, "", err
	}

	for i := range k.Slots {
		mk, ok := k.openSlot(&k.Slots[i], passphrase)
		if ok {
			return mk, k.Slots[i].ID, nil
		}
	}

	return nil, "", ErrWrongPassphrase
}

// Opens reports the ID of the slot passphrase opens, without keeping the key.
func (k *Keyring) Opens(passphrase []byte) (string, error) {
	master, slotID, err := k.Unlock(passphrase)
	if err != nil {
		return "", err
	}

	master.Destroy()

	return slotID, nil
}

// AddSlot wraps mk under passphrase in a new slot and returns its ID.
func (k *Keyring) AddSlot(master *MasterKey, passphrase []byte, name, origin string, now time.Time) (string, error) {
	if subtle.ConstantTimeCompare([]byte(master.KeyID()), []byte(k.KeyID)) != 1 {
		return "", ErrKeyMismatch
	}

	if len(k.Slots) >= MaxSlots {
		return "", ErrTooManySlots
	}

	if k.slotIndexByName(name) >= 0 {
		return "", errors.Wrapf(ErrDuplicateName, "%q", name)
	}

	slotID, err := k.freshSlotID()
	if err != nil {
		return "", err
	}

	var random [saltSize + nonceSize]byte

	_, err = io.ReadFull(rand.Reader, random[:])
	if err != nil {
		return "", errors.Wrap(err, "read random slot salt and nonce")
	}

	slot, err := sealSlot(master, passphrase, &slotParams{
		id:     slotID,
		name:   name,
		origin: origin,
		now:    now,
		salt:   random[:saltSize],
		nonce:  random[saltSize:],
	})
	if err != nil {
		return "", err
	}

	k.Slots = append(k.Slots, slot)

	return slot.ID, nil
}

// RemoveSlot removes the slot ref names, by ID or by name. It refuses the
// last slot and the slot protectedID, the one the cluster unlocks with.
func (k *Keyring) RemoveSlot(ref, protectedID string) error {
	idx := k.SlotIndex(ref)
	if idx < 0 {
		return errors.Wrapf(ErrSlotNotFound, "%q", ref)
	}

	if len(k.Slots) == 1 {
		return ErrLastSlot
	}

	if k.Slots[idx].ID == protectedID {
		return ErrProtectedSlot
	}

	k.Slots = slices.Delete(k.Slots, idx, idx+1)

	return nil
}

// Supersede marks the slot id as replaced: it still opens the keyring, and
// is removed once the passphrase Secret opens another slot.
func (k *Keyring) Supersede(id string) error {
	idx := k.slotIndexByID(id)
	if idx < 0 {
		return errors.Wrapf(ErrSlotNotFound, "%q", id)
	}

	k.Slots[idx].Superseded = true

	return nil
}

// RemoveSuperseded drops every superseded slot other than unlockID, the one
// the passphrase Secret opens, and reports whether it removed any. It never
// empties the keyring.
func (k *Keyring) RemoveSuperseded(unlockID string) bool {
	kept := make([]Slot, 0, len(k.Slots))

	for i := range k.Slots {
		if k.Slots[i].Superseded && k.Slots[i].ID != unlockID {
			continue
		}

		kept = append(kept, k.Slots[i])
	}

	if len(kept) == len(k.Slots) || len(kept) == 0 {
		return false
	}

	k.Slots = kept

	return true
}

// SlotIndex returns the index of the slot ref names, by ID first and then
// by name, or -1.
func (k *Keyring) SlotIndex(ref string) int {
	idx := k.slotIndexByID(ref)
	if idx >= 0 {
		return idx
	}

	return k.slotIndexByName(ref)
}

// freshSlotID draws a random slot ID no slot of k has. Four bytes collide
// once in billions of adds, so the loop almost never runs twice.
func (k *Keyring) freshSlotID() (string, error) {
	for {
		var raw [slotIDSize]byte

		_, err := io.ReadFull(rand.Reader, raw[:])
		if err != nil {
			return "", errors.Wrap(err, "read random slot ID")
		}

		slotID := hex.EncodeToString(raw[:])
		if k.slotIndexByID(slotID) < 0 {
			return slotID, nil
		}
	}
}

func (k *Keyring) slotIndexByID(id string) int {
	return slices.IndexFunc(k.Slots, func(s Slot) bool { return s.ID == id })
}

func (k *Keyring) slotIndexByName(name string) int {
	return slices.IndexFunc(k.Slots, func(s Slot) bool { return s.Name == name })
}

// openSlot tries passphrase on one validated slot. It reports only whether
// the slot opened, so no caller can tell a wrong passphrase from a damaged
// slot.
func (k *Keyring) openSlot(slot *Slot, passphrase []byte) (*MasterKey, bool) {
	kek := deriveKEK(passphrase, &slot.KDF)
	defer clear(kek)

	aead, err := newGCM(kek)
	if err != nil {
		return nil, false
	}

	raw, err := aead.Open(nil, slot.Nonce, slot.WrappedKey, slotAAD(slot.ID))
	if err != nil {
		return nil, false
	}

	defer clear(raw)

	master, err := masterKeyFrom(raw)
	if err != nil {
		return nil, false
	}

	// A slot copied in from another keyring opens to another key.
	if subtle.ConstantTimeCompare([]byte(master.KeyID()), []byte(k.KeyID)) != 1 {
		master.Destroy()

		return nil, false
	}

	return master, true
}

type slotParams struct {
	id, name, origin string
	now              time.Time
	salt, nonce      []byte
}

func sealSlot(master *MasterKey, passphrase []byte, params *slotParams) (Slot, error) {
	slot := Slot{
		ID:         params.id,
		Name:       params.name,
		Origin:     params.origin,
		CreatedAt:  params.now.UTC(),
		Superseded: false,
		KDF: KDF{
			Type:        KDFArgon2id,
			Salt:        append([]byte(nil), params.salt...),
			Time:        argonTime,
			MemoryKiB:   argonMemoryKiB,
			Parallelism: argonParallelism,
		},
		AEAD:       AEADAES256GCM,
		Nonce:      append([]byte(nil), params.nonce...),
		WrappedKey: nil,
	}

	err := slot.validateMeta()
	if err != nil {
		return Slot{}, err
	}

	slot.WrappedKey, err = sealMasterKey(master, passphrase, &slot)
	if err != nil {
		return Slot{}, err
	}

	return slot, nil
}

func sealMasterKey(master *MasterKey, passphrase []byte, slot *Slot) ([]byte, error) {
	kek := deriveKEK(passphrase, &slot.KDF)
	defer clear(kek)

	aead, err := newGCM(kek)
	if err != nil {
		return nil, err
	}

	return aead.Seal(nil, slot.Nonce, master.key[:], slotAAD(slot.ID)), nil
}

func deriveKEK(passphrase []byte, kdf *KDF) []byte {
	return argon2.IDKey(passphrase, kdf.Salt, kdf.Time, kdf.MemoryKiB, kdf.Parallelism, MasterKeySize)
}

func slotAAD(id string) []byte {
	return []byte(aadSlot + "\x00" + id)
}

func (s *Slot) validate() error {
	err := s.validateMeta()
	if err != nil {
		return err
	}

	if len(s.WrappedKey) != wrappedKeySize {
		return errors.Wrapf(ErrMalformed, "slot %q: wrapped key is %d bytes", s.ID, len(s.WrappedKey))
	}

	return nil
}

func (s *Slot) validateMeta() error {
	switch {
	case !validHex(s.ID, slotIDSize):
		return errors.Wrapf(ErrMalformed, "slot ID %q", s.ID)
	case s.Name == "":
		return errors.Wrapf(ErrMalformed, "slot %q has no name", s.ID)
	case !slices.Contains([]string{OriginSecret, OriginAPI, OriginGenerated}, s.Origin):
		return errors.Wrapf(ErrMalformed, "slot %q: origin %q", s.ID, s.Origin)
	case s.AEAD != AEADAES256GCM:
		return errors.Wrapf(ErrMalformed, "slot %q: aead %q", s.ID, s.AEAD)
	case len(s.Nonce) != nonceSize:
		return errors.Wrapf(ErrMalformed, "slot %q: nonce is %d bytes", s.ID, len(s.Nonce))
	}

	return s.KDF.validate(s.ID)
}

// validate refuses parameters below the ones new slots get and above the
// ceilings. Lower parameters would only make a weak passphrase cheaper to
// guess; blockstor never writes them, so a slot carrying them was edited.
func (kdf *KDF) validate(slotID string) error {
	switch {
	case kdf.Type != KDFArgon2id:
		return errors.Wrapf(ErrMalformed, "slot %q: kdf %q", slotID, kdf.Type)
	case len(kdf.Salt) != saltSize:
		return errors.Wrapf(ErrMalformed, "slot %q: salt is %d bytes", slotID, len(kdf.Salt))
	case kdf.Time < argonTime || kdf.Time > maxArgonTime,
		kdf.MemoryKiB < argonMemoryKiB || kdf.MemoryKiB > maxArgonMemoryKiB,
		kdf.Parallelism < argonParallelism || kdf.Parallelism > maxArgonParallelism:
		return errors.Wrapf(ErrMalformed, "slot %q: argon2id t=%d m=%dKiB p=%d out of bounds",
			slotID, kdf.Time, kdf.MemoryKiB, kdf.Parallelism)
	}

	return nil
}

func validHex(s string, size int) bool {
	if len(s) != 2*size {
		return false
	}

	_, err := hex.DecodeString(s)

	return err == nil
}
