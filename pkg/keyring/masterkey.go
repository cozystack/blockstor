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

// Package keyring holds the cluster master key that wraps the password of
// every LUKS replica, and the passphrase slots that unlock it.
//
// The master key is 32 random bytes and is never derived from a passphrase,
// so a passphrase can be added, removed or changed without touching a single
// volume. Each slot wraps the master key under a key derived from one
// passphrase with Argon2id. Two sub-keys come from the master key through
// HKDF-SHA256: a public key ID that names the key a volume password was
// wrapped under, and the key that wraps the volume passwords themselves.
// Both wraps are AES-256-GCM, so a wrong passphrase, a wrong key or a
// tampered blob fails the tag check rather than yielding garbage.
//
// The design is docs/encryption-keys.md.
package keyring

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"io"

	"github.com/cockroachdb/errors"
)

const (
	// MasterKeySize is the length of the master key in bytes.
	MasterKeySize = 32

	// keyIDSize is the length of the public key ID in bytes.
	keyIDSize = 8

	// nonceSize is the AES-GCM nonce length, the standard 96 bits.
	nonceSize = 12

	// passwordBytes is the entropy of a generated volume password.
	passwordBytes = 32

	// frameBlock is the multiple a framed volume password is padded to,
	// so a wrapped blob does not reveal the length of an imported,
	// user-chosen password.
	frameBlock = 64

	// frameMax is the longest password the framing can carry.
	frameMax = 0xffff

	infoKeyID   = "blockstor.org/key-id/v1"
	infoWrapKey = "blockstor.org/volume-key-wrap/v1"
	aadVolume   = "blockstor.org/luks-volume-key/v1"
)

var (
	// ErrKeyIDMismatch is a wrapped volume password that names another
	// master key than the one it is unwrapped with.
	ErrKeyIDMismatch = errors.New("the volume password is wrapped under another master key")

	// ErrUnwrap is a wrapped volume password that does not open under the
	// master key it names: tampered, truncated, or not one of ours.
	ErrUnwrap = errors.New("the volume password does not unwrap")

	// ErrPasswordTooLong is a volume password the framing cannot carry.
	ErrPasswordTooLong = errors.New("the volume password is too long")

	// ErrMasterKeySize is raw key material of the wrong length.
	ErrMasterKeySize = errors.New("the master key has the wrong length")
)

// MasterKey is the unlocked cluster master key. It is held only in memory;
// Destroy clears it once the holder is done with it.
type MasterKey struct {
	key     [MasterKeySize]byte
	keyID   string
	wrapKey [MasterKeySize]byte
}

// WrappedKey is one volume password wrapped under a master key: the key's
// public ID and nonce || ciphertext || tag.
type WrappedKey struct {
	KeyID   string
	Wrapped []byte
}

// NewMasterKey returns a fresh random master key.
func NewMasterKey() (*MasterKey, error) {
	var raw [MasterKeySize]byte

	_, err := io.ReadFull(rand.Reader, raw[:])
	if err != nil {
		return nil, errors.Wrap(err, "read random master key")
	}

	defer clear(raw[:])

	return masterKeyFrom(raw[:])
}

// masterKeyFrom derives the sub-keys of raw, which it copies.
func masterKeyFrom(raw []byte) (*MasterKey, error) {
	if len(raw) != MasterKeySize {
		return nil, errors.Wrapf(ErrMasterKeySize, "%d bytes, want %d", len(raw), MasterKeySize)
	}

	master := &MasterKey{key: [MasterKeySize]byte{}, keyID: "", wrapKey: [MasterKeySize]byte{}}
	copy(master.key[:], raw)

	keyID, err := hkdf.Key(sha256.New, raw, nil, infoKeyID, keyIDSize)
	if err != nil {
		return nil, errors.Wrap(err, "derive key ID")
	}

	master.keyID = hex.EncodeToString(keyID)

	wrapKey, err := hkdf.Key(sha256.New, raw, nil, infoWrapKey, MasterKeySize)
	if err != nil {
		master.Destroy()

		return nil, errors.Wrap(err, "derive wrap key")
	}

	copy(master.wrapKey[:], wrapKey)
	clear(wrapKey)

	return master, nil
}

// KeyID is the public name of the key: it can be stored next to every
// wrapped password without revealing anything about the key.
func (mk *MasterKey) KeyID() string {
	return mk.keyID
}

// Destroy clears the key material. The key is unusable afterwards.
func (mk *MasterKey) Destroy() {
	clear(mk.key[:])
	clear(mk.wrapKey[:])
}

// GeneratePassword returns a new random volume password: 32 random bytes as
// unpadded base64url, 43 ASCII characters, so an operator can still type it
// into cryptsetup in a recovery shell.
func GeneratePassword() ([]byte, error) {
	var raw [passwordBytes]byte

	_, err := io.ReadFull(rand.Reader, raw[:])
	if err != nil {
		return nil, errors.Wrap(err, "read random volume password")
	}

	defer clear(raw[:])

	out := make([]byte, base64.RawURLEncoding.EncodedLen(passwordBytes))
	base64.RawURLEncoding.Encode(out, raw[:])

	return out, nil
}

// WrapVolumePassword wraps password under the master key.
func (mk *MasterKey) WrapVolumePassword(password []byte) (WrappedKey, error) {
	var nonce [nonceSize]byte

	_, err := io.ReadFull(rand.Reader, nonce[:])
	if err != nil {
		return WrappedKey{}, errors.Wrap(err, "read random nonce")
	}

	return mk.wrapVolumePassword(password, nonce[:])
}

// UnwrapVolumePassword returns the password wrapped in wrapped. The caller
// clears it once it has handed it on.
func (mk *MasterKey) UnwrapVolumePassword(wrapped WrappedKey) ([]byte, error) {
	if subtle.ConstantTimeCompare([]byte(wrapped.KeyID), []byte(mk.keyID)) != 1 {
		return nil, errors.Wrapf(ErrKeyIDMismatch, "wrapped under %q, unlocked key is %q", wrapped.KeyID, mk.keyID)
	}

	aead, err := newGCM(mk.wrapKey[:])
	if err != nil {
		return nil, err
	}

	if len(wrapped.Wrapped) < nonceSize+aead.Overhead() {
		return nil, ErrUnwrap
	}

	nonce, sealed := wrapped.Wrapped[:nonceSize], wrapped.Wrapped[nonceSize:]

	framed, err := aead.Open(nil, nonce, sealed, volumeAAD(mk.keyID))
	if err != nil {
		return nil, ErrUnwrap
	}

	defer clear(framed)

	return unframe(framed)
}

func (mk *MasterKey) wrapVolumePassword(password, nonce []byte) (WrappedKey, error) {
	framed, err := frame(password)
	if err != nil {
		return WrappedKey{}, err
	}

	defer clear(framed)

	aead, err := newGCM(mk.wrapKey[:])
	if err != nil {
		return WrappedKey{}, err
	}

	out := make([]byte, 0, nonceSize+len(framed)+aead.Overhead())
	out = append(out, nonce...)
	out = aead.Seal(out, nonce, framed, volumeAAD(mk.keyID))

	return WrappedKey{KeyID: mk.keyID, Wrapped: out}, nil
}

func volumeAAD(keyID string) []byte {
	return []byte(aadVolume + "\x00" + keyID)
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.Wrap(err, "new AES cipher")
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.Wrap(err, "new GCM")
	}

	return aead, nil
}

// frame is a 2-byte big-endian length, the password, and zero fill to a
// multiple of frameBlock.
func frame(password []byte) ([]byte, error) {
	if len(password) > frameMax {
		return nil, ErrPasswordTooLong
	}

	size := (2 + len(password) + frameBlock - 1) / frameBlock * frameBlock
	out := make([]byte, size)
	binary.BigEndian.PutUint16(out, uint16(len(password))) //nolint:gosec // bounded by frameMax above
	copy(out[2:], password)

	return out, nil
}

func unframe(framed []byte) ([]byte, error) {
	if len(framed) < 2 || len(framed)%frameBlock != 0 {
		return nil, ErrUnwrap
	}

	size := int(binary.BigEndian.Uint16(framed))
	if 2+size > len(framed) {
		return nil, ErrUnwrap
	}

	for _, b := range framed[2+size:] {
		if b != 0 {
			return nil, ErrUnwrap
		}
	}

	return append([]byte(nil), framed[2:2+size]...), nil
}
