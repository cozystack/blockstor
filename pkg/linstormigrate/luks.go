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

package linstormigrate

import (
	"bytes"
	"crypto/aes"
	"crypto/pbkdf2"
	"crypto/sha1" //nolint:gosec // LINSTOR's KDF is PBKDF2-HMAC-SHA1; only the importer reads it
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/errors"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/keyring"
)

// LINSTOR's scheme for the master key and the volume passwords, as
// linstor-server v1.33.1 implements it (docs/encryption-keys.md, section
// 2.3). It is read here and nowhere else: the passwords are re-wrapped
// under blockstor's own master key at import.
const (
	linstorKDFIterations = 5000
	linstorKeySize       = 16
	linstorIVSize        = aes.BlockSize
	linstorPadBlock      = 32
	linstorMasterKeyProp = "encrypted/masterkey"
	linstorSaltProp      = "encrypted/passphrasesalt"
	linstorHashProp      = "encrypted/masterhash"
)

// linstorPropsInstances are where the controller keeps the three
// properties: /CTRL, and /CTRLCFG before LINSTOR's 2024-10-24 migration.
//
//nolint:gochecknoglobals // a fixed lookup list
var linstorPropsInstances = []string{"/CTRL", "/CTRLCFG"}

var (
	// ErrLUKSNeedsPassphrase is a dump with LUKS volumes converted
	// without the LINSTOR master passphrase: the volumes' passwords
	// could not be carried over, and they would never open.
	ErrLUKSNeedsPassphrase = errors.New(
		"the dump has LUKS volumes: pass the LINSTOR master passphrase with -linstor-passphrase-file")

	// ErrLinstorWrongPassphrase is a passphrase whose derived key does
	// not decrypt LINSTOR's master key to the stored hash.
	ErrLinstorWrongPassphrase = errors.New("the LINSTOR master passphrase does not match the dump's master key")

	// ErrLinstorNoMasterKey is a dump with LUKS volumes and no
	// encrypted master key in the controller properties.
	ErrLinstorNoMasterKey = errors.New("the dump has no LINSTOR master key (encrypted/masterkey)")

	// ErrLinstorBadPassword is a volume password that does not decrypt
	// to a well-formed padded block. CFB has no MAC, so a good-looking
	// result is no proof; the satellite opening the volume is.
	ErrLinstorBadPassword = errors.New("a LINSTOR volume password does not decrypt to a padded block")
)

// LUKSImport is what carrying LINSTOR's LUKS volume passwords over needs.
type LUKSImport struct {
	// LinstorPassphrase is the LINSTOR master passphrase, the value of
	// MASTER_PASSPHRASE in the piraeus Secret.
	LinstorPassphrase []byte

	// MasterKey is the blockstor master key the passwords are wrapped
	// under.
	MasterKey *keyring.MasterKey
}

// LinstorMasterKey recovers LINSTOR's master key from the controller
// properties of the dump with passphrase, and checks it against the
// stored hash. The caller clears the key.
func LinstorMasterKey(props []PropsContainerRow, passphrase []byte) ([]byte, error) {
	values := map[string]string{}

	for _, row := range props {
		for _, instance := range linstorPropsInstances {
			if strings.EqualFold(row.PropsInstance, instance) {
				values[strings.ToLower(row.PropKey)] = row.PropValue
			}
		}
	}

	if values[linstorMasterKeyProp] == "" || values[linstorSaltProp] == "" || values[linstorHashProp] == "" {
		return nil, ErrLinstorNoMasterKey
	}

	encrypted, errKey := base64.StdEncoding.DecodeString(values[linstorMasterKeyProp])
	salt, errSalt := base64.StdEncoding.DecodeString(values[linstorSaltProp])
	hash, errHash := base64.StdEncoding.DecodeString(values[linstorHashProp])

	err := errors.CombineErrors(errKey, errors.CombineErrors(errSalt, errHash))
	if err != nil {
		return nil, errors.Wrap(err, "decode the LINSTOR master key properties")
	}

	kek, err := pbkdf2.Key(sha1.New, string(javaPassphraseBytes(passphrase)), salt, linstorKDFIterations, linstorKeySize)
	if err != nil {
		return nil, errors.Wrap(err, "derive the LINSTOR key-encryption key")
	}

	defer clear(kek)

	padded, err := linstorDecrypt(kek, encrypted)
	if err != nil {
		return nil, err
	}

	defer clear(padded)

	sum := sha512.Sum512(padded)
	if subtle.ConstantTimeCompare(sum[:], hash) != 1 {
		return nil, ErrLinstorWrongPassphrase
	}

	master, err := linstorUnpad(padded)
	if err != nil || len(master) != linstorKeySize {
		clear(master)

		return nil, errors.Wrap(ErrLinstorWrongPassphrase, "the master key block is malformed")
	}

	return master, nil
}

// LinstorVolumePassword decrypts one LAYER_LUKS_VOLUMES.ENCRYPTED_PASSWORD
// under LINSTOR's master key. The caller clears the password.
func LinstorVolumePassword(master []byte, encoded string) ([]byte, error) {
	encrypted, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, errors.Wrap(err, "decode the encrypted volume password")
	}

	padded, err := linstorDecrypt(master, encrypted)
	if err != nil {
		return nil, err
	}

	defer clear(padded)

	return linstorUnpad(padded)
}

// linstorDecrypt is AES-CFB with a 128-bit segment, the 16-byte IV first,
// no MAC: LINSTOR's `AES/CFB/NoPadding`.
func linstorDecrypt(key, blob []byte) ([]byte, error) {
	if len(blob) <= linstorIVSize {
		return nil, errors.Wrapf(ErrLinstorBadPassword, "%d bytes", len(blob))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.Wrap(err, "new AES cipher")
	}

	prev := bytes.Clone(blob[:linstorIVSize])
	sealed := blob[linstorIVSize:]
	out := make([]byte, len(sealed))
	stream := make([]byte, aes.BlockSize)

	for off := 0; off < len(sealed); off += aes.BlockSize {
		block.Encrypt(stream, prev)

		end := min(off+aes.BlockSize, len(sealed))
		for i := off; i < end; i++ {
			out[i] = sealed[i] ^ stream[i-off]
		}

		prev = sealed[off:end]
	}

	clear(stream)

	return out, nil
}

// linstorUnpad strips LINSTOR's length padding: a 2-byte big-endian
// length, the data, random fill to a multiple of 32.
func linstorUnpad(padded []byte) ([]byte, error) {
	if len(padded) < 2 || len(padded)%linstorPadBlock != 0 {
		return nil, errors.Wrapf(ErrLinstorBadPassword, "a %d-byte block", len(padded))
	}

	size := int(binary.BigEndian.Uint16(padded))
	if size == 0 || 2+size > len(padded) {
		return nil, errors.Wrapf(ErrLinstorBadPassword, "length %d in a %d-byte block", size, len(padded))
	}

	return bytes.Clone(padded[2 : 2+size]), nil
}

// javaPassphraseBytes is what the JDK hands PBKDF2 for LINSTOR's
// passphrase: LINSTOR turns each byte into a char by sign extension
// (JclSymmetricKeyCipher.java:150-154), and the JDK's PBKDF2 encodes those
// chars as UTF-8. ASCII passes through unchanged; TestLinstorJDKVectors
// pins a non-ASCII passphrase against a real JDK.
func javaPassphraseBytes(passphrase []byte) []byte {
	out := make([]byte, 0, len(passphrase))

	for _, b := range passphrase {
		char := rune(uint16(int16(int8(b)))) //nolint:gosec // the sign extension is the point
		out = utf8.AppendRune(out, char)
	}

	return out
}

// importedLUKSKey wraps a LINSTOR volume password under blockstor's master
// key, for a Resource or a Snapshot.
func importedLUKSKey(master *keyring.MasterKey, password []byte) (crdv1alpha1.LUKSKey, error) {
	wrapped, err := master.WrapVolumePassword(password)
	if err != nil {
		return crdv1alpha1.LUKSKey{}, errors.Wrap(err, "wrap an imported volume password")
	}

	return crdv1alpha1.LUKSKey{KeyID: wrapped.KeyID, Wrapped: wrapped.Wrapped, Origin: crdv1alpha1.LUKSKeyOriginLinstorImport}, nil
}
