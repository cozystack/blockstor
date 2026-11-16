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

// Package passphrase is the cluster's master passphrase plumbing: the
// passphrase Secret it unlocks from (piraeus's shape, MASTER_PASSPHRASE,
// or a Secret blockstor generates), the keyring Secret that holds the
// master key's slots, the unlocked state every component derives from the
// two, and the passphrase operations the REST verbs and the CLI share.
//
// It also refuses the properties that once carried a passphrase in
// plaintext: no passphrase is ever read from a property.
package passphrase

import (
	"slices"
	"strings"
)

// PropKeyCanonical is upstream LINSTOR's cluster-scope master-key
// property name (`linstor controller set-property
// DrbdOptions/EncryptPassphrase <pass>`). blockstor never reads the
// passphrase from a property: a property is stored in plaintext on its
// CR, and any dump of the CRs for a backup would carry the key to every
// LUKS volume. The REST API refuses to store it, and the satellite drops
// it from a definition's properties before it adds the Secret's value
// under this key for the dispatcher.
const PropKeyCanonical = "DrbdOptions/EncryptPassphrase"

// PropKeyLegacy is the early per-RD alias of PropKeyCanonical. It is
// refused and dropped the same way.
const PropKeyLegacy = "DrbdOptions/Encryption/passphrase"

// PropKeyVolume is upstream LINSTOR's per-volume passphrase property,
// stamped by `linstor vd modify-passphrase`. blockstor has no per-volume
// keys: every LUKS volume uses the cluster passphrase, so the door that
// wrote it is refused and the key is refused and dropped like the others.
const PropKeyVolume = "DrbdOptions/Encrypt/Passphrase"

// WirePropKey is the property the satellite's LUKS layer once read the
// cluster passphrase from. Nothing reads it now that every volume has its
// own password; a property of that name is still refused and dropped.
const WirePropKey = "LuksPassphrase"

// IsPlaintextPropKey reports whether key would carry the master
// passphrase in a property. Matched without regard to case, so no
// spelling slips through.
func IsPlaintextPropKey(key string) bool {
	for _, k := range []string{PropKeyCanonical, PropKeyLegacy, PropKeyVolume, WirePropKey} {
		if strings.EqualFold(key, k) {
			return true
		}
	}

	return false
}

// DropPlaintextProps removes every property that would carry the
// master passphrase from props, and returns the keys it removed that
// held a value, sorted, for the caller to name in a log line.
func DropPlaintextProps(props map[string]string) []string {
	var dropped []string

	for key, value := range props {
		if IsPlaintextPropKey(key) {
			delete(props, key)

			if value != "" {
				dropped = append(dropped, key)
			}
		}
	}

	slices.Sort(dropped)

	return dropped
}
