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

package drbd

import (
	"crypto/rand"
	"encoding/base64"
	"strings"
)

const (
	// SharedSecretProp is the property carrying a definition's DRBD peer
	// authentication secret; the satellite renders it as the net
	// section's shared-secret.
	SharedSecretProp = PropPrefix + "Net/" + sharedSecretOption
	// CramHMACAlgProp is the property naming the digest DRBD
	// authenticates peers with; without it a shared secret is inert.
	CramHMACAlgProp = PropPrefix + "Net/cram-hmac-alg"
)

// sharedSecretBytes is how much randomness a generated secret carries.
// LINSTOR draws 15 bytes and base64-encodes them, which gives a
// 20-character secret with no padding; the same shape keeps a definition
// blockstor created indistinguishable from one LINSTOR did.
const sharedSecretBytes = 15

// GenerateSharedSecret returns a fresh random peer authentication secret
// in the shape LINSTOR generates for every definition it creates.
func GenerateSharedSecret() (string, error) {
	buf := make([]byte, sharedSecretBytes)

	_, err := rand.Read(buf)
	if err != nil {
		return "", err //nolint:wrapcheck // crypto/rand names its own failure
	}

	return base64.StdEncoding.EncodeToString(buf), nil
}

// DropSharedSecret removes the shared secret a definition's props were
// copied with. A clone or a restore is a new DRBD resource with its own
// connections, and LINSTOR gives it its own secret rather than the
// source's.
func DropSharedSecret(props map[string]string) {
	for key := range props {
		if strings.EqualFold(key, SharedSecretProp) {
			delete(props, key)
		}
	}
}

// HasPropFold reports whether props names key in any letter case.
func HasPropFold(props map[string]string, key string) bool {
	for name := range props {
		if strings.EqualFold(name, key) {
			return true
		}
	}

	return false
}
