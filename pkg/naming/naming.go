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

// Package naming holds the one identity LINSTOR names have in blockstor.
//
// LINSTOR names are case-insensitive, and the API server takes only
// RFC 1123 names, so every object is stored under the key Key gives its
// name: the lowercase name when that is a valid RFC 1123 name, and a short
// hash of the name as given, followed by a slug, otherwise. The key is the
// object's CR name. Node-1 and node-1 are one node; My_Res and my_res hash
// to two keys and are two definitions, as they are two objects in the store.
//
// A name arrives in several spellings: as an operator or a client typed it,
// as it was registered (kept in annotations for display), as a child row
// stored it in spec, and as the CR name itself. Key maps every one of them to
// the same key, and is idempotent on a key, so code above the store compares
// and indexes node, definition, group, pool and snapshot names only through
// Key or Same, never by folding case or by comparing two spellings as they
// are. A test in this package walks the source and fails on anything else.
package naming

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

// rfc1123 is the API server's metadata.Name rule.
var rfc1123 = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

const (
	// maxName is the metadata.Name length cap.
	maxName = 253

	// hashPrefixBytes is the length of the hash a name outside RFC 1123
	// is keyed by: 8 hex digits.
	hashPrefixBytes = 4
)

// Key is the identity of a LINSTOR name: the CR name the store keeps the
// object under. It lowercases first, and returns that when it is a valid
// RFC 1123 name. Otherwise it replaces every run of other characters with
// '-' and prefixes a hash of the name as given, so two names that differ
// only in case outside RFC 1123 stay two keys.
func Key(in string) string {
	lower := strings.ToLower(in)

	if lower != "" && len(lower) <= maxName && rfc1123.MatchString(lower) {
		return lower
	}

	var builder strings.Builder

	prevDash := true

	for _, r := range lower {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			builder.WriteRune(r)

			prevDash = false
		default:
			if !prevDash {
				builder.WriteByte('-')

				prevDash = true
			}
		}
	}

	slug := strings.Trim(builder.String(), "-")

	digest := sha256.Sum256([]byte(in))
	out := hex.EncodeToString(digest[:hashPrefixBytes])

	// A key that ends on a dash, from a name with no letter or digit or from
	// a cut, is not a name the API server takes, and Key of it would hash it
	// again. No stored object carries one, so leaving the dash out keeps
	// Key idempotent on every key it returns and changes no existing name.
	if slug != "" {
		out += "-" + slug
	}

	if len(out) > maxName {
		out = strings.TrimRight(out[:maxName], "-")
	}

	return out
}

// Same reports whether two spellings name the same object: whether they
// have the same Key.
func Same(a, b string) bool {
	return Key(a) == Key(b)
}
