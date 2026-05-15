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

package rest

import (
	"strings"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// redactedPropValue replaces a sensitive property value at the REST
// boundary. We preserve the key so tooling like `linstor c lp` still
// shows that the property is set (operators expect to see the row),
// but the secret material never leaves the controller process.
//
// Bug 115: prior to this gate, `DrbdOptions/EncryptPassphrase` was
// surfaced verbatim through every read-side props path the
// inheritance walk touched (`c lp`, `rd lp`, `GET /v1/resource-
// definitions/{rd}`, `/v1/view/resources`). Anyone with read-only
// LINSTOR access could extract the LUKS master key for every
// encrypted volume in the cluster.
const redactedPropValue = "<redacted>"

// sensitivePropSubstrings is the case-insensitive substring deny
// list applied to every key whose value flows out through a REST
// read handler. A key is sensitive when its lower-cased form
// contains any of these tokens. Matched at the substring level so
// upstream-LINSTOR-spawned aliases (e.g. `Aux/encrypt-passphrase`,
// `DrbdOptions/Net/shared-secret`) don't need an exact-name entry
// to be covered.
//
// sensitivePropTokenSecret is the bare "secret" substring used by
// the deny list. Pulled into a const so the linter's goconst pass
// (which counts >=4 raw `"secret"` literals across the package as
// a smell) collapses to a single shared symbol; it also keeps the
// shared-secret entry below readable as a composition.
const sensitivePropTokenSecret = "secret"

//nolint:gochecknoglobals // immutable allowlist, read-only after init
var sensitivePropSubstrings = []string{
	passphraseSecretKey,
	"password",
	sensitivePropTokenSecret,
	"cram-hmac-alg",
	"shared-" + sensitivePropTokenSecret,
	"encrypt",
}

// isSensitivePropKey returns true when `key` matches any deny-list
// substring (case-insensitive). Empty input is non-sensitive — the
// caller has nothing to redact.
func isSensitivePropKey(key string) bool {
	if key == "" {
		return false
	}

	lower := strings.ToLower(key)
	for _, token := range sensitivePropSubstrings {
		if strings.Contains(lower, token) {
			return true
		}
	}

	return false
}

// redactSensitiveProps walks a string→string property map and
// rewrites every value whose key is on the deny list to
// `redactedPropValue`. Mutates the input map in place — callers
// that need the original bag (the LUKS-prereq gate, the dispatcher's
// satellite-side reads) must hold their own reference BEFORE this
// runs. A nil map is a no-op.
//
// Idempotent: a second pass over an already-redacted map is cheap
// and produces the same shape.
func redactSensitiveProps(props map[string]string) {
	if props == nil {
		return
	}

	for k := range props {
		if isSensitivePropKey(k) {
			props[k] = redactedPropValue
		}
	}
}

// redactSensitiveEffectiveProps is the sibling for the
// scope-annotated EffectiveProperties bag exposed on RD-get,
// RD-list, and `/v1/view/resources`. Same deny-list, same
// `<redacted>` replacement; the scope tag is preserved so the
// python CLI's `(R)` inheritance marker still renders.
func redactSensitiveEffectiveProps(eff apiv1.EffectiveProperties) {
	if eff == nil {
		return
	}

	for k, entry := range eff {
		if isSensitivePropKey(k) {
			entry.Value = redactedPropValue
			eff[k] = entry
		}
	}
}
