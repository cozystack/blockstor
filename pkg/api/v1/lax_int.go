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

package v1

import (
	"strconv"

	"github.com/cockroachdb/errors"
)

// LaxInt32 unmarshals both `42` and `"42"` from JSON. Upstream LINSTOR's
// CLI (linstor-client) marshals integer flags as quoted strings even
// though the OpenAPI spec types them as int32 — strictly-typed Go
// decoders bail with "cannot unmarshal string into Go struct field …
// of type int32" otherwise.
//
// On marshal we always emit the bare integer form so a request
// round-tripped through blockstor matches the spec, not the CLI's
// quirk.
type LaxInt32 int32

// UnmarshalJSON accepts either a JSON number or a JSON string that
// parses as an int32. Empty / null values decode to zero.
func (i *LaxInt32) UnmarshalJSON(data []byte) error {
	if isJSONNull(data) {
		*i = 0

		return nil
	}

	// Strip surrounding double quotes if the value is JSON-string-encoded.
	if data[0] == '"' && data[len(data)-1] == '"' {
		data = data[1 : len(data)-1]
	}

	if len(data) == 0 {
		*i = 0

		return nil
	}

	n, err := strconv.ParseInt(string(data), 10, 32)
	if err != nil {
		return errors.Wrapf(err, "LaxInt32: parse %q", string(data))
	}

	*i = LaxInt32(n)

	return nil
}

// MarshalJSON emits the bare integer form (no quotes) so blockstor's
// own responses don't propagate the CLI's stringified-integer quirk.
func (i LaxInt32) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatInt(int64(i), 10)), nil
}

// isJSONNull reports whether data is empty or the literal JSON `null`.
func isJSONNull(data []byte) bool {
	if len(data) == 0 {
		return true
	}

	const null = "null"

	return len(data) == len(null) &&
		data[0] == 'n' && data[1] == 'u' && data[2] == 'l' && data[3] == 'l'
}
