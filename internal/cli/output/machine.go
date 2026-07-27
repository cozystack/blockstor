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

// Package output writes the CLI's non-table output formats.
//
// The `--machine-readable` shape is a contract with everything that
// consumes this CLI programmatically: the jq expressions across
// tests/e2e/cli-matrix and tests/operator-harness, and the integration
// harness. List payloads are DOUBLE-nested — `[[obj, ...]]` — which is
// what the `.[][]?` and `.[0][]?` paths in those scripts expect;
// singletons are flat.
package output

import (
	"encoding/json"
	"fmt"
	"io"
)

// MachineList writes a list payload in the double-nested envelope.
// An empty list still emits `[[]]`, so a jq path yields nothing
// instead of erroring on a null.
func MachineList[T any](w io.Writer, items []T) error {
	if items == nil {
		items = []T{}
	}

	return encode(w, []any{items})
}

// MachineSingle writes a singleton payload in the flat envelope.
func MachineSingle(w io.Writer, item any) error {
	return encode(w, []any{item})
}

// encode writes compact JSON followed by a newline. Machine output is
// consumed by jq, so it carries no colour and no indentation.
func encode(w io.Writer, payload any) error {
	enc := json.NewEncoder(w)

	err := enc.Encode(payload)
	if err != nil {
		return fmt.Errorf("encode machine-readable output: %w", err)
	}

	return nil
}
