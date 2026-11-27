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

package contract_test

import (
	"os"
	"regexp"
	"testing"
)

// knownDeltaRow matches a row of the accept-list table and captures its
// id.
var knownDeltaRow = regexp.MustCompile(`(?m)^\| *([0-9]+) *\|`)

// Rows of docs/cli-parity-known-deltas.md are cited by id from code
// comments, tests and other docs, and cli-parity-refresh.sh accepts a
// report row whose id appears in the file. Two rows under one id make
// every such citation ambiguous, which is what several branches adding
// rows at the next free id at the same time produce.
func TestKnownDeltaRowIDsAreUnique(t *testing.T) {
	t.Parallel()

	doc, err := os.ReadFile("../../docs/cli-parity-known-deltas.md")
	if err != nil {
		t.Fatal(err)
	}

	seen := map[string]bool{}

	for _, m := range knownDeltaRow.FindAllStringSubmatch(string(doc), -1) {
		if seen[m[1]] {
			t.Errorf("row id %s is used by more than one row", m[1])
		}

		seen[m[1]] = true
	}

	if len(seen) == 0 {
		t.Fatal("found no rows; has the table format changed?")
	}
}
