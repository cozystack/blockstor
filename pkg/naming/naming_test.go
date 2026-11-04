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

package naming_test

import (
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/naming"
)

func TestKey(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ in, want string }{
		{"pvc-1", "pvc-1"},
		{"PVC-1", "pvc-1"},
		{"Node-1", "node-1"},
		{"DfltRscGrp", "dfltrscgrp"},
	} {
		if got := naming.Key(tc.in); got != tc.want {
			t.Errorf("Key(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// Names outside RFC 1123 hash the name as given, so two case variants are
// two keys, as they are two objects in the store.
func TestKeyKeepsCaseVariantsOutsideRFC1123Apart(t *testing.T) {
	t.Parallel()

	if naming.Same("My_Res", "my_res") {
		t.Error("My_Res and my_res share a key")
	}

	if !naming.Same("My_Res", "My_Res") || !naming.Same("PVC-X", "pvc-x") {
		t.Error("one name in two RFC 1123 spellings, or one spelling twice, is two keys")
	}
}

// Key is idempotent: a key is its own key, so every spelling, a key
// included, compares the same way. A long name cut on a dash would not be.
func TestKeyIsIdempotent(t *testing.T) {
	t.Parallel()

	for _, in := range []string{
		"pvc-1", "Node-1", "My_Res", "my_res", "a.b_C",
		strings.Repeat("ab_", 100),
		strings.Repeat("a", 243) + "_" + strings.Repeat("b", 20),
	} {
		key := naming.Key(in)
		if again := naming.Key(key); again != key {
			t.Errorf("Key(Key(%q)) = %q, want %q", in, again, key)
		}

		if len(key) > 253 {
			t.Errorf("Key(%q) is %d long", in, len(key))
		}
	}
}
