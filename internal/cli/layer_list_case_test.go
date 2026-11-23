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

package cli

import (
	"slices"
	"testing"
)

// The CLI writes the CRDs directly, so it stores the stack in the same
// canonical case as the REST doors.
func TestParseLayerListStoresCanonicalCase(t *testing.T) {
	t.Parallel()

	got, err := parseLayerList("drbd,Luks,storage")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	want := []string{"DRBD", "LUKS", "STORAGE"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
