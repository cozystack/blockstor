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

package store_test

import (
	"testing"

	"github.com/cozystack/blockstor/pkg/store"
)

// A restore marker names a definition and a snapshot. Each half compares as
// the store addresses the object, not as one folded string: PVC-X and pvc-x
// are one definition, My_Res and my_res are two.
func TestSameRestoreMarker(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		a, b string
		want bool
	}{
		{"pvc-x:snap-1", "pvc-x:snap-1", true},
		{"PVC-X:Snap-1", "pvc-x:snap-1", true},
		{"My_Res:snap-1", "my_res:snap-1", false},
		{"My_Res:snap-1", "My_Res:snap-1", true},
		{"pvc-x:snap-1", "pvc-x:snap-2", false},
		{"pvc-x:snap-1", "pvc-x", false},
		{"", "pvc-x:snap-1", false},
	} {
		if got := store.SameRestoreMarker(tc.a, tc.b); got != tc.want {
			t.Errorf("SameRestoreMarker(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}
