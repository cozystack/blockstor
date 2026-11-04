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

package rest

import "testing"

// A list filter names objects the way the store addresses them: PVC-X finds
// pvc-x, and my_res does not find My_Res, a definition of its own.
func TestMatchAnyNameMatchesByKey(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		filter, value string
		want          bool
	}{
		{"PVC-X", "pvc-x", true},
		{"my_res", "My_Res", false},
		{"My_Res", "My_Res", true},
		{"pvc-y", "pvc-x", false},
	} {
		if got := matchAnyName([]string{tc.filter}, tc.value); got != tc.want {
			t.Errorf("matchAnyName(%q, %q) = %v, want %v", tc.filter, tc.value, got, tc.want)
		}
	}
}
