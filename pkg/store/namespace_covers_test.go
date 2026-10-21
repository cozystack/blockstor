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

func TestNamespaceCovers(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		ns, key string
		want    bool
	}{
		{"DrbdOptions", "DrbdOptions/Net/protocol", true},
		{"DrbdOptions/", "DrbdOptions/Net/protocol", true},
		{"DrbdOptions//", "DrbdOptions/Net/protocol", true},
		{"DrbdOptions", "DrbdOptions", false},
		{"DrbdOptions/", "DrbdOptions", false},
		{"DrbdOptions", "DrbdOptionsOther", false},
		{"", "DrbdOptions/Net/protocol", false},
		{"/", "DrbdOptions/Net/protocol", false},
	} {
		if got := store.NamespaceCovers(tc.ns, tc.key); got != tc.want {
			t.Errorf("NamespaceCovers(%q, %q) = %v, want %v", tc.ns, tc.key, got, tc.want)
		}
	}
}
