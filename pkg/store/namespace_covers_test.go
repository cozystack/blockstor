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

// The server-owned guard reads a namespace the way the deleter does: it
// refuses one the key lives in, in every spelling, and lets through a key
// spelled as a namespace, which the deleter leaves alone.
func TestServerOwnedPropEditReadsNamespacesAsTheDeleterDoes(t *testing.T) {
	t.Parallel()

	for _, ns := range []string{"Blockstor", "Blockstor/", "Blockstor//"} {
		if got := store.ServerOwnedPropEdit(nil, nil, []string{ns}); got == "" {
			t.Errorf("delete_namespaces=[%q] over the server-owned props was let through", ns)
		}
	}

	if got := store.ServerOwnedPropEdit(nil, nil, []string{store.RestoreFromSnapshotProp}); got != "" {
		t.Errorf("a namespace spelled as the key %q was refused as %q; the deleter does not remove it",
			store.RestoreFromSnapshotProp, got)
	}
}
