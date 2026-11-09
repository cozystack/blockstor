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

package drbd

import "testing"

// A connection stored as Node-1/beta renders between the hosts node-1 and
// beta, in either order.
func TestLookupPathsMatchesAConnectionInAnotherSpelling(t *testing.T) {
	t.Parallel()

	conns := []ResourceConnection{{
		NodeA: "Node-1", NodeB: "beta",
		Paths: []ResourcePath{{Name: "p1", AddressA: "10.0.0.1", AddressB: "10.0.0.2"}},
	}}

	if got := lookupPaths(conns, "node-1", "beta"); len(got) != 1 || got[0].AddressA != "10.0.0.1" {
		t.Errorf("node-1, beta: %+v", got)
	}

	if got := lookupPaths(conns, "beta", "node-1"); len(got) != 1 || got[0].AddressA != "10.0.0.2" {
		t.Errorf("beta, node-1: %+v", got)
	}
}
