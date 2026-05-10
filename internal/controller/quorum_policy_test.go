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

package controller_test

import (
	"testing"

	controllerpkg "github.com/cozystack/blockstor/internal/controller"
)

// TestQuorumPolicy mirrors upstream LINSTOR's isQuorumFeasible
// decision so a cluster running blockstor sees identical
// majority/off transitions as one running upstream — important for
// the cozystack migration story (operators must be able to flip
// between implementations without quorum behaviour changing).
//
// Truth table (diskful = NOT-DISKLESS replicas; diskless = total
// DISKLESS including TIE_BREAKER witnesses):
//
//	0 diskful → off (no replicas at all is a degenerate case)
//	1 diskful → off (no quorum possible without majority)
//	2 diskful + 0 diskless → off (split-brain on partition: both halves
//	                              have 1 of 2 = no majority)
//	2 diskful + ≥1 diskless → majority (witness breaks the tie)
//	≥3 diskful → majority (odd or even, partition has a clear winner)
func TestQuorumPolicy(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		diskful  int
		diskless int
		want     string
	}{
		{"degenerate 0/0", 0, 0, "off"},
		{"single replica", 1, 0, "off"},
		{"single + witness still no quorum", 1, 1, "off"},
		{"2-replica no witness", 2, 0, "off"},
		{"2-replica + witness", 2, 1, "majority"},
		{"2-replica + multiple witnesses", 2, 3, "majority"},
		{"3-replica no witness", 3, 0, "majority"},
		{"3-replica + witness", 3, 1, "majority"},
		{"4-replica no witness", 4, 0, "majority"},
		{"5-replica no witness", 5, 0, "majority"},
	}

	for _, c := range cases {
		got := controllerpkg.QuorumPolicy(c.diskful, c.diskless)
		if got != c.want {
			t.Errorf("%s (diskful=%d, diskless=%d): got %q, want %q",
				c.name, c.diskful, c.diskless, got, c.want)
		}
	}
}
