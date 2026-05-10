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

// TestPtrEqI32 pins the nil-aware *int32 equality helper that the
// DRBD-id allocator's "did Status change?" probe relies on. Three
// branches matter:
//  1. both nil → equal (controller hasn't allocated yet)
//  2. one nil → unequal (allocation just landed; Status mutated)
//  3. both non-nil → compare values
//
// A regression that flipped (2) to "equal" would silently make the
// allocator skip the Update call after first-allocation — the
// satellite would never get the persisted IDs and dispatch with
// zero values, corrupting DRBD's wire protocol.
func TestPtrEqI32(t *testing.T) {
	t.Parallel()

	one := int32(1)
	otherOne := int32(1)
	two := int32(2)

	cases := []struct {
		name string
		a, b *int32
		want bool
	}{
		{"both nil", nil, nil, true},
		{"a nil only", nil, &one, false},
		{"b nil only", &one, nil, false},
		{"equal values", &one, &otherOne, true},
		{"different values", &one, &two, false},
	}

	for _, c := range cases {
		got := controllerpkg.PtrEqI32(c.a, c.b)
		if got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
