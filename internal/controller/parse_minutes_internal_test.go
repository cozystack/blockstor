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

package controller

import "testing"

// TestParseMinutesVariants pins the boundary between the two minute
// parsers (BUG-022 secondary fix): parsePositiveMinutes keeps
// rejecting 0 (an Interval of 0 is not a meaningful cadence), while
// parseNonNegativeMinutes accepts it (GracePeriod 0 = "no grace
// window" is a legal upstream value). Both reject negatives, empty
// strings and garbage so the resolver falls back to defaults.
func TestParseMinutesVariants(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in          string
		wantPos     int
		wantPosOK   bool
		wantNonNeg  int
		wantNNegOK  bool
		description string
	}{
		{"", 0, false, 0, false, "empty means unset"},
		{"0", 0, false, 0, true, "zero: rejected as positive, legal as non-negative"},
		{"1", 1, true, 1, true, "smallest positive"},
		{"60", 60, true, 60, true, "typical grace"},
		{"-1", 0, false, 0, false, "negatives are garbage in both"},
		{"abc", 0, false, 0, false, "non-numeric is garbage in both"},
		{"1.5", 0, false, 0, false, "fractions are not upstream shape"},
		{" 1", 0, false, 0, false, "whitespace is not trimmed"},
	}

	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			t.Parallel()

			gotPos, okPos := parsePositiveMinutes(tc.in)
			if gotPos != tc.wantPos || okPos != tc.wantPosOK {
				t.Errorf("parsePositiveMinutes(%q) = (%d, %v), want (%d, %v)",
					tc.in, gotPos, okPos, tc.wantPos, tc.wantPosOK)
			}

			gotNNeg, okNNeg := parseNonNegativeMinutes(tc.in)
			if gotNNeg != tc.wantNonNeg || okNNeg != tc.wantNNegOK {
				t.Errorf("parseNonNegativeMinutes(%q) = (%d, %v), want (%d, %v)",
					tc.in, gotNNeg, okNNeg, tc.wantNonNeg, tc.wantNNegOK)
			}
		})
	}
}
