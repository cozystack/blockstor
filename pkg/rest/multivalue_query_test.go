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

import (
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"
)

// TestMultiValueQuery pins both wire dialects we have to accept on
// the `/v1/view/*` filter query params:
//
//   - golinstor / linstor-csi: `?nodes=a,b,c`        (CSV)
//   - python-linstor:          `?nodes=a&nodes=b`    (repeat-key,
//     produced by urllib.urlencode(..., doseq=True))
//
// Regression target: an earlier implementation used
// `r.URL.Query().Get("nodes")` which only returns the FIRST value,
// silently dropping every node after the first one when called from
// the Python CLI. `linstor r l -n worker-1 -n worker-2` would
// degenerate to filtering by `worker-1` alone.
func TestMultiValueQuery(t *testing.T) {
	cases := []struct {
		name     string
		rawQuery string
		key      string
		want     []string
	}{
		{
			name:     "empty",
			rawQuery: "",
			key:      "nodes",
			want:     nil,
		},
		{
			name:     "single value",
			rawQuery: "nodes=worker-1",
			key:      "nodes",
			want:     []string{"worker-1"},
		},
		{
			name:     "CSV — golinstor dialect",
			rawQuery: "nodes=worker-1,worker-2,worker-3",
			key:      "nodes",
			want:     []string{"worker-1", "worker-2", "worker-3"},
		},
		{
			name:     "repeat-key — Python urlencode(doseq=True) dialect",
			rawQuery: "nodes=worker-1&nodes=worker-2&nodes=worker-3",
			key:      "nodes",
			want:     []string{"worker-1", "worker-2", "worker-3"},
		},
		{
			name:     "mixed: repeat-key with CSV inside",
			rawQuery: "nodes=worker-1,worker-2&nodes=worker-3",
			key:      "nodes",
			want:     []string{"worker-1", "worker-2", "worker-3"},
		},
		{
			name:     "whitespace trimmed, empty segments dropped",
			rawQuery: "nodes=%20worker-1%20,,worker-2%20",
			key:      "nodes",
			want:     []string{"worker-1", "worker-2"},
		},
		{
			name:     "different key — unrelated values ignored",
			rawQuery: "nodes=worker-1&resources=foo",
			key:      "nodes",
			want:     []string{"worker-1"},
		},
		{
			name:     "key absent",
			rawQuery: "resources=foo",
			key:      "nodes",
			want:     nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), "GET", "/?"+tc.rawQuery, nil)
			got := multiValueQuery(req, tc.key)

			// Order is implementation-defined when mixing
			// dialects; sort both sides for stable comparison.
			sortNil(got)
			sortNil(tc.want)

			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("multiValueQuery: got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSplitCSV pins the inner CSV splitter — boundary cases that
// caused subtle filter bugs historically: empty input must mean "no
// filter" (nil, not []string{""}), and surrounding whitespace must
// survive a copy/paste from a YAML manifest.
func TestSplitCSV(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"a", []string{"a"}},
		{"a,b,c", []string{"a", "b", "c"}},
		{" a , b , c ", []string{"a", "b", "c"}},
		{",,a,,b,,", []string{"a", "b"}},
		{"   ", nil},
	}

	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got := splitCSV(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("splitCSV(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// TestMatchAnyFold pins case-insensitive set-membership — Java
// LINSTOR is case-insensitive on these filters, and we follow suit
// so a node manifest with `worker-1` vs `Worker-1` doesn't surprise
// callers.
func TestMatchAnyFold(t *testing.T) {
	cases := []struct {
		name      string
		needles   []string
		candidate string
		want      bool
	}{
		{"empty needles accept anything", nil, "worker-1", true},
		{"exact match", []string{"worker-1"}, "worker-1", true},
		{"case-insensitive match", []string{"Worker-1"}, "WORKER-1", true},
		{"miss", []string{"worker-1"}, "worker-2", false},
		{"one of many", []string{"a", "b", "worker-3"}, "WORKER-3", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := matchAnyFold(tc.needles, tc.candidate)
			if got != tc.want {
				t.Errorf("matchAnyFold(%v, %q) = %v, want %v",
					tc.needles, tc.candidate, got, tc.want)
			}
		})
	}
}

func sortNil(s []string) {
	if s != nil {
		sort.Strings(s)
	}
}
