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

package v1_test

import (
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// TestResolveLayerStack: RD wins over RG; both empty → default.
func TestResolveLayerStack(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		rd   []string
		rg   []string
		want []string
	}{
		{
			name: "RD set, RG ignored",
			rd:   []string{"LUKS", "STORAGE"},
			rg:   []string{"DRBD", "STORAGE"},
			want: []string{"LUKS", "STORAGE"},
		},
		{
			name: "RD empty, RG wins",
			rg:   []string{"DRBD", "LUKS", "STORAGE"},
			want: []string{"DRBD", "LUKS", "STORAGE"},
		},
		{
			name: "both empty → default",
			want: []string{"DRBD", "STORAGE"},
		},
		{
			name: "RD empty slice → default",
			rd:   []string{},
			rg:   []string{},
			want: []string{"DRBD", "STORAGE"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			got := apiv1.ResolveLayerStack(c.rd, c.rg)
			if !slices.Equal(got, c.want) {
				t.Errorf("got %v, want %v", got, c.want)
			}
		})
	}
}

// TestLayerInStack: case-insensitive presence check.
func TestLayerInStack(t *testing.T) {
	t.Parallel()

	stack := []string{"DRBD", "STORAGE"}

	if !apiv1.LayerInStack(stack, "DRBD") {
		t.Errorf("DRBD should be in %v", stack)
	}

	if !apiv1.LayerInStack(stack, "drbd") {
		t.Errorf("case-insensitive match for drbd should hit DRBD")
	}

	if apiv1.LayerInStack(stack, "LUKS") {
		t.Errorf("LUKS not in %v", stack)
	}

	if apiv1.LayerInStack(nil, "DRBD") {
		t.Errorf("empty stack should not match anything")
	}
}
