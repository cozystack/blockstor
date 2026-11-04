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

package linstormigrate

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

const unreadableStack = `["DRBD",`

// countWarnings counts the warnings that carry substr.
func countWarnings(res *Result, substr string) int {
	n := 0

	for _, w := range res.Warnings {
		if strings.Contains(w, substr) {
			n++
		}
	}

	return n
}

// A layer_stack column the converter cannot read is reported once, with its
// value, whatever happens to the definition. When the replicas cannot settle
// the stack the definition is refused, and the refusal says the column was
// unreadable rather than that the definition has no stack of its own.
func TestAnUnreadableRDStackIsReportedOnce(t *testing.T) {
	t.Parallel()

	luks := func(first int32, node string) []LayerResourceIDRow {
		return layerChain(first, node, "PVC-S", "DRBD", "LUKS", "STORAGE")
	}
	plain := func(first int32, node string) []LayerResourceIDRow {
		return layerChain(first, node, "PVC-S", "DRBD", "STORAGE")
	}

	for name, tc := range map[string]struct {
		chains    [][]LayerResourceIDRow
		converted bool
		stack     []string
	}{
		"replicas agree": {
			chains: [][]LayerResourceIDRow{luks(1, "NODE-A"), luks(10, "NODE-B")}, converted: true,
			stack: []string{"DRBD", "LUKS", "STORAGE"},
		},
		"replicas disagree":      {chains: [][]LayerResourceIDRow{luks(1, "NODE-A"), plain(10, "NODE-B")}},
		"a replica has no chain": {chains: [][]LayerResourceIDRow{luks(1, "NODE-A")}},
		"no replicas":            {converted: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dump := stackDump(unreadableStack, tc.chains...)
			if len(tc.chains) > 0 {
				dump = withReplicas(dump)
			}

			res, err := Convert(dump)
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}

			if got := countWarnings(res, strconv.Quote(unreadableStack)); got != 1 {
				t.Errorf("the unreadable layer_stack %q is in %d warnings, want 1; warnings %v", unreadableStack, got, res.Warnings)
			}

			if tc.stack != nil && !hasWarning(res, "took the stack its replicas were built with") {
				t.Errorf("the warning does not say which stack was taken; warnings %v", res.Warnings)
			}

			if hasWarning(res, "no layer stack of its own") {
				t.Errorf("an unreadable layer_stack was reported as no stack; warnings %v", res.Warnings)
			}

			idx := slices.IndexFunc(res.ResourceDefinitions, func(rd crdv1alpha1.ResourceDefinition) bool { return rd.Name == "pvc-s" })
			if converted := idx >= 0; converted != tc.converted {
				t.Fatalf("converted = %v, want %v; warnings %v", converted, tc.converted, res.Warnings)
			}

			if tc.converted && !slices.Equal(res.ResourceDefinitions[idx].Spec.LayerStack, tc.stack) {
				t.Errorf("stack %v, want %v", res.ResourceDefinitions[idx].Spec.LayerStack, tc.stack)
			}
		})
	}
}
