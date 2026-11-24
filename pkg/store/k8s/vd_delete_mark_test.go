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

package k8s_test

import (
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// A modify built from a wire value that never saw the delete keeps it: the
// DELETE flag and the deleteID survive the rebuild of the entry, through
// both VD-scoped write paths.
func TestVolumeDefinitionModifyKeepsTheDeleteMark(t *testing.T) {
	t.Parallel()

	for name, modify := range map[string]func(st store.Store, rd string) error{
		"update": func(st store.Store, rd string) error {
			return st.VolumeDefinitions().Update(t.Context(), rd, &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: bug433TwoGiB})
		},
		"patch": func(st store.Store, rd string) error {
			return st.VolumeDefinitions().PatchVolumeDefinitionSpec(t.Context(), rd, 0, func(vd *apiv1.VolumeDefinition) error {
				vd.Flags, vd.DeleteID = nil, ""

				return nil
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			rd := "pvc-vdd-mark-" + name
			st, _ := bug433SeedStore(t, rd)

			flagged, marked, err := store.MarkVolumeDefinitionDeleted(t.Context(), st, rd, 0)
			if err != nil || !marked || flagged.DeleteID == "" {
				t.Fatalf("mark: %+v %v %v", flagged, marked, err)
			}

			if err := modify(st, rd); err != nil {
				t.Fatalf("modify: %v", err)
			}

			got, err := st.VolumeDefinitions().Get(t.Context(), rd, 0)
			if err != nil {
				t.Fatal(err)
			}

			if !got.Deleting() || got.DeleteID != flagged.DeleteID {
				t.Errorf("after %s: flags %v deleteID %q, want DELETE and %q kept", name, got.Flags, got.DeleteID, flagged.DeleteID)
			}
		})
	}
}
