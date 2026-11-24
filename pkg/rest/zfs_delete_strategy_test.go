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

package rest

import (
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/validate"
)

// seedDeleteStrategy builds a definition with one volume, a replica on node-a and a
// snapshot, with the delete strategy set on the given scope.
func seedDeleteStrategy(t *testing.T, scope, value string) store.Store {
	t.Helper()

	backend := store.NewInMemory()
	ctx := t.Context()

	rg := &apiv1.ResourceGroup{Name: "rg-gated"}
	rd := &apiv1.ResourceDefinition{Name: "rd-gated", ResourceGroupName: "rg-gated"}

	switch scope {
	case "rd":
		rd.Props = map[string]string{validate.ZFSDeleteStrategyProp: value}
	case "rg":
		rg.Props = map[string]string{validate.ZFSDeleteStrategyProp: value}
	case "controller":
		if err := backend.ControllerProps().Set(ctx, map[string]string{validate.ZFSDeleteStrategyProp: value}); err != nil {
			t.Fatalf("seed controller props: %v", err)
		}
	}

	if err := backend.ResourceGroups().Create(ctx, rg); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := backend.ResourceDefinitions().Create(ctx, rd); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := backend.VolumeDefinitions().Create(ctx, "rd-gated", &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	if err := backend.Resources().Create(ctx, &apiv1.Resource{Name: "rd-gated", NodeName: "node-a"}); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	if err := backend.Snapshots().Create(ctx, &apiv1.Snapshot{Name: "s1", ResourceName: "rd-gated"}); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}

	return backend
}

// Under LINSTOR's DELETE strategy, set on any scope, a replica or a volume
// definition of a definition with snapshots is refused; the default and the
// other values let it through.
func TestZFSDeleteStrategyGatesReplicaAndVolumeDeletes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, scope, value string
		refused            bool
	}{
		{"default", "", "", false},
		{"rd delete", "rd", "delete", true},
		{"rg DELETE", "rg", "DELETE", true},
		{"controller DELETE", "controller", "DELETE", true},
		{"rd rename", "rd", "RENAME", false},
		{"rd dynamic", "rd", "Dynamic", false},
		{"unknown value reads as dynamic", "rd", "nonsense", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			for _, path := range []string{
				"/v1/resource-definitions/rd-gated/resources/node-a",
				"/v1/resource-definitions/rd-gated/volume-definitions/0",
			} {
				backend := seedDeleteStrategy(t, tc.scope, tc.value)

				base, stop := startServerWithStore(t, backend)

				resp := httpDelete(t, base+path)
				_ = resp.Body.Close()

				stop()

				if tc.refused && resp.StatusCode != http.StatusConflict {
					t.Errorf("%s: status %d, want 409", path, resp.StatusCode)
				}

				if !tc.refused && resp.StatusCode == http.StatusConflict {
					t.Errorf("%s: refused with 409", path)
				}
			}
		})
	}
}

// Without snapshots the DELETE strategy refuses nothing.
func TestZFSDeleteStrategyWithoutSnapshotsAllowsTheDelete(t *testing.T) {
	t.Parallel()

	backend := seedDeleteStrategy(t, "rd", "DELETE")
	if err := backend.Snapshots().Delete(t.Context(), "rd-gated", "s1"); err != nil {
		t.Fatalf("drop snapshot: %v", err)
	}

	base, stop := startServerWithStore(t, backend)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/rd-gated/resources/node-a")
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		t.Errorf("refused with no snapshot left")
	}
}
