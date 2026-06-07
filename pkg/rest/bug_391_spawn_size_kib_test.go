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
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 391 (spawn-size unit, 2026-06-06): `linstor rg spawn-resources
// <rg> <rd> 32M` created a 32 KiB volume instead of a 32 MiB one.
//
// Root cause: the spawn handler treated `volume_sizes` as BYTES and
// divided every entry by 1024 to derive size_kib. But the field is
// KiB:
//
//   - The python linstor client encodes the operator's size argument
//     with `parse_volume_size_to_kib` before POSTing — `32M` becomes
//     the integer 32768 in the `volume_sizes` array (see
//     linstorapi.py:resource_group_spawn → parse_volume_size_to_kib).
//   - The upstream LINSTOR REST API spec documents `volume_sizes` as
//     "sizes (in kib)" (rest_v1_openapi.yaml, ResourceGroupSpawn).
//
// So `32M` → client sends `32768` → handler divided by 1024 → a
// 32 KiB VD. Below DRBD's ~4 MiB per-device floor, the satellite
// reconciler then hot-looped on `drbdadm create-md`.
//
// The fix uses each `volume_sizes` entry directly as size_kib,
// matching the `vd c` path (volume_definitions.go reads `size_kib`
// verbatim). This test pins the exact operator-reported repro: the
// integer the python client puts on the wire for "32M" (32768) must
// land as a 32768 KiB VolumeDefinition — NOT 32 KiB.

// TestBug391_SpawnSizeIsKibNotBytes is the canonical pin: the wire
// integer 32768 (what `parse_volume_size_to_kib("32M")` produces)
// must materialise a VD of exactly 32768 KiB.
func TestBug391_SpawnSizeIsKibNotBytes(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-391",
		// PlaceCount=0 → spawnAutoplace returns nil immediately, so we
		// get a clean 201 without needing seeded nodes/pools; the size
		// is all this test cares about.
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 0},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// 32768 is exactly what the python client puts on the wire for the
	// operator's `32M` argument (parse_volume_size_to_kib → KiB).
	const wireSizeKibFor32M int64 = 32768

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "bug391-rd",
		VolumeSizes:            []int64{wireSizeKibFor32M},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups/rg-391/spawn", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	vds, err := st.VolumeDefinitions().List(ctx, "bug391-rd")
	if err != nil {
		t.Fatalf("list VDs: %v", err)
	}

	if len(vds) != 1 {
		t.Fatalf("VD count: got %d, want 1", len(vds))
	}

	// The bug: a /1024 divide here would land 32 KiB. The fix keeps it
	// at the requested 32768 KiB (= 32 MiB).
	if vds[0].SizeKib != wireSizeKibFor32M {
		t.Errorf("VD size: got %d KiB, want %d KiB (32M must not be divided to 32 KiB)",
			vds[0].SizeKib, wireSizeKibFor32M)
	}
}

// TestBug391_SpawnSizeMultiUnitTable pins a representative slice of
// the unit ladder the python client emits, so a future "helpful"
// re-introduction of a /1024 (or a ×1024) divide is caught regardless
// of which unit the operator typed. Each `wireKib` value is exactly
// what `parse_volume_size_to_kib` returns for the matching CLI token.
func TestBug391_SpawnSizeMultiUnitTable(t *testing.T) {
	cases := []struct {
		name    string // operator's CLI size token
		wireKib int64  // what the python client POSTs
	}{
		{"4M", 4096},
		{"32M", 32768},
		{"1G", 1048576},
		{"10G", 10485760},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := store.NewInMemory()
			ctx := t.Context()

			if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
				Name:         "rg-tab",
				SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 0},
			}); err != nil {
				t.Fatalf("seed RG: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			body, err := json.Marshal(apiv1.ResourceGroupSpawn{
				ResourceDefinitionName: "tab-rd",
				VolumeSizes:            []int64{tc.wireKib},
			})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			resp := httpPost(t, base+"/v1/resource-groups/rg-tab/spawn", body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusCreated {
				t.Fatalf("status: got %d, want 201", resp.StatusCode)
			}

			vds, err := st.VolumeDefinitions().List(ctx, "tab-rd")
			if err != nil {
				t.Fatalf("list VDs: %v", err)
			}

			if len(vds) != 1 || vds[0].SizeKib != tc.wireKib {
				t.Errorf("%s: VD size got %v, want one VD of %d KiB",
					tc.name, vds, tc.wireKib)
			}
		})
	}
}
