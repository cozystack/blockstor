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
	"io"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 381 (P3, bughunt round 11 — 2026-06-02): the spawn fast path
// `POST /v1/resource-groups/{rg}/spawn` silently accepted
// non-positive `volume_sizes` entries. The wire field is bytes
// (Bug-92 shape); each entry is divided by 1024 to land as size_kib
// on the VD, so a `-100` truncated to `size_kib=0` and a `0` stayed
// `0`. Both spawned the RD with a zero-sized VD — the satellite
// reconciler then looped on `drbdadm create-md` indefinitely
// (DRBD's per-device minimum is ~4 MiB once metadata is reserved).
//
// The direct `POST /v1/resource-definitions/{rd}/volume-definitions`
// path already rejected the same input via Bug 155
// writeVDSizeRejection. Bug 381 mirrors that gate on the spawn
// branch — operators get one consistent LINSTOR envelope on bad
// input and no orphan RD is left behind.

// TestBug381_SpawnNegativeVolumeSizeRejected: spawn with
// `volume_sizes: [-100]` returns 400 + Bug-155-shaped envelope and
// does NOT create the RD.
func TestBug381_SpawnNegativeVolumeSizeRejected(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{
		Name: "rg-1",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  1,
			StoragePool: "lvm-thin",
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "bug381-rd",
		VolumeSizes:            []int64{-100},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups/rg-1/spawn", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 400; body=%s", resp.StatusCode, respBody)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	var rcs []apiv1.APICallRc
	if err := json.Unmarshal(respBody, &rcs); err != nil {
		t.Fatalf("unmarshal envelope: %v; body=%s", err, respBody)
	}

	if len(rcs) != 1 {
		t.Fatalf("envelope length: got %d, want 1; body=%s", len(rcs), respBody)
	}

	if !strings.Contains(rcs[0].Message, "below minimum") {
		t.Errorf("message must mention `below minimum`; got %q", rcs[0].Message)
	}
	if !strings.Contains(rcs[0].Message, "bug381-rd") {
		t.Errorf("message must name the offending RD; got %q", rcs[0].Message)
	}

	// Crucially: no orphan RD is left behind. Pre-fix the spawn
	// handler created the RD before the bad-size error surfaced.
	if _, err := st.ResourceDefinitions().Get(t.Context(), "bug381-rd"); err == nil {
		t.Errorf("RD must NOT exist after rejection (orphan-RD bug)")
	}
}

// TestBug381_SpawnZeroVolumeSizeRejected: spawn with
// `volume_sizes: [0]` returns 400 + Bug-155-shaped envelope. Pre-fix
// the RD spawned with a zero-sized VD and the satellite reconciler
// hot-looped on `drbdadm create-md`.
func TestBug381_SpawnZeroVolumeSizeRejected(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{
		Name: "rg-1",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  1,
			StoragePool: "lvm-thin",
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "bug381-zero-rd",
		VolumeSizes:            []int64{0},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups/rg-1/spawn", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 400; body=%s", resp.StatusCode, respBody)
	}
}

// TestBug381_SpawnEmptyVolumeSizesStillSucceeds is the regression
// guard for the definitions-only spawn path (RG with PlaceCount=0,
// no VDs). Pre-Bug 381 this branch took the for-loop with zero
// iterations and the envelope said `resource definition spawned:
// <name>`. We must keep that contract — the gate only fires on
// non-positive entries.
func TestBug381_SpawnEmptyVolumeSizesStillSucceeds(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{
		Name: "rg-empty",
		// PlaceCount=0 → spawnAutoplace returns nil immediately,
		// so we get a clean 201 without needing seeded nodes.
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 0},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "bug381-defs-only-rd",
		// No VolumeSizes → definitions-only spawn, must still succeed.
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups/rg-empty/spawn", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 201; body=%s", resp.StatusCode, respBody)
	}
}
