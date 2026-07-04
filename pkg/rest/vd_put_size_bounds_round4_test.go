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

// VD-resize size-bounds gap (adversarial round 4, 2026-07-03): the VD
// CREATE path gates size_kib into [4 MiB, 16 TiB] via validateVDSize
// (Bug 155), so the satellite never hot-loops on `drbdadm create-md`
// for an unmaterializable size. The RESIZE path
// (`PUT /v1/resource-definitions/{rd}/volume-definitions/{vn}`, i.e.
// `linstor vd set-size`) enforced only the Bug 383 non-positive floor
// and the scenario 4.W13 shrink-vs-force gate — it never called
// validateVDSize. So a `vd set-size` below the 4 MiB metadata floor
// (with force to clear the shrink gate) or above the 16 TiB ceiling
// was accepted (200) and stored verbatim, reproducing the exact Bug
// 155 satellite hot-loop through the resize verb instead of create.
//
// The fix mirrors the create gate on the PUT branch (rejectVDPatch
// OutOfBounds), evaluated — like the Bug 383 non-positive floor — BEFORE
// the shrink-vs-force check: `force` waives the shrink-direction opt-in,
// never the physical floor/ceiling, and running the bounds check first
// hands the operator the accurate "invalid size" envelope instead of an
// "add --force" hint on a size that would be rejected even with force.
//
// These are the L1 fail-on-bug regressions: each FAILS on the pre-fix
// tree (PUT 200s and mutates the stored row) and PASSES with the gate.

// TestVDPutBelowFloorWithForceRejected: a force-shrink to a positive
// size below the 4 MiB DRBD metadata floor must be refused with the
// same 400 + FAIL_INVLD_VLM_SIZE envelope the create path returns, and
// must NOT mutate the stored size.
func TestVDPutBelowFloorWithForceRejected(t *testing.T) {
	st := store.NewInMemory()
	const origSize = int64(1024 * 1024) // 1 GiB, comfortably in-bounds
	seedRDWithVD(t, st, "r4-floor-rd", origSize)

	base, stop := startServerWithStore(t, st)
	defer stop()

	belowFloor := minVolumeDefinitionSizeKib - 1024 // 1 MiB below the 4 MiB floor, still > 0
	body, err := json.Marshal(volumeDefinitionModifyBody{
		SizeKib: &belowFloor,
		Force:   true, // clears the shrink-without-force gate
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/r4-floor-rd/volume-definitions/0", body)
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400 (sub-floor size must be refused like create); body=%s",
			resp.StatusCode, respBody)
	}

	assertVDSizeRejectionEnvelope(t, respBody, "below minimum")

	// The stored row must stay at the pre-PUT size — a rejected resize
	// leaves the spec untouched.
	assertVDSize(t, st, "r4-floor-rd", origSize)
}

// TestVDPutAboveMaxRejected: a grow above the 16 TiB DRBD per-device
// ceiling (a pure grow, so only a max-bound gate can stop it) must be
// refused and must not mutate the stored size.
func TestVDPutAboveMaxRejected(t *testing.T) {
	st := store.NewInMemory()
	const origSize = int64(8192)
	seedRDWithVD(t, st, "r4-max-rd", origSize)

	base, stop := startServerWithStore(t, st)
	defer stop()

	aboveMax := maxVolumeDefinitionSizeKib + (1024 * 1024) // 1 GiB past the ceiling
	body, err := json.Marshal(volumeDefinitionModifyBody{
		SizeKib: &aboveMax,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/r4-max-rd/volume-definitions/0", body)
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400 (over-ceiling size must be refused like create); body=%s",
			resp.StatusCode, respBody)
	}

	assertVDSizeRejectionEnvelope(t, respBody, "above maximum")

	assertVDSize(t, st, "r4-max-rd", origSize)
}

// TestVDPutAtBoundsAccepted: the boundary is inclusive on both ends
// (validateVDSize rejects `< min` and `> max`), so a resize to exactly
// the floor (force-shrink) and exactly the ceiling (grow) must still
// land. Guards the new gate against being one-off over-broad.
func TestVDPutAtBoundsAccepted(t *testing.T) {
	t.Run("exact-floor-force-shrink", func(t *testing.T) {
		st := store.NewInMemory()
		seedRDWithVD(t, st, "r4-atfloor-rd", 1024*1024)

		base, stop := startServerWithStore(t, st)
		defer stop()

		atFloor := minVolumeDefinitionSizeKib
		body, _ := json.Marshal(volumeDefinitionModifyBody{SizeKib: &atFloor, Force: true})

		resp := httpPut(t, base+"/v1/resource-definitions/r4-atfloor-rd/volume-definitions/0", body)
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			rb, _ := io.ReadAll(resp.Body)
			t.Fatalf("status: got %d, want 200 (exact floor is in-bounds); body=%s", resp.StatusCode, rb)
		}

		assertVDSize(t, st, "r4-atfloor-rd", atFloor)
	})

	t.Run("exact-ceiling-grow", func(t *testing.T) {
		st := store.NewInMemory()
		seedRDWithVD(t, st, "r4-atmax-rd", 8192)

		base, stop := startServerWithStore(t, st)
		defer stop()

		atMax := maxVolumeDefinitionSizeKib
		body, _ := json.Marshal(volumeDefinitionModifyBody{SizeKib: &atMax})

		resp := httpPut(t, base+"/v1/resource-definitions/r4-atmax-rd/volume-definitions/0", body)
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			rb, _ := io.ReadAll(resp.Body)
			t.Fatalf("status: got %d, want 200 (exact ceiling is in-bounds); body=%s", resp.StatusCode, rb)
		}

		assertVDSize(t, st, "r4-atmax-rd", atMax)
	})
}

// TestVDPutInBoundsForceShrinkStillWorks: a legitimate in-bounds
// force-shrink (1 GiB → 8 MiB, well above the 4 MiB floor) must still
// land 200 and persist. The bounds gate must not regress the
// scenario-4.W13 force-shrink path linstor-csi drives after a
// `resize2fs -s` on the consumer — `force` still clears the shrink-
// direction gate, and the in-bounds size passes the new floor/ceiling.
func TestVDPutInBoundsForceShrinkStillWorks(t *testing.T) {
	st := store.NewInMemory()
	seedRDWithVD(t, st, "r4-inbounds-shrink-rd", 1024*1024) // 1 GiB

	base, stop := startServerWithStore(t, st)
	defer stop()

	newSize := int64(8 * 1024) // 8 MiB, comfortably in-bounds
	body, err := json.Marshal(volumeDefinitionModifyBody{SizeKib: &newSize, Force: true})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/r4-inbounds-shrink-rd/volume-definitions/0", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		rb, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 200 (in-bounds force-shrink must still land); body=%s", resp.StatusCode, rb)
	}

	assertVDSize(t, st, "r4-inbounds-shrink-rd", newSize)
}

// assertVDSizeRejectionEnvelope pins that a rejected resize returns the
// single-entry LINSTOR envelope with the FAIL_INVLD_VLM_SIZE sub-code
// (create-path parity) and a message naming the specific bound.
func assertVDSizeRejectionEnvelope(t *testing.T, respBody []byte, wantBound string) {
	t.Helper()

	var rcs []apiv1.APICallRc
	if err := json.Unmarshal(respBody, &rcs); err != nil {
		t.Fatalf("unmarshal envelope: %v; body=%s", err, respBody)
	}

	if len(rcs) != 1 {
		t.Fatalf("envelope length: got %d, want 1; body=%s", len(rcs), respBody)
	}

	if rcs[0].RetCode&apiCallRcFailInvldVlmSize == 0 {
		t.Errorf("ret_code missing FAIL_INVLD_VLM_SIZE sub-code (create-path parity); got %d", rcs[0].RetCode)
	}

	if !strings.Contains(rcs[0].Message, wantBound) {
		t.Errorf("message must name the %q bound; got %q", wantBound, rcs[0].Message)
	}
}
