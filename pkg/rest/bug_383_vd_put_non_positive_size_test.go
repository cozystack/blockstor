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

// Bug 383 (P3, bughunt round 12 — 2026-06-02): the VD PUT update path
// `PUT /v1/resource-definitions/{rd}/volume-definitions/{vn}` silently
// accepted non-positive `size_kib` when `force=true` was set. The
// force escape hatch was scoped only at the scenario 4.W13 "no auto-
// shrink" refusal — callers that already ran `resize2fs -s` on the
// consumer know the new size is below the live one and need to land
// the smaller spec. force was never intended to let a caller persist
// `size_kib=0` or a negative value into the spec.
//
// The satellite reconciler then looped on `drbdadm create-md`
// indefinitely (DRBD's per-device minimum is ~4 MiB once metadata is
// reserved), identical to the Bug 381 spawn-fast-path footgun but
// reached through the PUT update path. The POST VD create path
// already rejected the same input via Bug 155 validateVDSize. Bug 383
// mirrors that gate on the PUT branch before the shrink+force check —
// the operator-facing message is the right one ("must be > 0",
// not "filesystem shrink-then-resize required").

// seedRDWithVD seeds a single-volume RD into the in-memory store so
// the PUT tests can run against a real VD without a spawn round-trip.
func seedRDWithVD(t *testing.T, st store.Store, rdName string, sizeKib int64) {
	t.Helper()

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name:              rdName,
		ResourceGroupName: "DfltRscGrp",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(t.Context(), rdName, &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      sizeKib,
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}
}

// assertVDSize fetches the VD from the store and fails the test when
// its persisted size_kib doesn't match the expected value. Used by the
// Bug 383 tests to pin that a rejected PUT did NOT mutate the row.
func assertVDSize(t *testing.T, st store.Store, rdName string, want int64) {
	t.Helper()

	vd, err := st.VolumeDefinitions().Get(t.Context(), rdName, 0)
	if err != nil {
		t.Fatalf("get VD after rejection: %v", err)
	}

	if vd.SizeKib != want {
		t.Errorf("size_kib leaked through rejection: got %d, want %d", vd.SizeKib, want)
	}
}

// TestBug383_VDPutNegativeSizeWithForceRejected: PUT VD with
// `size_kib=-100, force=true` returns 400 + Bug-155-shaped envelope
// and does NOT mutate the stored row. Pre-fix this 200'd and persisted
// `size_kib=-100` into the spec.
func TestBug383_VDPutNegativeSizeWithForceRejected(t *testing.T) {
	st := store.NewInMemory()
	seedRDWithVD(t, st, "bug383-neg-rd", 8192)

	base, stop := startServerWithStore(t, st)
	defer stop()

	negSize := int64(-100)
	body, err := json.Marshal(volumeDefinitionModifyBody{
		SizeKib: &negSize,
		Force:   true,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/bug383-neg-rd/volume-definitions/0", body)
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

	// Bug 383 message must point at the absolute floor, not the
	// shrink-then-resize cause text — operators reading the LINSTOR
	// error need to see "this size is invalid" not "you forgot to
	// resize2fs first".
	if !strings.Contains(rcs[0].Message, "must be > 0") {
		t.Errorf("message must mention the > 0 floor; got %q", rcs[0].Message)
	}
	if strings.Contains(rcs[0].Message, "filesystem shrink-then-resize") {
		t.Errorf("message must NOT confuse Bug 383 with the shrink-force path; got %q", rcs[0].Message)
	}

	// Crucially: the stored row stays at the pre-PUT size.
	assertVDSize(t, st, "bug383-neg-rd", 8192)
}

// TestBug383_VDPutZeroSizeWithForceRejected: PUT VD with
// `size_kib=0, force=true` returns 400 + Bug-155-shaped envelope and
// does NOT mutate the stored row. Pre-fix this 200'd and persisted
// `size_kib=0` into the spec, then the satellite reconciler hot-
// looped on `drbdadm create-md`.
func TestBug383_VDPutZeroSizeWithForceRejected(t *testing.T) {
	st := store.NewInMemory()
	seedRDWithVD(t, st, "bug383-zero-rd", 8192)

	base, stop := startServerWithStore(t, st)
	defer stop()

	zeroSize := int64(0)
	body, err := json.Marshal(volumeDefinitionModifyBody{
		SizeKib: &zeroSize,
		Force:   true,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/bug383-zero-rd/volume-definitions/0", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 400; body=%s", resp.StatusCode, respBody)
	}

	assertVDSize(t, st, "bug383-zero-rd", 8192)
}

// TestBug383_VDPutNegativeSizeWithoutForceRejected: PUT VD with
// `size_kib=-100` and no force still rejects. This was already
// blocked pre-Bug 383 via the shrink-without-force path, but the
// message routing changes (the Bug 383 floor check runs first), so
// pin the regression that the gate stays closed and the row stays
// untouched.
func TestBug383_VDPutNegativeSizeWithoutForceRejected(t *testing.T) {
	st := store.NewInMemory()
	seedRDWithVD(t, st, "bug383-noforce-rd", 8192)

	base, stop := startServerWithStore(t, st)
	defer stop()

	negSize := int64(-100)
	body, err := json.Marshal(volumeDefinitionModifyBody{
		SizeKib: &negSize,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/bug383-noforce-rd/volume-definitions/0", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 400; body=%s", resp.StatusCode, respBody)
	}

	assertVDSize(t, st, "bug383-noforce-rd", 8192)
}

// TestBug383_VDPutValidShrinkWithForceStillWorks: scenario 4.W13
// regression guard — the legitimate force-shrink path (positive new
// size below the previous size) must still land. This is the path
// linstor-csi uses after a `resize2fs -s` on the consumer, so the
// guard at Bug 383 must NOT regress it.
func TestBug383_VDPutValidShrinkWithForceStillWorks(t *testing.T) {
	st := store.NewInMemory()
	seedRDWithVD(t, st, "bug383-shrink-rd", 1024*1024)

	base, stop := startServerWithStore(t, st)
	defer stop()

	newSize := int64(512 * 1024)
	body, err := json.Marshal(volumeDefinitionModifyBody{
		SizeKib: &newSize,
		Force:   true,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/bug383-shrink-rd/volume-definitions/0", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 200; body=%s", resp.StatusCode, respBody)
	}

	assertVDSize(t, st, "bug383-shrink-rd", newSize)
}

// TestBug383_VDPutValidGrowStillWorks: positive growth path must
// still land. Confirms the Bug 383 gate is not over-broad.
func TestBug383_VDPutValidGrowStillWorks(t *testing.T) {
	st := store.NewInMemory()
	seedRDWithVD(t, st, "bug383-grow-rd", 8192)

	base, stop := startServerWithStore(t, st)
	defer stop()

	newSize := int64(16384)
	body, err := json.Marshal(volumeDefinitionModifyBody{
		SizeKib: &newSize,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/bug383-grow-rd/volume-definitions/0", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 200; body=%s", resp.StatusCode, respBody)
	}

	assertVDSize(t, st, "bug383-grow-rd", newSize)
}
