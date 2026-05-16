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
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 150 — `GET /v1/key-value-store/<bogus>` used to return 200 with
// `{"name":"<path-param>"}` (wrapped in a one-element array) instead
// of a 404 + LINSTOR envelope. Operators piping the call through
// `python-linstor` or any other LINSTOR client saw a phantom KVS
// instance whose name was whatever string they passed.
//
// Fix mirrors the rest of the read-side: unknown instance => 404 +
// `[]ApiCallRc` envelope with MASK_ERROR; the existing KVS bag
// lookup gates the response body.

// Bug 150 dropped: investigated and reclassified as not-a-bug.
// Upstream LINSTOR's documented contract for `kvs show <unknown>`
// is `200 + []KV{{Name: <input>}}` (empty props), NOT 404. The
// pre-existing TestKVGetReturnsEmptyPropsArrayForUnknown pinned
// this and explicitly cited upstream parity. Changing to 404
// would break python-linstor 1.27.1's empty-bag-on-unknown path.

// Bug 154 — `DELETE /v1/resource-groups/{rg}/properties/{key}` was
// never wired, so `linstor rg dp <rg> <key>` hit the 404 catch-all
// envelope (Bug 103) instead of the real handler. Mirrors Bug 142's
// per-key DELETE shape exactly (the node-scope analog).

// TestBug154RGPropertyDeleteReturns200Envelope pins the happy path:
// seed an RG with a property, DELETE through the per-key endpoint,
// confirm the property is gone and the envelope is 200 + info-mask.
func TestBug154RGPropertyDeleteReturns200Envelope(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-1",
		Props: map[string]string{
			"DrbdOptions/auto-quorum":     "io-error",
			"DrbdOptions/Net/sndbuf-size": "1048576",
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-groups/rg-1/properties/DrbdOptions/auto-quorum")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		raw, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 200 (Bug 154 per-key DELETE). Body: %s",
			resp.StatusCode, raw)
	}

	raw, _ := readAllBody(resp)

	var rcs []apiv1.APICallRc
	if jsonErr := json.Unmarshal(raw, &rcs); jsonErr != nil {
		t.Fatalf("body is not a []ApiCallRc envelope: %v\n%s", jsonErr, raw)
	}

	if len(rcs) == 0 {
		t.Fatalf("envelope: got empty, want one entry")
	}

	if rcs[0].RetCode&apiCallRcError != 0 {
		t.Errorf("envelope ret_code carries MASK_ERROR (delete should succeed): %+v", rcs[0])
	}

	got, err := st.ResourceGroups().Get(ctx, "rg-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if _, present := got.Props["DrbdOptions/auto-quorum"]; present {
		t.Errorf("Props[DrbdOptions/auto-quorum]: still present after DELETE; got %+v", got.Props)
	}

	// Sibling key must survive — the per-key DELETE must NOT nuke
	// the whole property bag.
	if got.Props["DrbdOptions/Net/sndbuf-size"] != "1048576" {
		t.Errorf("Props[DrbdOptions/Net/sndbuf-size]: collateral-deleted; got %+v", got.Props)
	}
}

// TestBug154RGPropertyDeleteIdempotent pins the idempotency-on-absent-key
// clause: LINSTOR treats "delete a property that wasn't set" as a no-op
// (warn-mask), not an error, so reconciler retries don't hot-spin.
func TestBug154RGPropertyDeleteIdempotent(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{
		Name: "rg-1",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-groups/rg-1/properties/DrbdOptions/ghost")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		raw, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 200 (idempotent delete-of-missing). Body: %s",
			resp.StatusCode, raw)
	}

	raw, _ := readAllBody(resp)

	var rcs []apiv1.APICallRc
	if jsonErr := json.Unmarshal(raw, &rcs); jsonErr != nil {
		t.Fatalf("body is not a []ApiCallRc envelope: %v\n%s", jsonErr, raw)
	}

	if len(rcs) == 0 {
		t.Fatalf("envelope: got empty, want one entry")
	}

	if rcs[0].RetCode&apiCallRcError != 0 {
		t.Errorf("envelope ret_code carries MASK_ERROR (no-op delete should succeed): %+v", rcs[0])
	}

	low := strings.ToLower(rcs[0].Message)
	if !strings.Contains(low, "absent") && !strings.Contains(low, "not") {
		t.Errorf("envelope message should mark the key as absent; got %q", rcs[0].Message)
	}
}

// Bug 155 — `POST /v1/resource-definitions/{rd}/volume-definitions`
// previously accepted any size_kib value, including zero, single-byte
// (in KiB: 0 is impossible), or absurdly-large petabyte counts. The
// satellite would then loop on drbdadm create-md failure because the
// DRBD metadata reservation (32 KB per peer) exceeds the requested
// device size. Refuse at the REST boundary with 400 + LINSTOR
// envelope citing the accepted bounds.

// TestBug155VDCreateRefusesZeroSize pins the canonical reproducer:
// `linstor vd c X 0` => REST POST size_kib=0 must 400 + envelope.
func TestBug155VDCreateRefusesZeroSize(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	const rdName = "pvc-bug155-zero"

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.VolumeDefinitionCreate{
		VolumeDefinition: apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 0},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/volume-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		raw, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (Bug 155 zero size). Body: %s",
			resp.StatusCode, raw)
	}

	raw, _ := readAllBody(resp)

	var rcs []apiv1.APICallRc
	if jsonErr := json.Unmarshal(raw, &rcs); jsonErr != nil {
		t.Fatalf("body is not a []ApiCallRc envelope: %v\n%s", jsonErr, raw)
	}

	if len(rcs) == 0 || rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("envelope must carry MASK_ERROR; got %+v", rcs)
	}

	// VD must NOT have been persisted.
	vds, err := st.VolumeDefinitions().List(ctx, rdName)
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(vds) != 0 {
		t.Errorf("VolumeDefinitions: got %d, want 0 (refused create must not persist)", len(vds))
	}
}

// TestBug155VDCreateRefusesTinySize: 1 KiB is below the DRBD metadata
// minimum and must be refused.
func TestBug155VDCreateRefusesTinySize(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	const rdName = "pvc-bug155-tiny"

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.VolumeDefinitionCreate{
		VolumeDefinition: apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/volume-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		raw, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (Bug 155 tiny size). Body: %s",
			resp.StatusCode, raw)
	}
}

// TestBug155VDCreateRefusesAbsurdSize: 100 PiB (100*1024*1024*1024
// KiB) exceeds DRBD's per-device hard ceiling and must be refused.
func TestBug155VDCreateRefusesAbsurdSize(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	const rdName = "pvc-bug155-huge"

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// 100 PiB expressed in KiB.
	const absurdKiB int64 = 100 * 1024 * 1024 * 1024 * 1024

	body, _ := json.Marshal(apiv1.VolumeDefinitionCreate{
		VolumeDefinition: apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: absurdKiB},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/volume-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		raw, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (Bug 155 absurd size). Body: %s",
			resp.StatusCode, raw)
	}

	raw, _ := readAllBody(resp)

	var rcs []apiv1.APICallRc
	if jsonErr := json.Unmarshal(raw, &rcs); jsonErr != nil {
		t.Fatalf("body is not a []ApiCallRc envelope: %v\n%s", jsonErr, raw)
	}

	if len(rcs) == 0 || rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("envelope must carry MASK_ERROR; got %+v", rcs)
	}
}

// TestBug155VDCreateAcceptsReasonable: 32 MiB (32*1024 KiB) is the
// canonical CSI-sanity-sized create and must keep landing 200.
func TestBug155VDCreateAcceptsReasonable(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	const rdName = "pvc-bug155-ok"

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.VolumeDefinitionCreate{
		VolumeDefinition: apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 32 * 1024},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/volume-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		raw, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 200 (Bug 155 reasonable size). Body: %s",
			resp.StatusCode, raw)
	}
}
