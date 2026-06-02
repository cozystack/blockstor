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

// Bug 365 (P2, hunt-caught 2026-06-02) — `linstor vd d <rd> -1`,
// `linstor vd d <rd> 65536` and `linstor vd d <rd> 99999` all
// returned 200 + "volume definition already absent" instead of a
// 400 + FAIL_INVLD_VLM_NR refusal. Bug 363 pins the [0, 65535]
// addressable range at `vd c`, but the symmetric DELETE / GET /
// PUT wire boundaries silently masked out-of-range inputs as
// "already absent" — confusing tooling that audits idempotent
// deletes for "the row was there but now isn't" semantics.
//
// Fix: apply `validateVolumeNumber` (Bug 363's existing helper) at
// the DELETE, GET and PUT wire boundaries — same rejection
// envelope as the create path. The contract is now symmetric
// across the full VD CRUD surface.
func TestBug365VDDeleteRejectsOutOfRangeVolumeNumber(t *testing.T) {
	t.Parallel()

	const rdName = "pvc-bug-365"

	cases := []struct {
		name string
		vn   string
	}{
		{name: "negative", vn: "-1"},
		{name: "just-above-max", vn: "65536"},
		{name: "ten-times-max", vn: "655350"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			ctx := t.Context()

			if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
				Name: rdName,
			}); err != nil {
				t.Fatalf("seed RD: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			resp := httpDelete(t, base+"/v1/resource-definitions/"+rdName+
				"/volume-definitions/"+tc.vn)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusBadRequest {
				got, _ := readAllBody(resp)
				t.Fatalf("status: got %d, want 400 (Bug 365: invalid VlmNr %s). Body: %s",
					resp.StatusCode, tc.vn, got)
			}

			var rcs []apiv1.APICallRc
			if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
				t.Fatalf("decode envelope: %v", err)
			}

			if len(rcs) == 0 {
				t.Fatalf("envelope empty; want a Bug-365 refusal description")
			}

			// FAIL_INVLD_VLM_NR is the typed Bug 363 sub-code; the
			// vd-delete gate must surface the same sub-code so
			// tooling that classifies replies on the typed band sees
			// a consistent error class across the VD CRUD surface.
			if rcs[0].RetCode&apiCallRcFailInvldVlmNr == 0 {
				t.Errorf("ret_code: got %#x, want FAIL_INVLD_VLM_NR (%#x) bit set",
					rcs[0].RetCode, apiCallRcFailInvldVlmNr)
			}
		})
	}
}

// TestBug365VDGetRejectsOutOfRangeVolumeNumber pins the symmetric
// GET refusal — the audit-replay tooling that reads the surface
// expects the same 400 envelope for `vd l --vlm-nr 65536` as for
// `vd c --vlm-nr 65536`.
func TestBug365VDGetRejectsOutOfRangeVolumeNumber(t *testing.T) {
	t.Parallel()

	const rdName = "pvc-bug-365-get"

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: rdName,
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/"+rdName+"/volume-definitions/65536")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (Bug 365: GET on out-of-range VlmNr). Body: %s",
			resp.StatusCode, got)
	}
}
