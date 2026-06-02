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
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 363: `linstor vd c -n <out-of-bound>` (e.g. -1 or 100000) was
// accepted by the REST surface, persisted in the store, and then hung
// the satellite reconciler in
//
//	{"msg":"waiting for controller-side DRBD-ID allocation",
//	 "resource":"<rd>.<node>","nodeID":0,"port":20000,"minor":null}
//
// indefinitely because no positive DRBD minor can be derived from a
// negative VlmNr (and DRBD-9 caps the per-resource volume namespace at
// 16 bits — values above 65535 trigger the same DRBD-ID stall).
//
// Reproduction on dev stand (HEAD 6f69c5678, 2026-06-01):
// `vd c -n -1 <rd> 64M` returned SUCCESS; the follow-up `r c ... <rd>
// --storage-pool stand` also returned SUCCESS; `r l -r <rd>` then
// printed `Unknown` state forever while the satellite logged
// `waiting for controller-side DRBD-ID allocation` indefinitely
// (no positive minor can be derived from a negative VlmNr).
//
// Fix: validate explicit `volume_number` is in [0, 65535] at the REST
// wire boundary, before any partial state lands. The auto-assign path
// (no `-n` flag) is always valid by construction and bypasses the
// gate.

// TestBug363VDCreateRefusedOnNegativeVolumeNumber covers the headline
// regression: `vd c -n -1 <rd>` must be refused with a 400 envelope,
// no VD persisted.
func TestBug363VDCreateRefusedOnNegativeVolumeNumber(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd-bug363"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.VolumeDefinitionCreate{
		VolumeDefinition: apiv1.VolumeDefinition{
			VolumeNumber: -1,
			SizeKib:      65536,
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/rd-bug363/volume-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 for VlmNr=-1. Body: %s", resp.StatusCode, got)
	}

	got, _ := readAllBody(resp)
	gotStr := string(got)

	if !strings.Contains(gotStr, "below minimum") {
		t.Errorf("envelope should name the rule (below minimum); got %s", gotStr)
	}

	// No VD persisted = no zombie RD entry.
	vds, _ := st.VolumeDefinitions().List(ctx, "rd-bug363")
	if len(vds) != 0 {
		t.Errorf("VD must NOT be persisted on refusal; got %d entries", len(vds))
	}
}

// TestBug363VDCreateRefusedOnOversizedVolumeNumber pins the upper
// bound: `vd c -n 65536` and `vd c -n 100000` must both be refused.
// Upstream DRBD-9 indexes volumes with a 16-bit field; anything above
// 65535 trips the same satellite stall as a negative VlmNr.
func TestBug363VDCreateRefusedOnOversizedVolumeNumber(t *testing.T) {
	t.Parallel()

	cases := []int32{65536, 100000, 2147483647}

	for _, vn := range cases {
		t.Run(strings.ReplaceAll(http.StatusText(int(vn%256)), " ", "-"), func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			ctx := t.Context()

			if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd-bug363-hi"}); err != nil {
				t.Fatalf("seed RD: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			body, _ := json.Marshal(apiv1.VolumeDefinitionCreate{
				VolumeDefinition: apiv1.VolumeDefinition{
					VolumeNumber: vn,
					SizeKib:      65536,
				},
			})

			resp := httpPost(t, base+"/v1/resource-definitions/rd-bug363-hi/volume-definitions", body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusBadRequest {
				got, _ := readAllBody(resp)
				t.Fatalf("status: got %d, want 400 for VlmNr=%d. Body: %s",
					resp.StatusCode, vn, got)
			}

			got, _ := readAllBody(resp)
			gotStr := string(got)

			if !strings.Contains(gotStr, "above maximum") {
				t.Errorf("envelope should name the rule (above maximum) for VlmNr=%d; got %s",
					vn, gotStr)
			}

			vds, _ := st.VolumeDefinitions().List(ctx, "rd-bug363-hi")
			if len(vds) != 0 {
				t.Errorf("VD must NOT be persisted on refusal for VlmNr=%d; got %d entries",
					vn, len(vds))
			}
		})
	}
}

// TestBug363VDCreateAcceptedAtBoundaries pins both bounds: VlmNr=0
// (the auto-assign zero value, also the explicit lower bound) and
// VlmNr=65535 (the DRBD-9 upper bound) must both be accepted. Guards
// against the gate being one-off in either direction.
func TestBug363VDCreateAcceptedAtBoundaries(t *testing.T) {
	t.Parallel()

	cases := []int32{0, 1, 65535}

	for _, vn := range cases {
		t.Run(http.StatusText(http.StatusOK), func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			ctx := t.Context()

			if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd-bug363-bnd"}); err != nil {
				t.Fatalf("seed RD: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			body, _ := json.Marshal(apiv1.VolumeDefinitionCreate{
				VolumeDefinition: apiv1.VolumeDefinition{
					VolumeNumber: vn,
					SizeKib:      65536,
				},
			})

			resp := httpPost(t, base+"/v1/resource-definitions/rd-bug363-bnd/volume-definitions", body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				got, _ := readAllBody(resp)
				t.Fatalf("status: got %d, want 200 for VlmNr=%d. Body: %s",
					resp.StatusCode, vn, got)
			}
		})
	}
}

// TestBug363VDCreateAutoAssignBypassesGate covers the Bug 191 happy
// path: when the body omits `volume_number`, the auto-assign branch
// fires and the gate is bypassed entirely (the assigned value is
// guaranteed to be in range by construction).
func TestBug363VDCreateAutoAssignBypassesGate(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd-bug363-auto"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Bare body without `volume_number` field — Bug 191 wire shape.
	body := []byte(`{"volume_definition":{"size_kib":65536}}`)

	resp := httpPost(t, base+"/v1/resource-definitions/rd-bug363-auto/volume-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 200. Body: %s", resp.StatusCode, got)
	}

	vds, _ := st.VolumeDefinitions().List(ctx, "rd-bug363-auto")
	if len(vds) != 1 {
		t.Fatalf("want 1 VD, got %d", len(vds))
	}

	if vds[0].VolumeNumber != 0 {
		t.Errorf("auto-assign on empty RD should pick VlmNr=0, got %d", vds[0].VolumeNumber)
	}
}
