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
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// H3 corner-case (toggle-disk state-machine campaign): the modern
// `linstor r c <node> <rd> --drbd-diskless` CLI flag posts the wire
// flag DRBD_DISKLESS, whereas the DEPRECATED `--diskless` alias posts
// the canonical DISKLESS. Verified empirically via `linstor --curl`
// against the upstream 1.33.2 oracle (client 1.27.1):
//
//	r c <n> <rd> --drbd-diskless -> {"resource":{... "flags":["DRBD_DISKLESS"]}}
//	r c <n> <rd> --diskless      -> {"resource":{... "flags":["DISKLESS"]}}
//	r c <n> <rd> -s pool         -> {"resource":{... "props":{"StorPoolName":"pool"}}}
//
// blockstor's diskless-detection surface (the placer's splitByDiskless,
// the satellite's applyStorageIfDiskful, the quorum/tiebreaker math)
// keys exclusively on apiv1.ResourceFlagDiskless == "DISKLESS". Without
// normalising DRBD_DISKLESS into DISKLESS at the create wire boundary,
// a replica requested with the RECOMMENDED `--drbd-diskless` flag would
// be persisted as a diskful intent: the satellite would carve a backing
// LV/ZVOL for a replica the operator asked to be diskless, and the
// quorum arithmetic would miscount it. These tests pin the wire-boundary
// canonicalisation so both spellings converge on the single internal
// DISKLESS flag.

// TestResourceCreateDrbdDisklessNormalised pins the load-bearing fix:
// a create body carrying ONLY DRBD_DISKLESS lands in the store as the
// canonical DISKLESS, with no backing storage pool resolved (the
// resolveStorPoolForFreshCreate short-circuit keys on the canonical
// flag, so a mis-classified diskful would otherwise get a pool stamped).
func TestResourceCreateDrbdDisklessNormalised(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-drbd-dl"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Nodes().Create(t.Context(), &apiv1.Node{Name: "worker-1"}); err != nil {
		t.Fatalf("seed Node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Exactly the body `linstor r c worker-1 pvc-drbd-dl --drbd-diskless`
	// posts (single-entry envelope shape).
	body, _ := json.Marshal([]apiv1.ResourceCreate{{
		Resource: apiv1.Resource{NodeName: "worker-1", Flags: []string{"DRBD_DISKLESS"}},
	}})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-drbd-dl/resources", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-drbd-dl", "worker-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	// Canonical DISKLESS present so every downstream diskless-keyed
	// branch (splitByDiskless, applyStorageIfDiskful, quorum math)
	// classifies the replica as diskless.
	if !slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("canonical DISKLESS missing after --drbd-diskless create: %v", got.Flags)
	}

	// The raw DRBD_DISKLESS spelling must be gone — a lingering
	// DRBD_DISKLESS would be invisible to the DISKLESS-keyed walks.
	if slices.Contains(got.Flags, "DRBD_DISKLESS") {
		t.Errorf("raw DRBD_DISKLESS survived normalisation: %v", got.Flags)
	}

	// A correctly-classified diskless replica resolves NO storage pool
	// (resolveStorPoolForFreshCreate returns early on DISKLESS). A pool
	// stamp here would prove the replica was mis-classified as diskful.
	if got.Props["StorPoolName"] != "" {
		t.Errorf("diskless replica got a StorPoolName stamped (mis-classified as diskful): %q",
			got.Props["StorPoolName"])
	}
}

// TestResourceCreateDeprecatedDisklessStillWorks pins that the
// deprecated `--diskless` alias (wire flag DISKLESS) is preserved
// verbatim — it was already the canonical spelling, so normalisation
// is a no-op for it. Guards against a regression where the normaliser
// accidentally drops or duplicates the canonical flag.
func TestResourceCreateDeprecatedDisklessStillWorks(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-dep-dl"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Nodes().Create(t.Context(), &apiv1.Node{Name: "worker-2"}); err != nil {
		t.Fatalf("seed Node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal([]apiv1.ResourceCreate{{
		Resource: apiv1.Resource{NodeName: "worker-2", Flags: []string{"DISKLESS"}},
	}})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-dep-dl/resources", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-dep-dl", "worker-2")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	disklessCount := 0

	for _, f := range got.Flags {
		if f == apiv1.ResourceFlagDiskless {
			disklessCount++
		}
	}

	if disklessCount != 1 {
		t.Errorf("DISKLESS flag count after --diskless create: got %d, want 1; flags=%v",
			disklessCount, got.Flags)
	}
}

// TestNormalizeDisklessFlag unit-tests the canonicalisation helper
// directly across the cases the create handler can hand it: the
// no-op paths (diskful, already-canonical), the rewrite path
// (DRBD_DISKLESS alone), and the de-dup path (BOTH spellings present,
// which must collapse to a single canonical DISKLESS so a later
// exact-match flag walk doesn't see a phantom duplicate). Other flags
// must survive untouched and keep their relative order.
func TestNormalizeDisklessFlag(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want []string
	}{
		{"nil", nil, nil},
		{"diskful-noop", []string{}, []string{}},
		{"already-canonical", []string{"DISKLESS"}, []string{"DISKLESS"}},
		{"drbd-diskless-alone", []string{"DRBD_DISKLESS"}, []string{"DISKLESS"}},
		{
			"drbd-diskless-with-tiebreaker",
			[]string{"DRBD_DISKLESS", "TIE_BREAKER"},
			[]string{"DISKLESS", "TIE_BREAKER"},
		},
		{
			"both-spellings-dedup",
			[]string{"DRBD_DISKLESS", "DISKLESS"},
			[]string{"DISKLESS"},
		},
		{
			"unrelated-flags-untouched",
			[]string{"INACTIVE", "EVACUATE"},
			[]string{"INACTIVE", "EVACUATE"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := &apiv1.Resource{Flags: tc.in}
			normalizeDisklessFlag(res)

			if !slices.Equal(res.Flags, tc.want) {
				t.Errorf("normalizeDisklessFlag(%v): got %v, want %v", tc.in, res.Flags, tc.want)
			}
		})
	}
}
