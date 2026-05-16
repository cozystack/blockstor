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

// Bug 156 — `linstor rd ap --place-count 2 --diskless-on-remaining
// false -- <rd>` still ended up with a TIE_BREAKER diskless replica
// on the third node. The placer's `diskless_on_remaining` branch
// (existing behaviour) honours the flag, but the auto-tiebreaker
// reconciler in `internal/controller` independently stamps a
// TIE_BREAKER witness whenever an RD has exactly 2 diskful replicas.
// The operator's intent — "no diskless residual, including the
// witness" — was silently ignored.
//
// The fix wires the REST autoplace handler to translate
// `diskless_on_remaining=false` into a per-RD opt-out from the
// auto-tiebreaker reconciler:
// `DrbdOptions/AutoAddQuorumTiebreaker=false` on the target RD.
// The controller's `isAutoTieBreakerEnabled` already reads this
// prop, so once the prop is stamped the witness creation path is a
// no-op for the RD.
//
// Distinguishing "explicit false" from "field absent" matters
// because the default is `true` (witness enabled). We probe the raw
// request body for the literal key — same pattern Bug 60 uses on RG
// PATCH for select-filter presence detection.

// commonBug156Setup seeds a 3-node + 1-pool cluster shared by every
// flag-state variant. Hoisted into a helper to keep each test case
// focused on its assertion.
func commonBug156Setup(t *testing.T, rdName string) (store.Store, string, func()) {
	t.Helper()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", n, err)
		}
	}

	base, stop := startServerWithStore(t, st)

	return st, base, stop
}

// TestBug156DisklessOnRemainingFalseSuppressesTiebreaker pins the
// core fix: explicit `diskless_on_remaining=false` on the autoplace
// request stamps `DrbdOptions/AutoAddQuorumTiebreaker=false` on the
// RD so the reconciler doesn't add a TIE_BREAKER witness.
//
// The REST handler doesn't materialise the witness inline (that's
// the controller's job); the contract pinned here is "the operator's
// intent is recorded as an RD-level prop", which the controller
// then honours via `isAutoTieBreakerEnabled`.
func TestBug156DisklessOnRemainingFalseSuppressesTiebreaker(t *testing.T) {
	st, base, stop := commonBug156Setup(t, "bug156-false")
	defer stop()

	body, _ := json.Marshal(map[string]any{
		"diskless_on_remaining": false,
		"select_filter": map[string]any{
			"place_count":  2,
			"storage_pool": "pool",
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/bug156-false/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	rd, err := st.ResourceDefinitions().Get(t.Context(), "bug156-false")
	if err != nil {
		t.Fatalf("get RD: %v", err)
	}

	// Bug 156: the explicit "false" must persist as a per-RD
	// auto-tiebreaker opt-out so the controller's reconciler
	// suppresses witness creation.
	const propKey = "DrbdOptions/AutoAddQuorumTiebreaker"

	got := rd.Props[propKey]
	if got != "false" {
		t.Errorf("Bug 156: %s: got %q, want %q "+
			"(diskless_on_remaining=false must record auto-witness opt-out)",
			propKey, got, "false")
	}

	// Sanity: the placer must not have placed a diskless replica
	// on n3 — only 2 diskful replicas total.
	resources, err := st.Resources().ListByDefinition(t.Context(), "bug156-false")
	if err != nil {
		t.Fatalf("list replicas: %v", err)
	}

	if len(resources) != 2 {
		t.Errorf("Bug 156: placed %d replicas, want exactly 2 (no diskless residual)",
			len(resources))
	}

	for _, r := range resources {
		if slices.Contains(r.Flags, apiv1.ResourceFlagDiskless) ||
			slices.Contains(r.Flags, apiv1.ResourceFlagTieBreaker) {
			t.Errorf("Bug 156: replica on %s has DISKLESS/TIE_BREAKER flags %v "+
				"(diskless_on_remaining=false must suppress all diskless residual)",
				r.NodeName, r.Flags)
		}
	}
}

// TestBug156DisklessOnRemainingTrueStillCreatesTiebreaker is the
// inverse pin: when the operator OPTS IN to diskless-on-remaining,
// the REST handler must NOT stamp the auto-tiebreaker opt-out. The
// controller's default path then creates the witness as usual.
func TestBug156DisklessOnRemainingTrueStillCreatesTiebreaker(t *testing.T) {
	st, base, stop := commonBug156Setup(t, "bug156-true")
	defer stop()

	body, _ := json.Marshal(map[string]any{
		"diskless_on_remaining": true,
		"select_filter": map[string]any{
			"place_count":  2,
			"storage_pool": "pool",
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/bug156-true/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	rd, err := st.ResourceDefinitions().Get(t.Context(), "bug156-true")
	if err != nil {
		t.Fatalf("get RD: %v", err)
	}

	const propKey = "DrbdOptions/AutoAddQuorumTiebreaker"

	got := rd.Props[propKey]
	if got == "false" {
		t.Errorf("Bug 156: %s must NOT be stamped 'false' when "+
			"diskless_on_remaining=true; got %q (auto-witness must stay enabled)",
			propKey, got)
	}
}

// TestBug156DisklessOnRemainingUnsetDefaultsToTrue pins the
// no-flag-supplied path: a bare autoplace body (no
// `diskless_on_remaining` key at all) must behave like the
// happy-default — auto-witness stays enabled. The REST handler
// must not over-react to JSON's zero value (`false`) when the
// field was simply absent.
func TestBug156DisklessOnRemainingUnsetDefaultsToTrue(t *testing.T) {
	st, base, stop := commonBug156Setup(t, "bug156-unset")
	defer stop()

	// Body intentionally omits diskless_on_remaining entirely.
	body, _ := json.Marshal(map[string]any{
		"select_filter": map[string]any{
			"place_count":  2,
			"storage_pool": "pool",
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/bug156-unset/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	rd, err := st.ResourceDefinitions().Get(t.Context(), "bug156-unset")
	if err != nil {
		t.Fatalf("get RD: %v", err)
	}

	const propKey = "DrbdOptions/AutoAddQuorumTiebreaker"

	got := rd.Props[propKey]
	if got == "false" {
		t.Errorf("Bug 156: %s must NOT be stamped 'false' when "+
			"diskless_on_remaining is absent; got %q "+
			"(zero-value-vs-explicit-false must be distinguished)",
			propKey, got)
	}
}
