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

// Corner-case D6 — BalanceResources property surface
// (linstor-administration.adoc ~887-907).
//
// Upstream LINSTOR ≥1.26.0 runs a periodic BalanceResources task that
// tops replica counts back up to the RG place-count; it is disabled
// via the `BalanceResourcesEnabled` property (RD > RG > controller
// resolution). blockstor has its OWN equivalent — the
// RGRebalanceReconciler (internal/controller/rg_rebalance_controller.go)
// — which honours `BalanceResourcesEnabled=false` at controller scope
// (pinned by TestRGRebalanceReconcilerHonoursBalanceResourcesDisabled).
//
// This file pins the PROPERTY-SURFACE half of corner D6: blockstor
// ACCEPTS `controller set-property BalanceResourcesEnabled <v>` exactly
// like upstream (accept-inert; stored verbatim, round-trips through
// list-properties). Oracle capture on the dev stand 2026-06-04:
//
//   $ linstor controller set-property BalanceResourcesEnabled false
//   SUCCESS  (exit 0)
//   $ linstor controller list-properties | grep -i balance
//   | BalanceResourcesEnabled | false |
//
// The property bag is permissive (no per-key value schema) on both
// sides, so a typo'd value is accepted too — the reconciler treats any
// value other than the literal "false" as "enabled" (default-on),
// matching upstream's default-enabled semantics.

// TestCornerD6_BalanceResourcesEnabledAcceptedAtController pins that a
// controller-scope `BalanceResourcesEnabled` write is accepted (201)
// and round-trips through GET /v1/controller/properties — matching the
// oracle's accept-inert behaviour.
func TestCornerD6_BalanceResourcesEnabledAcceptedAtController(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	for _, val := range []string{"false", "true"} {
		body, _ := json.Marshal(apiv1.GenericPropsModify{
			OverrideProps: map[string]string{
				apiv1.PropBalanceResourcesEnabled: val,
			},
		})

		resp := httpPost(t, base+"/v1/controller/properties", body)
		status := resp.StatusCode
		_ = resp.Body.Close()

		if status != http.StatusCreated {
			t.Fatalf("set BalanceResourcesEnabled=%q: status %d, want 201", val, status)
		}

		got := getControllerProps(t, base)
		if got[apiv1.PropBalanceResourcesEnabled] != val {
			t.Errorf("BalanceResourcesEnabled round-trip: got %q, want %q",
				got[apiv1.PropBalanceResourcesEnabled], val)
		}
	}
}

// TestCornerD6_BalanceResourcesTuningAcceptedAtController pins that the
// Interval / GracePeriod tuning knobs are likewise accept-inert at
// controller scope. NOTE the documented unit divergence (whitelisted in
// docs/cli-parity-known-deltas.md): blockstor interprets these values
// as MINUTES, upstream as SECONDS. The property surface accepts the raw
// string the same way; only the reconciler's interpretation differs.
func TestCornerD6_BalanceResourcesTuningAcceptedAtController(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{
			apiv1.PropBalanceResourcesInterval:    "10",
			apiv1.PropBalanceResourcesGracePeriod: "30",
		},
	})

	resp := httpPost(t, base+"/v1/controller/properties", body)
	status := resp.StatusCode
	_ = resp.Body.Close()

	if status != http.StatusCreated {
		t.Fatalf("set BalanceResources tuning: status %d, want 201", status)
	}

	got := getControllerProps(t, base)
	if got[apiv1.PropBalanceResourcesInterval] != "10" {
		t.Errorf("BalanceResourcesInterval round-trip: got %q, want \"10\"",
			got[apiv1.PropBalanceResourcesInterval])
	}

	if got[apiv1.PropBalanceResourcesGracePeriod] != "30" {
		t.Errorf("BalanceResourcesGracePeriod round-trip: got %q, want \"30\"",
			got[apiv1.PropBalanceResourcesGracePeriod])
	}
}

// getControllerProps GETs the controller property bag.
func getControllerProps(t *testing.T, base string) map[string]string {
	t.Helper()

	resp := httpGet(t, base+"/v1/controller/properties")
	defer func() { _ = resp.Body.Close() }()

	var props map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&props); err != nil {
		t.Fatalf("decode controller props: %v", err)
	}

	return props
}
