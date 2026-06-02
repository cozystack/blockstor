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

// Bug 370: POST /v1/nodes used to surface HTTP 500 + the raw k8s CEL
// rejection ("spec.type: Unsupported value …") when the body carried
// a `type` outside the upstream enum (e.g. `"type":"INVALID"`).
// Bug-hunt v7 (2026-06-02) reproduced on a live dev stand:
//
//   $ curl -sS -X POST http://.../v1/nodes -d '{"name":"badnode","type":"INVALID","net_interfaces":[...]}'
//   [{"ret_code":-4611686018427387904,
//     "message":"store error: create Node \"badnode\": <backend>
//                \"badnode\" is invalid: spec.type: Unsupported value:
//                \"INVALID\": supported values: \"CONTROLLER\", ..."}]
//   HTTP=500
//
// python-linstor (and golinstor) classify the response by RetCode bits;
// the bare apiCallRcError carries MASK_ERROR only — no sub-code — so
// `ApiCallError.Is(FAIL_INVLD_NODE_TYPE)` returns false and the CLI
// surfaces an opaque internal error instead of an actionable refusal.
//
// The fix wires `validateNodeType` at the wire boundary BEFORE the
// store path, mirroring how Bug 97 / Bug 120 / Bug 368 / Bug 369 / Bug
// 371 already gate other typed enum fields. Returns 400 +
// FAIL_INVLD_NODE_TYPE (upstream sub-code 430) with the offending
// value + the accepted enumeration listed inline.

// TestBug370POSTNodeRefusesInvalidType pins the reproducer body.
func TestBug370POSTNodeRefusesInvalidType(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.Node{
		Name: "badnode",
		Type: "INVALID",
		NetInterfaces: []apiv1.NetInterface{
			{Name: DefaultNetInterfaceName, Address: "10.0.0.10"},
		},
	})

	resp := httpPost(t, base+"/v1/nodes", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (Bug 370 invalid node type). Body: %s",
			resp.StatusCode, got)
	}

	got, _ := readAllBody(resp)
	if !strings.Contains(string(got), "INVALID") {
		t.Errorf("envelope missing offending value: %s", got)
	}

	if !strings.Contains(string(got), "SATELLITE") {
		t.Errorf("envelope missing accepted-value enumeration: %s", got)
	}

	// FAIL_INVLD_NODE_TYPE (430) sub-code must be set so
	// golinstor's `Is(FAIL_INVLD_NODE_TYPE)` returns true.
	var rcs []apiv1.APICallRc
	if err := json.Unmarshal(got, &rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) != 1 {
		t.Fatalf("envelope length: got %d, want 1", len(rcs))
	}

	const failInvldNodeType = int64(430)
	if rcs[0].RetCode&failInvldNodeType != failInvldNodeType {
		t.Errorf("ret_code: got %#x, want FAIL_INVLD_NODE_TYPE (430) bit set",
			rcs[0].RetCode)
	}

	// Node must NOT have been persisted.
	if _, err := st.Nodes().Get(t.Context(), "badnode"); err == nil {
		t.Error("Node 'badnode' was persisted despite the refusal")
	}
}

// TestBug370POSTNodeAcceptsEmptyType pins the empty-type fallthrough.
// The canonical `linstor n c <name> <ip>` body has no `type` key; the
// store-write path defaults missing Type to SATELLITE, so the validator
// must accept "" without surfacing the refusal envelope.
func TestBug370POSTNodeAcceptsEmptyType(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.Node{
		Name: "okay",
		NetInterfaces: []apiv1.NetInterface{
			{Name: DefaultNetInterfaceName, Address: "10.0.0.11"},
		},
	})

	resp := httpPost(t, base+"/v1/nodes", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201 (empty type must default through). Body: %s",
			resp.StatusCode, got)
	}
}

// TestBug370POSTNodeAcceptsCanonicalTypes pins the upstream enum
// values one-by-one so a future schema drift doesn't silently break
// canonical types alongside the bad-enum refusal.
func TestBug370POSTNodeAcceptsCanonicalTypes(t *testing.T) {
	t.Parallel()

	for _, typ := range []string{
		apiv1.NodeTypeSatellite,
		apiv1.NodeTypeController,
		apiv1.NodeTypeCombined,
		apiv1.NodeTypeAuxiliary,
	} {
		t.Run(typ, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()

			base, stop := startServerWithStore(t, st)
			defer stop()

			body, _ := json.Marshal(apiv1.Node{
				Name: "node-" + strings.ToLower(typ),
				Type: typ,
				NetInterfaces: []apiv1.NetInterface{
					{Name: DefaultNetInterfaceName, Address: "10.0.0.12"},
				},
			})

			resp := httpPost(t, base+"/v1/nodes", body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusCreated {
				got, _ := readAllBody(resp)
				t.Fatalf("status: got %d, want 201 for type %q. Body: %s",
					resp.StatusCode, typ, got)
			}
		})
	}
}

// TestBug370POSTNodeAcceptsLowercaseType pins the case-insensitive
// branch: upstream LINSTOR accepts "satellite" as a valid alias for
// "SATELLITE" (CtrlApiCallHandler.normalizeNodeType uppercases the
// input). The CRD admission also normalises before its enum check,
// so we mirror that behaviour rather than refuse a legal CLI input.
func TestBug370POSTNodeAcceptsLowercaseType(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.Node{
		Name: "lowercase-type",
		Type: "satellite",
		NetInterfaces: []apiv1.NetInterface{
			{Name: DefaultNetInterfaceName, Address: "10.0.0.13"},
		},
	})

	resp := httpPost(t, base+"/v1/nodes", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201 (case-insensitive accept). Body: %s",
			resp.StatusCode, got)
	}
}
