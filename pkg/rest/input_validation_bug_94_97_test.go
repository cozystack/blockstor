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
	"errors"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bugs 94 + 97 reproducers, all wired through the REST surface so the
// guard at the wire boundary (not just the store layer) is exercised
// end-to-end. Both bugs ship as a single hardening pass on user-
// supplied identifiers + the Node-existence cross-check.
//
// Bug 94: `linstor r c bogus-node bogtest` creates a phantom Resource
// CRD pointing at a non-existent node. Expected: 4xx + envelope, no
// CRD persisted.
//
// Bug 97: `linstor rd c "  "` leaks the raw k8s "metadata.name is
// invalid: <hex>-..." apimachinery error to the operator, exposing
// pkg/store/k8s.Name()'s internal hash-prefix scheme. Expected: 400 +
// LINSTOR envelope naming the offending input and rule, no CRD
// persisted, no hash-prefix in the message.

// TestBug94ResourceCreateRefusedOnUnknownNode is the primary repro:
// POST a Resource against a Node CRD that doesn't exist. The REST
// handler must 404 with a LINSTOR envelope naming the missing node,
// and the Resource store must NOT carry an entry for the (rd, node)
// pair afterwards.
func TestBug94ResourceCreateRefusedOnUnknownNode(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "bogtest"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{NodeName: "bogus-node"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/bogtest/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 400 || resp.StatusCode >= 500 {
		t.Fatalf("status: got %d, want 4xx (Bug 94: bogus node must be refused)",
			resp.StatusCode)
	}

	got, _ := readAllBody(resp)
	if !strings.Contains(string(got), "bogus-node") {
		t.Errorf("envelope missing offending node name: %s", got)
	}

	// Envelope must be a LINSTOR-shaped []ApiCallRc, not a bare error.
	var rcs []apiv1.APICallRc

	if err := json.Unmarshal(got, &rcs); err != nil {
		t.Fatalf("body is not a []ApiCallRc envelope: %v\n%s", err, got)
	}

	if len(rcs) == 0 || rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("envelope ret_code does not carry MASK_ERROR: %+v", rcs)
	}

	// Phantom CRD must not be persisted.
	_, err := st.Resources().Get(ctx, "bogtest", "bogus-node")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Resource bogtest.bogus-node persisted despite 4xx: err=%v", err)
	}
}

// TestBug94AutoplaceRefusedOnUnknownNodeInFilter pins the autoplace
// variant: `linstor r c --auto-place 1 --node bogus-node <rd>` lands
// as `select_filter.node_name_list=["bogus-node"]`. The handler must
// refuse with the same LINSTOR envelope shape rather than fall through
// to the placer's generic "no candidate pools" shortfall.
func TestBug94AutoplaceRefusedOnUnknownNodeInFilter(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "bogtest"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:   1,
			NodeNameList: []string{"bogus-node"},
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/bogtest/autoplace", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 400 || resp.StatusCode >= 500 {
		t.Fatalf("status: got %d, want 4xx (Bug 94 autoplace variant)",
			resp.StatusCode)
	}

	got, _ := readAllBody(resp)
	if !strings.Contains(string(got), "bogus-node") {
		t.Errorf("envelope missing offending node name: %s", got)
	}

	// Phantom replica must not be persisted.
	_, err := st.Resources().Get(ctx, "bogtest", "bogus-node")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Resource bogtest.bogus-node persisted despite 4xx: err=%v", err)
	}
}

// TestBug94ResourceCreateAcceptedOnExistingNode is the happy-path
// counterpart: with the Node CRD pre-seeded, `r c <node> <rd>` lands
// as 201 + envelope and the Resource is persisted. Without this guard
// the new node-existence check could regress every existing CSI
// reconcile path.
func TestBug94ResourceCreateAcceptedOnExistingNode(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rdok"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed Node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{NodeName: "n1"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/rdok/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201. Body: %s", resp.StatusCode, got)
	}

	if _, err := st.Resources().Get(ctx, "rdok", "n1"); err != nil {
		t.Errorf("Resource rdok.n1 not persisted: %v", err)
	}
}

// TestBug97RDCreateRefusedOnWhitespaceName is the canonical Bug 97
// repro from kvaps' production environment: `linstor rd c "  "` used
// to leak the raw apimachinery `metadata.name: Invalid value:
// "6c179f21-": …` envelope. With the gate in place the request must
// 400 with a LINSTOR-shaped envelope that names the offending input
// + the rule it violated, and the K8s hash prefix MUST NOT appear in
// the response.
func TestBug97RDCreateRefusedOnWhitespaceName(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"resource_definition":{"name":"  "}}`)

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (Bug 97 whitespace-only name). Body: %s",
			resp.StatusCode, got)
	}

	got, _ := readAllBody(resp)
	if !strings.Contains(string(got), "resource definition name") {
		t.Errorf("envelope must name the offending object kind: %s", got)
	}

	// The raw k8s "metadata.name: Invalid value: \"<hex>-\":" string —
	// the symptom of the previous behaviour — MUST NOT appear in the
	// response: Bug 97 explicitly asks for the hash-prefix scheme to
	// be hidden from operators.
	if strings.Contains(string(got), "metadata.name") {
		t.Errorf("envelope leaks the k8s metadata.name internal: %s", got)
	}

	// RD must not be persisted under ANY name (slug or raw).
	rds, err := st.ResourceDefinitions().List(t.Context())
	if err != nil {
		t.Fatalf("list RDs: %v", err)
	}

	if len(rds) != 0 {
		t.Errorf("RD persisted despite 400 — gate is leaky: %+v", rds)
	}
}

// TestBug97RDCreateRefusedOnInvalidNameShapes drives the wire shape
// through several invalid forms the operator-facing CLI/REST may
// emit: uppercase + space (`Foo Bar`), pure uppercase, embedded dot,
// leading hyphen. All must 4xx with the same envelope shape; none
// must persist.
func TestBug97RDCreateRefusedOnInvalidNameShapes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		body string
	}{
		{"uppercase+space", `{"resource_definition":{"name":"Foo Bar"}}`},
		{"pure-uppercase", `{"resource_definition":{"name":"FOOBAR"}}`},
		{"embedded-dot", `{"resource_definition":{"name":"foo.bar"}}`},
		{"leading-hyphen", `{"resource_definition":{"name":"-foo"}}`},
		{"trailing-hyphen", `{"resource_definition":{"name":"foo-"}}`},
		{"empty", `{"resource_definition":{"name":""}}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()

			base, stop := startServerWithStore(t, st)
			defer stop()

			resp := httpPost(t, base+"/v1/resource-definitions", []byte(tc.body))
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusBadRequest {
				got, _ := readAllBody(resp)
				t.Fatalf("status: got %d, want 400. Body: %s",
					resp.StatusCode, got)
			}

			rds, err := st.ResourceDefinitions().List(t.Context())
			if err != nil {
				t.Fatalf("list RDs: %v", err)
			}

			if len(rds) != 0 {
				t.Errorf("RD persisted despite 400: %+v", rds)
			}
		})
	}
}

// TestBug97RDCreateAcceptedOnValidName is the happy-path: a clean
// RFC-1123 subdomain name still round-trips. Guards against the gate
// being over-strict on production names like `pvc-<uuid>` (linstor-csi
// volume IDs) and `dflt-rsc-grp`.
func TestBug97RDCreateAcceptedOnValidName(t *testing.T) {
	t.Parallel()

	cases := []string{
		"pvc-1",
		"pvc-c8a1d6b9-3e2f-4d1b-8e8f-2c5e9e8e8e8e",
		"foo123",
		"a",
	}

	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()

			base, stop := startServerWithStore(t, st)
			defer stop()

			body, _ := json.Marshal(apiv1.ResourceDefinitionCreate{
				ResourceDefinition: apiv1.ResourceDefinition{Name: name},
			})

			resp := httpPost(t, base+"/v1/resource-definitions", body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusCreated {
				got, _ := readAllBody(resp)
				t.Fatalf("status: got %d, want 201. Body: %s",
					resp.StatusCode, got)
			}

			if _, err := st.ResourceDefinitions().Get(t.Context(), name); err != nil {
				t.Errorf("RD %q not persisted: %v", name, err)
			}
		})
	}
}

// TestBug97NodeCreateRefusedOnInvalidName: the same validator must
// fire on `linstor n c "  "` / `linstor n c "Foo Bar"`. Without this
// the operator hit the same K8s metadata-leak path on node create as
// on RD create.
func TestBug97NodeCreateRefusedOnInvalidName(t *testing.T) {
	t.Parallel()

	cases := []string{"", "  ", "Foo Bar", "FOO", "node.with.dot"}

	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()

			base, stop := startServerWithStore(t, st)
			defer stop()

			body, _ := json.Marshal(apiv1.Node{
				Name: name,
				NetInterfaces: []apiv1.NetInterface{
					{Name: "default", Address: "10.0.0.5"},
				},
			})

			resp := httpPost(t, base+"/v1/nodes", body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusBadRequest {
				got, _ := readAllBody(resp)
				t.Fatalf("status: got %d, want 400 (name=%q). Body: %s",
					resp.StatusCode, name, got)
			}

			nodes, err := st.Nodes().List(t.Context())
			if err != nil {
				t.Fatalf("list nodes: %v", err)
			}

			if len(nodes) != 0 {
				t.Errorf("Node persisted despite 400 (name=%q): %+v", name, nodes)
			}
		})
	}
}

// TestBug97RGCreateRefusedOnInvalidName: same Bug-97 gate for
// `linstor rg c "Bad Name"`.
func TestBug97RGCreateRefusedOnInvalidName(t *testing.T) {
	t.Parallel()

	cases := []string{"", "  ", "Bad Name", "BAD"}

	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()

			base, stop := startServerWithStore(t, st)
			defer stop()

			body, _ := json.Marshal(apiv1.ResourceGroup{Name: name})

			resp := httpPost(t, base+"/v1/resource-groups", body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusBadRequest {
				got, _ := readAllBody(resp)
				t.Fatalf("status: got %d, want 400 (name=%q). Body: %s",
					resp.StatusCode, name, got)
			}

			rgs, err := st.ResourceGroups().List(t.Context())
			if err != nil {
				t.Fatalf("list RGs: %v", err)
			}

			// Strip the lazily-created DfltRscGrp the RD-create path
			// would have stamped — we never went through RD-create here,
			// so the store must remain empty.
			for _, rg := range rgs {
				if rg.Name == "" || rg.Name == "Bad Name" || rg.Name == "BAD" || rg.Name == "  " {
					t.Errorf("RG persisted despite 400 (name=%q)", name)
				}
			}
		})
	}
}
