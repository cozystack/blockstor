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

// Bug 132 — `linstor n c <existing-node> <new-ip>` silently overwrote
// the controller's stored NetInterface.Address. Resource .res files
// then referenced the new IP while the live satellite was still
// bound to the old IP, breaking DRBD wiring with no audit trail.
//
// Fix mirrors the Bug 92 / Bug 111 refusal pattern: `POST /v1/nodes`
// against an existing name whose NetInterface[].Address differs from
// the request body refuses with 409 + LINSTOR envelope that lists the
// current address and points at `linstor n modify` (or `?force=true`).
// Same name + same IP is the idempotent path the cozystack reconciler
// relies on (Bug 66 contract) and MUST keep returning 201.

// TestBug132NodeCreateRefusesIPRewriteOfExistingNode pins the refusal
// for the canonical reproducer: seed `n1` with `10.0.0.1`, POST a new
// body with the same name but `10.0.0.2`. Must surface 409 + envelope
// echoing the current IP; the stored Node MUST keep the original IP.
func TestBug132NodeCreateRefusesIPRewriteOfExistingNode(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.1"},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.2"},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/nodes", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		raw, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 409 (Bug 132 IP-rewrite refusal). Body: %s",
			resp.StatusCode, raw)
	}

	raw, _ := readAllBody(resp)

	var rcs []apiv1.APICallRc
	if jsonErr := json.Unmarshal(raw, &rcs); jsonErr != nil {
		t.Fatalf("body is not a []ApiCallRc envelope: %v\n%s", jsonErr, raw)
	}

	if len(rcs) == 0 || rcs[0].RetCode&apiCallRcError == 0 {
		t.Fatalf("envelope ret_code does not carry MASK_ERROR: %+v", rcs)
	}

	// The envelope's cause MUST echo the current stored IP so the
	// operator can see exactly what they would have overwritten.
	low := strings.ToLower(rcs[0].Message + " " + rcs[0].Cause + " " + rcs[0].Correc)
	if !strings.Contains(low, "10.0.0.1") {
		t.Errorf("envelope must echo current IP 10.0.0.1; got message=%q cause=%q correction=%q",
			rcs[0].Message, rcs[0].Cause, rcs[0].Correc)
	}

	if !strings.Contains(rcs[0].Correc, "force=true") {
		t.Errorf("correction: missing force=true escape hatch; got %q", rcs[0].Correc)
	}

	// State MUST be unchanged — the refused operation must NOT have
	// touched the Node's NetInterfaces.
	got, err := st.Nodes().Get(ctx, "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if len(got.NetInterfaces) != 1 || got.NetInterfaces[0].Address != "10.0.0.1" {
		t.Errorf("NetInterfaces silently overwritten: got %+v, want default=10.0.0.1 (refused)",
			got.NetInterfaces)
	}
}

// TestBug132NodeCreateIdempotentSameIPAccepted pins the idempotency
// path: re-posting the SAME (name, ip) pair must still succeed — the
// cozystack reconciler re-issues `n c <name> <ip>` on every operator
// restart (Bug 66 contract), and that retry must not surface 409.
func TestBug132NodeCreateIdempotentSameIPAccepted(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.1"},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.1"},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/nodes", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 200 or 201 (idempotent same-IP repost)", resp.StatusCode)
	}

	got, err := st.Nodes().Get(ctx, "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if len(got.NetInterfaces) != 1 || got.NetInterfaces[0].Address != "10.0.0.1" {
		t.Errorf("NetInterfaces: got %+v, want default=10.0.0.1 (idempotency preserved)",
			got.NetInterfaces)
	}
}

// TestBug132NodeCreateWithForceTrueOverwrites pins the escape hatch.
// Operators in an actual IP-renumber emergency need a path forward;
// `?force=true` accepts the rewrite, matching the same query-string
// override Bug 92 / Bug 111 wired for the delete handlers.
func TestBug132NodeCreateWithForceTrueOverwrites(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.1"},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.2"},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/nodes?force=true", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201 (force=true overwrite)", resp.StatusCode)
	}

	got, err := st.Nodes().Get(ctx, "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if len(got.NetInterfaces) != 1 || got.NetInterfaces[0].Address != "10.0.0.2" {
		t.Errorf("NetInterfaces with force=true: got %+v, want default=10.0.0.2",
			got.NetInterfaces)
	}
}

// Bug 142 — `linstor n dp <node> <key>` returned 404 because the REST
// surface never registered a per-key DELETE route for node properties.
// Operators had to fall through to PUT /v1/nodes/{node} with
// `delete_props: [...]`. The controller-property scope already exposes
// `DELETE /v1/controller/properties/{key...}`; the node scope must
// mirror the same shape so the python CLI's `n dp` works without
// special-casing the namespace.

// TestBug142NodeDeletePropertyEnvelope pins the happy path: seed a
// property, DELETE it through the per-key endpoint, and confirm the
// property is gone and the envelope is the LINSTOR-canonical info
// shape (200 + `[]ApiCallRc`).
func TestBug142NodeDeletePropertyEnvelope(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		Props: map[string]string{
			"Aux/rack-id":                 "rack-7",
			"DrbdOptions/Net/sndbuf-size": "1048576",
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/n1/properties/Aux/rack-id")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		raw, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 200 (Bug 142 per-key DELETE). Body: %s",
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

	got, err := st.Nodes().Get(ctx, "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if _, present := got.Props["Aux/rack-id"]; present {
		t.Errorf("Props[Aux/rack-id]: still present after DELETE; got %+v", got.Props)
	}

	// Sibling key must survive — the per-key DELETE must NOT nuke
	// the whole property bag.
	if got.Props["DrbdOptions/Net/sndbuf-size"] != "1048576" {
		t.Errorf("Props[DrbdOptions/Net/sndbuf-size]: collateral-deleted; got %+v", got.Props)
	}
}

// TestBug142NodeDeletePropertyNoSuchKey pins the idempotency-on-
// absent-key clause: LINSTOR treats "delete a property that wasn't
// set" as a no-op, not a 404, so reconciler-style retry loops don't
// hot-spin on the second pass. Mirrors the controller-property
// DELETE behaviour (`controller_props.go::deleteControllerProp`).
func TestBug142NodeDeletePropertyNoSuchKey(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	if err := st.Nodes().Create(t.Context(), &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/n1/properties/Aux/ghost")
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

	// Warn-mask is the canonical "already absent" surface in this
	// codebase (cf. warnNodeNotFound on `n d`, warnStoragePoolNotFound,
	// etc.). MASK_ERROR must NOT be set — `n dp <missing>` is success.
	if rcs[0].RetCode&apiCallRcError != 0 {
		t.Errorf("envelope ret_code carries MASK_ERROR (no-op delete should succeed): %+v", rcs[0])
	}

	low := strings.ToLower(rcs[0].Message)
	if !strings.Contains(low, "absent") && !strings.Contains(low, "not") {
		t.Errorf("envelope message should mark the key as absent; got %q", rcs[0].Message)
	}
}
