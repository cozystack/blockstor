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

// Bug 371: PUT /v1/nodes/{node}/net-interfaces/{name} was un-validated
// on both address and satellite_port. Bug-hunt v6 (2026-06-02)
// reproduced on the dev stand against a live, Online satellite —
// `PUT .../net-interfaces/default {"address":"garbage","satellite_port":-7}`
// returned HTTP 200 + a laconic "net-interface PUT " envelope, and
// the GET /v1/nodes/<n>/net-interfaces/default that followed showed
// address=garbage, satellite_port=-7 persisted into the live Node
// spec. That was enough to deform the actual controller→satellite
// handshake of a production node via an unauthenticated REST call.
//
// The fix wires three gates onto mutateNetInterface (which both POST
// and PUT share):
//
//  1. validateNetInterfaceAddresses — same rule handleNodeCreate's
//     Bug 120 fix uses: parseable IP literal or DNS-resolvable
//     hostname.
//  2. validateNetInterfacePorts — new helper, satellite_port must be
//     in [1, 65535] or 0 ("inherit default"). Covers Bug 368 (port
//     -1) and Bug 369 (port 99999) on POST /v1/nodes too.
//  3. Stamp the path's {name} into iface BEFORE either validator so
//     the error envelope always names the actual target, and the
//     trailing-space message bug ("net-interface PUT ") goes away —
//     now reads "net-interface modified: default" / "created: ...".

// TestBug371PUTNetInterfaceRefusesNegativePort pins the canonical
// satellite_port=-7 reproducer. Before the fix the value persisted
// into Node.NetInterfaces[0].SatellitePort=-7; after, the request
// returns 400 + LINSTOR envelope and the stored value stays untouched.
func TestBug371PUTNetInterfaceRefusesNegativePort(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	seed := &apiv1.Node{
		Name: "worker1",
		NetInterfaces: []apiv1.NetInterface{
			{Name: DefaultNetInterfaceName, Address: "10.0.0.1", SatellitePort: 3366},
		},
	}
	if err := st.Nodes().Create(ctx, seed); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.NetInterface{
		Address:       "10.0.0.1",
		SatellitePort: -7,
	})

	resp := httpPut(t, base+"/v1/nodes/worker1/net-interfaces/default", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (Bug 371 negative satellite_port). Body: %s",
			resp.StatusCode, got)
	}

	got, _ := readAllBody(resp)
	if !strings.Contains(string(got), "satellite_port -7 out of range") {
		t.Errorf("envelope missing offending port: %s", got)
	}

	// Persisted port must be untouched.
	stored, err := st.Nodes().Get(ctx, "worker1")
	if err != nil {
		t.Fatalf("re-fetch node: %v", err)
	}

	if got, want := stored.NetInterfaces[0].SatellitePort, 3366; got != want {
		t.Errorf("stored satellite_port: got %d, want %d (must not persist after rejection)", got, want)
	}
}

// TestBug371PUTNetInterfaceRefusesHugePort pins the >65535 variant
// (Bug 369 on the PUT path).
func TestBug371PUTNetInterfaceRefusesHugePort(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	seed := &apiv1.Node{
		Name: "worker1",
		NetInterfaces: []apiv1.NetInterface{
			{Name: DefaultNetInterfaceName, Address: "10.0.0.1", SatellitePort: 3366},
		},
	}
	if err := st.Nodes().Create(ctx, seed); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.NetInterface{
		Address:       "10.0.0.1",
		SatellitePort: 99999,
	})

	resp := httpPut(t, base+"/v1/nodes/worker1/net-interfaces/default", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (Bug 371 satellite_port>65535). Body: %s",
			resp.StatusCode, got)
	}

	got, _ := readAllBody(resp)
	if !strings.Contains(string(got), "satellite_port 99999 out of range") {
		t.Errorf("envelope missing offending port: %s", got)
	}
}

// TestBug371PUTNetInterfaceRefusesGarbageAddress pins the
// address="garbage" reproducer on PUT. Bug 120 already covered POST
// /v1/nodes; this fills the symmetric gap on the per-NetInterface
// PUT route.
func TestBug371PUTNetInterfaceRefusesGarbageAddress(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	seed := &apiv1.Node{
		Name: "worker1",
		NetInterfaces: []apiv1.NetInterface{
			{Name: DefaultNetInterfaceName, Address: "10.0.0.1", SatellitePort: 3366},
		},
	}
	if err := st.Nodes().Create(ctx, seed); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	srv := strictDNSServer(t, st)

	base, stop := startServerCustom(t, srv)
	defer stop()

	body, _ := json.Marshal(apiv1.NetInterface{
		Address:       "garbage",
		SatellitePort: 3366,
	})

	resp := httpPut(t, base+"/v1/nodes/worker1/net-interfaces/default", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (Bug 371 garbage address). Body: %s",
			resp.StatusCode, got)
	}

	got, _ := readAllBody(resp)
	if !strings.Contains(string(got), `garbage`) || !strings.Contains(string(got), "not a valid IPv4") {
		t.Errorf("envelope missing offending address or validator hint: %s", got)
	}

	stored, err := st.Nodes().Get(ctx, "worker1")
	if err != nil {
		t.Fatalf("re-fetch node: %v", err)
	}

	if got, want := stored.NetInterfaces[0].Address, "10.0.0.1"; got != want {
		t.Errorf("stored address: got %q, want %q (must not persist after rejection)", got, want)
	}
}

// TestBug371PUTNetInterfaceValidPayloadStillWorks pins the
// happy-path: a perfectly valid PUT must still succeed and return
// the polished envelope shape (no trailing space, "modified: <name>"
// verb), with the new value persisted.
func TestBug371PUTNetInterfaceValidPayloadStillWorks(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	seed := &apiv1.Node{
		Name: "worker1",
		NetInterfaces: []apiv1.NetInterface{
			{Name: DefaultNetInterfaceName, Address: "10.0.0.1", SatellitePort: 3366},
		},
	}
	if err := st.Nodes().Create(ctx, seed); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.NetInterface{
		Address:       "10.0.0.2",
		SatellitePort: 3367,
	})

	resp := httpPut(t, base+"/v1/nodes/worker1/net-interfaces/default", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 200. Body: %s", resp.StatusCode, got)
	}

	got, _ := readAllBody(resp)

	var rcs []apiv1.APICallRc
	if err := json.Unmarshal(got, &rcs); err != nil {
		t.Fatalf("body is not a []ApiCallRc envelope: %v\n%s", err, got)
	}

	if len(rcs) == 0 {
		t.Fatalf("empty envelope: %s", got)
	}

	// The post-fix envelope must say "modified: default" and must
	// not carry the legacy "net-interface PUT " trailing-space shape.
	if want := "net-interface modified: default"; rcs[0].Message != want {
		t.Errorf("envelope message: got %q, want %q", rcs[0].Message, want)
	}

	stored, err := st.Nodes().Get(ctx, "worker1")
	if err != nil {
		t.Fatalf("re-fetch node: %v", err)
	}

	if got, want := stored.NetInterfaces[0].Address, "10.0.0.2"; got != want {
		t.Errorf("stored address: got %q, want %q", got, want)
	}

	if got, want := stored.NetInterfaces[0].SatellitePort, 3367; got != want {
		t.Errorf("stored satellite_port: got %d, want %d", got, want)
	}
}

// TestBug368NodeCreateRefusesNegativePort pins the symmetric gate on
// POST /v1/nodes — the same validateNetInterfacePorts helper guards
// `n c bogusnode 10.99.99.99 -1` from minting an offline Node CRD
// with a permanently-broken port.
func TestBug368NodeCreateRefusesNegativePort(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.Node{
		Name: "badportnode",
		Type: "SATELLITE",
		NetInterfaces: []apiv1.NetInterface{
			{Name: DefaultNetInterfaceName, Address: "10.99.99.99", SatellitePort: -1},
		},
	})

	resp := httpPost(t, base+"/v1/nodes", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (Bug 368 negative satellite_port on n c). Body: %s",
			resp.StatusCode, got)
	}

	got, _ := readAllBody(resp)
	if !strings.Contains(string(got), "satellite_port -1 out of range") {
		t.Errorf("envelope missing offending port: %s", got)
	}

	// No phantom node CRD must land.
	if _, err := st.Nodes().Get(t.Context(), "badportnode"); err == nil {
		t.Errorf("Node badportnode persisted after rejection — must not")
	}
}

// TestBug369NodeCreateRefusesHugePort pins the symmetric gate for
// >65535 on POST /v1/nodes.
func TestBug369NodeCreateRefusesHugePort(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.Node{
		Name: "hugeportnode",
		Type: "SATELLITE",
		NetInterfaces: []apiv1.NetInterface{
			{Name: DefaultNetInterfaceName, Address: "10.99.99.99", SatellitePort: 99999},
		},
	})

	resp := httpPost(t, base+"/v1/nodes", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (Bug 369 satellite_port>65535 on n c). Body: %s",
			resp.StatusCode, got)
	}

	got, _ := readAllBody(resp)
	if !strings.Contains(string(got), "satellite_port 99999 out of range") {
		t.Errorf("envelope missing offending port: %s", got)
	}

	if _, err := st.Nodes().Get(t.Context(), "hugeportnode"); err == nil {
		t.Errorf("Node hugeportnode persisted after rejection — must not")
	}
}

// TestBug368369NodeCreatePortZeroStillAccepted pins the explicit
// upstream-LINSTOR semantic: satellite_port=0 means "inherit the
// default", and the validator must NOT reject it.
func TestBug368369NodeCreatePortZeroStillAccepted(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.Node{
		Name: "zeroportnode",
		Type: "SATELLITE",
		NetInterfaces: []apiv1.NetInterface{
			{Name: DefaultNetInterfaceName, Address: "10.99.99.99", SatellitePort: 0},
		},
	})

	resp := httpPost(t, base+"/v1/nodes", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201 (port=0 must be accepted). Body: %s",
			resp.StatusCode, got)
	}
}
