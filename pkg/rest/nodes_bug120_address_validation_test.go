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
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 120: `n c <name> 999.999.999.999` accepted invalid IPv4 / IPv6
// literals on the wire. Fix: parse each net-interface address with
// net.ParseIP; if that fails, fall back to DNS lookup with a 1s
// timeout. Anything that's neither a valid IP literal nor a resolvable
// hostname returns 400 + LINSTOR envelope. DNS names are still
// accepted because upstream LINSTOR accepts hostnames in this field
// (the controller DNS-resolves them at satellite-connect time) — see
// handleNodeCreate's defaultResolveHost path for the existing
// hostname-resolution seam this fix reuses.

// TestBug120IPv4InvalidRefused pins the validation gate for the
// canonical reproducer: `linstor n c bogusnode 999.999.999.999`
// (each octet > 255). Before the fix this lands at the store as a
// node create with NetInterfaces[0].Address="999.999.999.999" and
// returns 201; after the fix the REST handler refuses with 400 +
// LINSTOR envelope, no Node CRD persisted.
//
// The test wires a strict DNS stub that NXDOMAINs everything — the
// shared startServerCustom helper otherwise resolves every hostname
// to 127.0.0.1 (a CI-friendly default for the no-NetInterfaces
// fallback path), which would mask the validation by making any
// garbage "resolve". Production satellites that try to register with
// a bogus literal would hit the real resolver's NXDOMAIN, so we
// mirror that here.
func TestBug120IPv4InvalidRefused(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	srv := strictDNSServer(t, st)

	base, stop := startServerCustom(t, srv)
	defer stop()

	body, _ := json.Marshal(apiv1.Node{
		Name: "bogusnode120",
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "999.999.999.999"},
		},
	})

	resp := httpPost(t, base+"/v1/nodes", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (Bug 120 invalid IPv4). Body: %s",
			resp.StatusCode, got)
	}

	got, _ := readAllBody(resp)
	if !strings.Contains(string(got), "999.999.999.999") {
		t.Errorf("envelope must echo the offending literal so the operator can see what was rejected: %s", got)
	}

	var rcs []apiv1.APICallRc
	if err := json.Unmarshal(got, &rcs); err != nil {
		t.Fatalf("body is not a []ApiCallRc envelope: %v\n%s", err, got)
	}

	if len(rcs) == 0 || rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("envelope ret_code does not carry MASK_ERROR: %+v", rcs)
	}

	// Node MUST NOT be persisted on rejection.
	nodes, err := st.Nodes().List(t.Context())
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}

	if len(nodes) != 0 {
		t.Errorf("Node persisted despite 400: %+v", nodes)
	}
}

// TestBug120IPv6InvalidRefused: the same gate must fire on a bogus
// IPv6 literal. Upstream LINSTOR's NetInterface.address field accepts
// either family; a sibling repro pins both branches.
func TestBug120IPv6InvalidRefused(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	srv := strictDNSServer(t, st)

	base, stop := startServerCustom(t, srv)
	defer stop()

	body, _ := json.Marshal(apiv1.Node{
		Name: "bogusnode120ipv6",
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "fe80:::xyz::1"},
		},
	})

	resp := httpPost(t, base+"/v1/nodes", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (Bug 120 invalid IPv6). Body: %s",
			resp.StatusCode, got)
	}

	nodes, err := st.Nodes().List(t.Context())
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}

	if len(nodes) != 0 {
		t.Errorf("Node persisted despite 400: %+v", nodes)
	}
}

// TestBug120IPv4ValidAccepted is the happy-path counterpart: a
// well-formed IPv4 literal must still create the Node. Guards against
// the gate being over-strict on the canonical CLI shape used by every
// satellite registration.
func TestBug120IPv4ValidAccepted(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.Node{
		Name: "validipv4",
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.1"},
		},
	})

	resp := httpPost(t, base+"/v1/nodes", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201. Body: %s", resp.StatusCode, got)
	}

	if _, err := st.Nodes().Get(t.Context(), "validipv4"); err != nil {
		t.Errorf("Node not persisted: %v", err)
	}
}

// TestBug120IPv6ValidAccepted is the IPv6 happy-path: link-local
// addresses are still legal NetInterface.address values per LINSTOR.
func TestBug120IPv6ValidAccepted(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.Node{
		Name: "validipv6",
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "fe80::1"},
		},
	})

	resp := httpPost(t, base+"/v1/nodes", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201. Body: %s", resp.StatusCode, got)
	}

	if _, err := st.Nodes().Get(t.Context(), "validipv6"); err != nil {
		t.Errorf("Node not persisted: %v", err)
	}
}

// TestBug120DNSNameAccepted: upstream LINSTOR's NetInterface.address
// accepts DNS names — the controller DNS-resolves them at satellite-
// connect time. Operators routinely register satellites by hostname
// (`linstor n c worker-1 worker-1.cluster.local`); a too-strict
// IP-only gate would break every existing piraeus-operator deployment
// that uses K8s DNS for node discovery. The fix layers DNS lookup
// behind the ParseIP failure path with a 1s timeout — successful
// resolution is acceptance.
func TestBug120DNSNameAccepted(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	srv := &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    newFakeRESTClient(t),
		Namespace: testRESTNamespace,
	}

	// Stub DNS lookup so the test is deterministic and offline-safe:
	// `worker-1.local` resolves to a fixed IP.
	srv.SetResolveHost(func(_ context.Context, host string) ([]string, error) {
		if host == "worker-1.local" {
			return []string{"10.10.10.10"}, nil
		}

		return nil, &netLookupError{host: host}
	})

	base, stop := startServerCustom(t, srv)
	defer stop()

	body, _ := json.Marshal(apiv1.Node{
		Name: "workerone",
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "worker-1.local"},
		},
	})

	resp := httpPost(t, base+"/v1/nodes", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201 (DNS-resolvable hostname must be accepted). Body: %s",
			resp.StatusCode, got)
	}

	if _, err := st.Nodes().Get(t.Context(), "workerone"); err != nil {
		t.Errorf("Node not persisted: %v", err)
	}
}

// netLookupError is a deterministic stand-in for `*net.DNSError` —
// the test stub returns this when the synthetic resolver doesn't
// know the requested host, so the handler hits the same
// resolution-failed branch a real DNS NXDOMAIN would.
type netLookupError struct {
	host string
}

func (e *netLookupError) Error() string {
	return "lookup " + e.host + ": no such host"
}

// strictDNSServer builds a Server whose DNS-lookup seam NXDOMAINs
// every query. The shared startServerCustom default resolves every
// hostname to 127.0.0.1 (CI-friendly fallback for the no-
// NetInterfaces / synthesise-from-name path); for Bug-120 the test
// wants the DNS-fallback branch in validateNetInterfaceAddresses to
// FAIL so the IP-validation gate is the one in play.
func strictDNSServer(t *testing.T, st store.Store) *Server {
	t.Helper()

	srv := &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    newFakeRESTClient(t),
		Namespace: testRESTNamespace,
	}

	srv.SetResolveHost(func(_ context.Context, host string) ([]string, error) {
		return nil, &netLookupError{host: host}
	})

	return srv
}
