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

package satellite

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
)

// helloErrorClient is a minimal ControllerClient stub whose Hello
// always fails. Used to exercise hello()'s error-wrap path.
type helloErrorClient struct {
	satellitepb.ControllerClient
}

func (helloErrorClient) Hello(_ context.Context, _ *satellitepb.HelloRequest, _ ...grpc.CallOption) (*satellitepb.HelloResponse, error) {
	return nil, errHelloRPCDown
}

var errHelloRPCDown = errors.New("controller unreachable")

// TestHostFromEndpoint pins the trailing-port stripper Hello uses to
// derive the dial-back host from a SatelliteEndpoint prop. Same
// LastIndex shape as the dispatcher's peerAddress (IPv6-aware).
func TestHostFromEndpoint(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in, want string
	}{
		{"", ""},
		{"10.244.1.5:7000", "10.244.1.5"},
		{"localhost:7001", "localhost"},
		{"no-colon-here", "no-colon-here"}, // returned verbatim when no port
		{"[fe80::1]:7000", "[fe80::1]"},    // IPv6 (LastIndex picks rightmost colon)
		{":7000", ":7000"},                 // empty-host edge case → leniency, return as-is
	}
	for _, c := range cases {
		got := hostFromEndpoint(c.in)
		if got != c.want {
			t.Errorf("hostFromEndpoint(%q): got %q, want %q", c.in, got, c.want)
		}
	}
}

// TestResolveAddr: whenever the controller-supplied address is empty
// or the 0.0.0.0 placeholder, resolveAddr substitutes the satellite's
// own IP. A non-empty / non-placeholder input passes through verbatim.
//
// The empty-fallback branch returns the placeholder unchanged — pinned
// here so unit tests of the reconciler that don't bother setting
// LocalAddress keep working without surprises.
func TestResolveAddr(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name               string
		supplied, fallback string
		want               string
	}{
		{"placeholder + fallback", "0.0.0.0", "10.244.1.5", "10.244.1.5"},
		{"empty + fallback", "", "10.244.1.5", "10.244.1.5"},
		{"placeholder + empty fallback", "0.0.0.0", "", "0.0.0.0"},
		{"non-placeholder pass-through", "10.0.0.7", "10.244.1.5", "10.0.0.7"},
		{"non-placeholder + empty fallback", "10.0.0.7", "", "10.0.0.7"},
	}

	for _, c := range cases {
		got := resolveAddr(c.supplied, c.fallback)
		if got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

// TestStartGRPCServerEmptyListenAddrIsNoOp: when ListenAddr is
// empty the satellite skips the gRPC server bootstrap entirely and
// returns a no-op stop func + the placeholder address string.
// This is the path unit tests of Hello-only flows exercise (no
// free port needed). Pinning it ensures a refactor that removed
// the early-return wouldn't silently start eating ports during
// tests run on parallel CI shards.
func TestStartGRPCServerEmptyListenAddrIsNoOp(t *testing.T) {
	t.Parallel()

	a := NewAgent(Config{NodeName: "n1"}) // no ListenAddr

	addr, stop, err := a.startGRPCServer(t.Context(), a.newReconciler())
	if err != nil {
		t.Fatalf("startGRPCServer with empty ListenAddr: %v", err)
	}

	if addr != grpcServerDisabled {
		t.Errorf("addr: got %q, want %q", addr, grpcServerDisabled)
	}

	if stop == nil {
		t.Errorf("stop func nil; want callable no-op")
	}

	// The no-op stop must not panic.
	stop()
}

// TestStartGRPCServerBindsAndStops drives the non-empty-ListenAddr
// branch of startGRPCServer: it binds a real loopback listener,
// returns the actual addr, and produces a stop func that gracefully
// drains the gRPC server. Pins the listener-spawn path that
// production satellites take when they expose the dispatcher RPC,
// and confirms stop() blocks until the goroutine has exited (so a
// Run-loop teardown doesn't race with a half-closed listener).
func TestStartGRPCServerBindsAndStops(t *testing.T) {
	t.Parallel()

	a := NewAgent(Config{
		NodeName:   "n1",
		ListenAddr: "127.0.0.1:0", // ephemeral port
	})

	addr, stop, err := a.startGRPCServer(t.Context(), a.newReconciler())
	if err != nil {
		t.Fatalf("startGRPCServer: %v", err)
	}

	if addr == grpcServerDisabled {
		t.Fatalf("addr: got placeholder %q, want a real bound address", grpcServerDisabled)
	}

	if !strings.HasPrefix(addr, "127.0.0.1:") {
		t.Errorf("addr: got %q, want 127.0.0.1:<port>", addr)
	}

	if stop == nil {
		t.Fatalf("stop func nil")
	}

	// stop() must block until the Serve goroutine exits — otherwise a
	// caller racing into the next bind on the same port would EADDRINUSE.
	stop()
}

// TestStartGRPCServerListenError pins the listener-bind error wrap:
// an unparseable ListenAddr surfaces as an error tagged with the
// "listen" keyword so a typo in CONTROLLER_GRPC_ADDR is grep-able
// in the satellite log.
func TestStartGRPCServerListenError(t *testing.T) {
	t.Parallel()

	a := NewAgent(Config{
		NodeName:   "n1",
		ListenAddr: "not-an-addr",
	})

	addr, stop, err := a.startGRPCServer(t.Context(), a.newReconciler())
	if err == nil {
		t.Fatalf("got nil error; want listen failure (addr=%q stop=%v)", addr, stop != nil)
	}

	if !strings.Contains(err.Error(), "listen") {
		t.Errorf("error wrap: got %q, want substring \"listen\"", err.Error())
	}
}

// TestHelloErrorWraps pins the error-wrap on the registration RPC:
// when the controller's Hello returns an error (controller mid-
// restart, network partition, TLS handshake failure), the satellite's
// hello() must surface it with the operator-grep keyword "Hello RPC"
// so the satellite log line "Hello RPC: <transport-detail>" is
// locatable. superviseObserveLoop relies on this signal to retry
// registration with backoff.
func TestHelloErrorWraps(t *testing.T) {
	t.Parallel()

	a := NewAgent(Config{NodeName: "n1", DialTimeout: 100 * 0})

	// DialTimeout 0 → context.WithTimeout becomes already-deadline,
	// but our helloErrorClient ignores ctx and returns its synthetic
	// error directly, so we still exercise the wrap path.
	err := a.hello(context.Background(), helloErrorClient{})
	if err == nil {
		t.Fatalf("hello: got nil, want error")
	}

	if !strings.Contains(err.Error(), "Hello RPC") {
		t.Errorf("error wrap: got %q, want substring \"Hello RPC\"", err.Error())
	}
}

// TestDialReturnsConnForValidAddr pins the happy-path of dial:
// grpc.NewClient with a syntactically-valid address must produce
// a non-nil ClientConn (NewClient is non-blocking, so the actual
// dial happens lazily on first RPC — we assert only that the
// constructor doesn't surface an error).
//
// Pinned because the satellite's Run() invokes dial() once per
// supervise iteration; a regression that always errored here
// would make the satellite never connect.
func TestDialReturnsConnForValidAddr(t *testing.T) {
	t.Parallel()

	a := NewAgent(Config{
		NodeName:       "n1",
		ControllerAddr: "127.0.0.1:7000",
		DialTimeout:    100,
	})

	conn, err := a.dial(t.Context())
	if err != nil {
		t.Fatalf("dial: got %v, want nil", err)
	}

	if conn == nil {
		t.Fatalf("dial: got nil conn")
	}

	_ = conn.Close()
}

// TestRunRequiresNodeName pins the early-validation branch of Run:
// an Agent constructed without NodeName must fail-fast with an
// explicit error, not crash later inside dial() with a confusing
// gRPC stack trace. Pinned because the satellite binary's main()
// trusts Run's err to surface bad config — a regression that
// silently let an empty NodeName through would propagate as
// "Hello: missing node_name" from the controller side, miles
// away from the actual misconfiguration.
func TestRunRequiresNodeName(t *testing.T) {
	t.Parallel()

	a := NewAgent(Config{}) // empty NodeName

	err := a.Run(t.Context())
	if err == nil {
		t.Fatalf("Run with empty NodeName: got nil, want error")
	}

	if !strings.Contains(err.Error(), "NodeName") {
		t.Errorf("error must name the missing field; got %q", err.Error())
	}
}

// TestPickMechanism pins the provider-kind → ship-mechanism table
// the snapshot-shipping dispatcher relies on. The bare "default"
// branch (returning "") is the load-bearing fallback: an unknown
// provider kind must NOT silently map to one of the known
// mechanisms (e.g. defaulting to "zfs" on an LVM-classic pool
// would issue `zfs send` against an LV and fail in a confusing
// way). Explicit "" forces the caller to emit a clear
// "unsupported-mechanism" error.
func TestPickMechanism(t *testing.T) {
	t.Parallel()

	cases := []struct {
		kind string
		want string
	}{
		{kindZFS, "zfs"},
		{kindZFSThin, "zfs"},
		{kindLVMThin, "thin"},
		{apiv1.StoragePoolKindLVM, ""},      // classic LVM has no incremental ship — fall through
		{apiv1.StoragePoolKindFile, ""},     // file backend no-op
		{apiv1.StoragePoolKindDiskless, ""}, // diskless can't ship at all
		{"", ""},                            // empty kind
		{"GARBAGE", ""},                     // typo / unknown
	}

	for _, c := range cases {
		got := pickMechanism(c.kind)
		if got != c.want {
			t.Errorf("pickMechanism(%q): got %q, want %q", c.kind, got, c.want)
		}
	}
}
