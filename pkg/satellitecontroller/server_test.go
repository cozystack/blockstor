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

package satellitecontroller_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
	"github.com/cozystack/blockstor/pkg/satellitecontroller"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestHelloRegistersNode pins the controller-side contract for the Hello
// RPC: a satellite that has not been registered yet appears as a Node CRD
// after the round-trip.
func TestHelloRegistersNode(t *testing.T) {
	st := store.NewInMemory()

	addr, stop := startGRPC(t, st)
	defer stop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	c := satellitepb.NewControllerClient(conn)

	resp, err := c.Hello(t.Context(), &satellitepb.HelloRequest{
		NodeName:         "n1",
		BlockstorVersion: "0.0.0-test",
		LayerKinds:       []string{"DRBD", "STORAGE"},
		ProviderKinds:    []string{"LVM_THIN", "ZFS"},
		DrbdVersion:      "9.2.14",
	})
	if err != nil {
		t.Fatalf("Hello: %v", err)
	}

	if resp.GetClusterId() == "" {
		t.Errorf("ClusterId: empty, want non-empty")
	}

	got, err := st.Nodes().Get(t.Context(), "n1")
	if err != nil {
		t.Fatalf("Node not registered: %v", err)
	}

	if got.Type != "SATELLITE" {
		t.Errorf("Type: got %q, want SATELLITE", got.Type)
	}
}

// TestHelloIdempotent: the same Hello twice is fine — second call updates
// the existing Node, doesn't fail.
func TestHelloIdempotent(t *testing.T) {
	st := store.NewInMemory()

	addr, stop := startGRPC(t, st)
	defer stop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	c := satellitepb.NewControllerClient(conn)

	for i := range 2 {
		_, hErr := c.Hello(t.Context(), &satellitepb.HelloRequest{
			NodeName:         "n1",
			BlockstorVersion: "0.0.0-test",
		})
		if hErr != nil {
			t.Fatalf("Hello #%d: %v", i, hErr)
		}
	}

	nodes, err := st.Nodes().List(t.Context())
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(nodes) != 1 {
		t.Errorf("len: got %d, want 1 (Hello must not double-register)", len(nodes))
	}
}

// TestHelloRequiresNodeName: empty node_name → InvalidArgument.
func TestHelloRequiresNodeName(t *testing.T) {
	st := store.NewInMemory()

	addr, stop := startGRPC(t, st)
	defer stop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	c := satellitepb.NewControllerClient(conn)

	_, err = c.Hello(t.Context(), &satellitepb.HelloRequest{NodeName: ""})
	if err == nil {
		t.Errorf("Hello empty node_name: want error, got nil")
	}
}

// startGRPC spins up the satellite-facing gRPC server on a free loopback
// port; returns the dial address and a teardown func.
func startGRPC(t *testing.T, st store.Store) (string, func()) {
	t.Helper()

	lc := &net.ListenConfig{}

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := satellitecontroller.New(st, satellitecontroller.Config{
		ClusterID: "test-cluster",
	})

	gs := grpc.NewServer()
	satellitepb.RegisterControllerServer(gs, srv)

	errCh := make(chan error, 1)
	go func() { errCh <- gs.Serve(ln) }()

	stop := func() {
		gs.GracefulStop()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Errorf("gRPC server did not stop within 2s")
		}
	}

	return ln.Addr().String(), stop
}

// Compile-time check: Server implements the generated SatelliteServer.
var _ satellitepb.ControllerServer = (*satellitecontroller.Server)(nil)

// silence unused warnings if context isn't referenced in this file.
var _ = context.Background
