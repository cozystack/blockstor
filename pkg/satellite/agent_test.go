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

package satellite_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/cozystack/blockstor/pkg/satellite"
	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
	"github.com/cozystack/blockstor/pkg/satellitecontroller"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestAgentHelloEndToEnd: spin up the satellitecontroller gRPC server,
// run the satellite Agent against it, verify the Node appears in the
// store. Pins the satellite's first job — actually register itself.
func TestAgentHelloEndToEnd(t *testing.T) {
	st := store.NewInMemory()

	addr, stop := startServer(t, st, "test-cluster")
	defer stop()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	agent := satellite.NewAgent(satellite.Config{
		NodeName:       "n1",
		ControllerAddr: addr,
		DialTimeout:    2 * time.Second,
	})

	// Run blocks until ctx is cancelled. We don't care about the loop body
	// for this test, only that hello round-trips.
	errCh := make(chan error, 1)
	go func() { errCh <- agent.Run(ctx) }()

	// Poll until the Node appears or the test times out. We don't reach
	// inside Agent for synchronisation; the contract is "after Run starts,
	// the satellite is registered eventually".
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, err := st.Nodes().Get(t.Context(), "n1")
		if err == nil {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("Node never registered: %v", err)
		}

		time.Sleep(50 * time.Millisecond)
	}

	cancel()

	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Errorf("Run did not exit within 2s after cancel")
	}
}

// TestAgentRefusesEmptyNodeName: misconfigured Agent must fail-fast at
// hello rather than register an empty-name Node and confuse the cluster.
func TestAgentRefusesEmptyNodeName(t *testing.T) {
	st := store.NewInMemory()

	addr, stop := startServer(t, st, "test-cluster")
	defer stop()

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	agent := satellite.NewAgent(satellite.Config{
		ControllerAddr: addr,
		DialTimeout:    1 * time.Second,
	})

	err := agent.Run(ctx)
	if err == nil {
		t.Errorf("Run with empty NodeName: want error, got nil")
	}

	nodes, _ := st.Nodes().List(t.Context())
	if len(nodes) != 0 {
		t.Errorf("Nodes registered despite empty name: %v", nodes)
	}
}

func startServer(t *testing.T, st store.Store, clusterID string) (string, func()) {
	t.Helper()

	lc := &net.ListenConfig{}

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := satellitecontroller.New(st, satellitecontroller.Config{ClusterID: clusterID})
	gs := grpc.NewServer()
	satellitepb.RegisterSatelliteServer(gs, srv)

	errCh := make(chan error, 1)
	go func() { errCh <- gs.Serve(ln) }()

	stop := func() {
		gs.GracefulStop()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Errorf("server did not stop within 2s")
		}
	}

	return ln.Addr().String(), stop
}

// silence unused warnings.
var (
	_ = grpc.NewClient
	_ = insecure.NewCredentials
)
