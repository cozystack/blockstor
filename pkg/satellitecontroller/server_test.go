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

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
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

// TestHelloUpsertsPools: a Hello that carries a SatellitePool list
// must land each pool in the StoragePool store keyed by (node, pool).
// Pins the upsertPool path that previously had 0% in-package coverage.
func TestHelloUpsertsPools(t *testing.T) {
	st := store.NewInMemory()
	addr, stop := startGRPC(t, st)
	defer stop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	c := satellitepb.NewControllerClient(conn)

	_, err = c.Hello(t.Context(), &satellitepb.HelloRequest{
		NodeName: "n1",
		Pools: []*satellitepb.SatellitePool{
			{Name: "thinpool", ProviderKind: "LVM_THIN"},
			{Name: "zfs1", ProviderKind: "ZFS_THIN"},
		},
	})
	if err != nil {
		t.Fatalf("Hello: %v", err)
	}

	pools, err := st.StoragePools().ListByNode(t.Context(), "n1")
	if err != nil {
		t.Fatalf("ListByNode: %v", err)
	}

	if len(pools) != 2 {
		t.Errorf("pools: got %d, want 2 (got %+v)", len(pools), pools)
	}

	// Second Hello must Update existing entries, not duplicate.
	_, err = c.Hello(t.Context(), &satellitepb.HelloRequest{
		NodeName: "n1",
		Pools: []*satellitepb.SatellitePool{
			{Name: "thinpool", ProviderKind: "LVM_THIN"},
			{Name: "zfs1", ProviderKind: "ZFS_THIN"},
		},
	})
	if err != nil {
		t.Fatalf("Hello (second): %v", err)
	}

	pools, _ = st.StoragePools().ListByNode(t.Context(), "n1")
	if len(pools) != 2 {
		t.Errorf("re-Hello duplicated pools: got %d, want 2", len(pools))
	}
}

// TestReportPoolCapacityUpdatesStore: the satellite's periodic
// capacity push must land each pool's free/total bytes on the
// StoragePool record so /v1/view/storage-pools surfaces live numbers.
func TestReportPoolCapacityUpdatesStore(t *testing.T) {
	st := store.NewInMemory()

	if err := st.StoragePools().Create(t.Context(), &apiv1.StoragePool{
		NodeName:        "n1",
		StoragePoolName: "thinpool",
		ProviderKind:    "LVM_THIN",
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	addr, stop := startGRPC(t, st)
	defer stop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	c := satellitepb.NewControllerClient(conn)

	resp, err := c.ReportPoolCapacity(t.Context(), &satellitepb.ReportPoolCapacityRequest{
		NodeName: "n1",
		Pools: []*satellitepb.PoolCapacity{
			{
				PoolName:          "thinpool",
				FreeCapacityKib:   500_000,
				TotalCapacityKib:  1_000_000,
				SupportsSnapshots: true,
			},
		},
	})
	if err != nil {
		t.Fatalf("ReportPoolCapacity: %v", err)
	}

	if !resp.GetOk() {
		t.Errorf("Ok: got false, want true")
	}

	got, err := st.StoragePools().Get(t.Context(), "n1", "thinpool")
	if err != nil {
		t.Fatalf("Get pool: %v", err)
	}

	if got.FreeCapacity != 500_000 {
		t.Errorf("FreeCapacity: got %d, want 500000", got.FreeCapacity)
	}

	if got.TotalCapacity != 1_000_000 {
		t.Errorf("TotalCapacity: got %d, want 1000000", got.TotalCapacity)
	}

	if !got.SupportsSnapshot {
		t.Errorf("SupportsSnapshot: got false, want true")
	}
}

// TestReportPoolCapacityRequiresNodeName: empty node_name → InvalidArgument.
func TestReportPoolCapacityRequiresNodeName(t *testing.T) {
	st := store.NewInMemory()
	addr, stop := startGRPC(t, st)
	defer stop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	c := satellitepb.NewControllerClient(conn)

	_, err = c.ReportPoolCapacity(t.Context(), &satellitepb.ReportPoolCapacityRequest{NodeName: ""})
	if err == nil {
		t.Errorf("ReportPoolCapacity empty node_name: want error, got nil")
	}
}

// TestReportObservedAppliesEvent: a streamed events2 frame describing
// "resource X on node Y is Primary, in_use=true" must land on the
// matching Resource's State (InUse=true) and DrbdState prop.
// Pins the applyObserved path the network-partition / failover items
// rely on.
func TestReportObservedAppliesEvent(t *testing.T) {
	st := store.NewInMemory()

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-obs"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-obs",
		NodeName: "n1",
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	addr, stop := startGRPC(t, st)
	defer stop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	c := satellitepb.NewControllerClient(conn)

	stream, err := c.ReportObserved(t.Context())
	if err != nil {
		t.Fatalf("ReportObserved: %v", err)
	}

	err = stream.Send(&satellitepb.ResourceObservedEvent{
		ResourceName: "pvc-obs",
		NodeName:     "n1",
		DrbdState:    "UpToDate",
		InUse:        true,
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}

	if resp.GetReceived() != 1 {
		t.Errorf("Received: got %d, want 1", resp.GetReceived())
	}

	got, err := st.Resources().Get(t.Context(), "pvc-obs", "n1")
	if err != nil {
		t.Fatalf("Get Resource: %v", err)
	}

	if !got.State.InUse {
		t.Errorf("State.InUse: got false, want true (events2 Primary should flip InUse)")
	}

	if got.Props["DrbdState"] != "UpToDate" {
		t.Errorf("Props[DrbdState]: got %q, want UpToDate", got.Props["DrbdState"])
	}
}

// TestReportObservedSwallowsNotFound: events for an unknown
// resource (controller hasn't yet caught up with the satellite)
// must not tear down the stream — applyObserved swallows
// store.ErrNotFound and the next event lands fine.
func TestReportObservedSwallowsNotFound(t *testing.T) {
	st := store.NewInMemory()

	addr, stop := startGRPC(t, st)
	defer stop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	c := satellitepb.NewControllerClient(conn)
	stream, err := c.ReportObserved(t.Context())
	if err != nil {
		t.Fatalf("ReportObserved: %v", err)
	}

	// Send an event for a resource that doesn't exist; must be
	// swallowed silently (controller may not have caught up yet).
	err = stream.Send(&satellitepb.ResourceObservedEvent{
		ResourceName: "pvc-unknown",
		NodeName:     "n1",
		DrbdState:    "Inconsistent",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}

	if resp.GetReceived() != 1 {
		t.Errorf("Received: got %d, want 1 (NotFound must not abort the stream)", resp.GetReceived())
	}
}
