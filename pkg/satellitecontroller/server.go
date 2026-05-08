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

// Package satellitecontroller is the controller-side of the
// satellite/controller gRPC contract. Satellites dial in here on startup,
// stream observed state back, and execute the apply RPCs the controller
// sends them.
package satellitecontroller

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
	"github.com/cozystack/blockstor/pkg/store"
)

// Config carries the bits of cluster identity the controller hands back to
// satellites during the Hello handshake.
type Config struct {
	// ClusterID is a stable identifier for this controller's cluster.
	// Satellites refuse to talk to a controller whose ClusterID changes
	// across reconnects (would mean someone re-bootstrapped the cluster
	// under us).
	ClusterID string

	// ControllerEndpoint is the canonical address of the controller's
	// REST API; satellites surface it in their own diagnostics. Optional.
	ControllerEndpoint string
}

// Server implements satellitepb.ControllerServer on top of the blockstor
// state store. It is wired in cmd/main.go via google.golang.org/grpc.
type Server struct {
	satellitepb.UnimplementedControllerServer

	st  store.Store
	cfg Config
}

// New constructs a Server. The store is the controller's source of truth;
// every RPC mutates it (or just reads).
func New(st store.Store, cfg Config) *Server {
	return &Server{st: st, cfg: cfg}
}

// Hello is the satellite registration handshake. It registers the
// satellite as a Node CRD if missing, idempotently updates its type +
// version-tracking props if present, and returns the cluster identity.
func (s *Server) Hello(ctx context.Context, req *satellitepb.HelloRequest) (*satellitepb.HelloResponse, error) {
	if req.GetNodeName() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_name is required")
	}

	props := map[string]string{
		"BlockstorVersion":  req.GetBlockstorVersion(),
		"DrbdVersion":       req.GetDrbdVersion(),
		"SatelliteEndpoint": req.GetSatelliteEndpoint(),
	}

	// Hello completes only after the satellite has dialled in, so by
	// the time we're here the node is ONLINE in LINSTOR-speak. The
	// piraeus init-container `linstor-wait-node-online` checks this
	// exact field; without it linstor-csi-node DaemonSets stall on
	// satellites we know perfectly well are alive.
	node := apiv1.Node{
		Name:             req.GetNodeName(),
		Type:             apiv1.NodeTypeSatellite,
		Props:            props,
		ConnectionStatus: "ONLINE",
	}

	err := s.st.Nodes().Create(ctx, &node)
	switch {
	case err == nil:
		// fresh registration
	case errors.Is(err, store.ErrAlreadyExists):
		err = s.st.Nodes().Update(ctx, &node)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "update Node %q: %v", req.GetNodeName(), err)
		}
	default:
		return nil, status.Errorf(codes.Internal, "register Node %q: %v", req.GetNodeName(), err)
	}

	return &satellitepb.HelloResponse{
		ClusterId:          s.cfg.ClusterID,
		ControllerEndpoint: s.cfg.ControllerEndpoint,
	}, nil
}

// ReportObserved is the satellite→controller observed-state stream.
// Each frame describes one parsed `drbdsetup events2` line; we land
// it on the matching Resource CRD's Status.
//
// The handler intentionally swallows non-fatal per-event errors (the
// stream is best-effort; satellites reconnect on RPC errors); only
// transport faults bubble.
func (s *Server) ReportObserved(stream satellitepb.Controller_ReportObservedServer) error {
	count := int64(0)

	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&satellitepb.ReportObservedResponse{Received: count})
		}

		if err != nil {
			return status.Errorf(codes.Internal, "recv observed: %v", err)
		}

		applyErr := s.applyObserved(stream.Context(), ev)
		if applyErr != nil {
			// Log-and-skip — better than tearing the stream down on
			// a single mis-formed event.
			_ = applyErr
		}

		count++
	}
}

// applyObserved lands one parsed events2 frame on the matching
// Resource. We store the DRBD state as a `DrbdState` prop and the
// "in use" hint via Resource.State.InUse so existing REST callers
// (linstor-csi, kubectl-linstor) see the live runtime info without
// the schema needing to change. Granular per-volume disk state lands
// once the CRD's volume-level status fields settle.
func (s *Server) applyObserved(ctx context.Context, ev *satellitepb.ResourceObservedEvent) error {
	if ev.GetResourceName() == "" || ev.GetNodeName() == "" {
		return nil
	}

	res, err := s.st.Resources().Get(ctx, ev.GetResourceName(), ev.GetNodeName())
	if err != nil {
		// NotFound is normal during convergence — the satellite may
		// observe state for a resource the controller hasn't yet
		// created. Bubble nothing.
		return nil //nolint:nilerr // best-effort: missing resource isn't a stream-fatal event
	}

	if res.Props == nil {
		res.Props = map[string]string{}
	}

	if disk := ev.GetDrbdState(); disk != "" {
		res.Props["DrbdState"] = disk
	}

	res.State.InUse = ev.GetInUse()

	err = s.st.Resources().Update(ctx, &res)
	if err != nil {
		return errors.Wrap(err, "update resource state")
	}

	return nil
}
