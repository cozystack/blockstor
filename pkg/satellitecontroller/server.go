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

// Server implements satellitepb.SatelliteServer on top of the blockstor
// state store. It is wired in cmd/main.go via google.golang.org/grpc.
type Server struct {
	satellitepb.UnimplementedSatelliteServer

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
		"BlockstorVersion": req.GetBlockstorVersion(),
		"DrbdVersion":      req.GetDrbdVersion(),
	}

	node := apiv1.Node{
		Name:  req.GetNodeName(),
		Type:  apiv1.NodeTypeSatellite,
		Props: props,
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
