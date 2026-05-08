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

	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
)

// GRPCServer is the satellite-side implementation of `service Satellite`.
// It glues controller→satellite RPCs (ApplyResources, ApplyStoragePools,
// CreateSnapshot, DeleteSnapshot, ShipSnapshot) onto an in-process
// Reconciler. Agent.Run wires this into a gRPC listener so the
// controller can push desired state.
type GRPCServer struct {
	satellitepb.UnimplementedSatelliteServer

	rec *Reconciler
}

// NewGRPCServer constructs a server backed by rec.
func NewGRPCServer(rec *Reconciler) *GRPCServer {
	return &GRPCServer{rec: rec}
}

// ApplyResources delegates to Reconciler.Apply. Per-resource failures
// are surfaced via ResourceApplyResult.Ok=false rather than as gRPC
// errors so a single bad replica doesn't sink a batched apply.
func (g *GRPCServer) ApplyResources(ctx context.Context, req *satellitepb.ApplyResourcesRequest) (*satellitepb.ApplyResourcesResponse, error) {
	results, err := g.rec.Apply(ctx, req.GetResources())
	if err != nil {
		return nil, err
	}

	return &satellitepb.ApplyResourcesResponse{Results: results}, nil
}

// ApplyStoragePools is a placeholder that just OKs every requested pool
// for now. The satellite uses a Provider registry seeded at startup
// (CLI flags / env), so the controller's pool spec is informational
// today; we'll wire dynamic pool wiring once the per-pool runtime
// state lands.
func (g *GRPCServer) ApplyStoragePools(_ context.Context, req *satellitepb.ApplyStoragePoolsRequest) (*satellitepb.ApplyStoragePoolsResponse, error) {
	pools := req.GetPools()
	results := make([]*satellitepb.StoragePoolApplyResult, 0, len(pools))

	for _, pool := range pools {
		results = append(results, &satellitepb.StoragePoolApplyResult{
			Name: pool.GetName(),
			Ok:   true,
		})
	}

	return &satellitepb.ApplyStoragePoolsResponse{Results: results}, nil
}

// CreateSnapshot routes through the Reconciler's existing snapshot
// path. Per-snapshot errors land in the response body (Ok=false), the
// gRPC error path is reserved for context cancellation / transport
// faults.
func (g *GRPCServer) CreateSnapshot(ctx context.Context, req *satellitepb.CreateSnapshotRequest) (*satellitepb.CreateSnapshotResponse, error) {
	return g.rec.CreateSnapshot(ctx, req)
}

// DeleteSnapshot mirrors CreateSnapshot.
func (g *GRPCServer) DeleteSnapshot(ctx context.Context, req *satellitepb.DeleteSnapshotRequest) (*satellitepb.DeleteSnapshotResponse, error) {
	return g.rec.DeleteSnapshot(ctx, req)
}

// ShipSnapshot routes through the Reconciler's ship dispatch.
func (g *GRPCServer) ShipSnapshot(ctx context.Context, req *satellitepb.ShipSnapshotRequest) (*satellitepb.ShipSnapshotResponse, error) {
	return g.rec.ShipSnapshot(ctx, req)
}
