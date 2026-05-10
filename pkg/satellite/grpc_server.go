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
	"github.com/cozystack/blockstor/pkg/storage"
)

// GRPCServer is the satellite-side implementation of `service Satellite`.
// It glues controller→satellite RPCs (ApplyResources, ApplyStoragePools,
// CreateSnapshot, DeleteSnapshot, ShipSnapshot) onto an in-process
// Reconciler. Agent.Run wires this into a gRPC listener so the
// controller can push desired state.
//
// `exec` is used by `ApplyStoragePools` to construct freshly-arrived
// `storage.Provider` instances (Phase 10.5 — dynamic pool wiring).
// In tests `exec` is `storage.FakeExec`; production uses `storage.RealExec{}`.
type GRPCServer struct {
	satellitepb.UnimplementedSatelliteServer

	rec  *Reconciler
	exec storage.Exec
}

// NewGRPCServer constructs a server backed by rec. `exec` plumbs into
// dynamically-instantiated providers in `ApplyStoragePools`. nil
// disables dynamic pool registration (the request frame falls back to
// reporting Ok=false for every requested pool).
func NewGRPCServer(rec *Reconciler, exec storage.Exec) *GRPCServer {
	return &GRPCServer{rec: rec, exec: exec}
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

// ApplyStoragePools instantiates a `storage.Provider` for each
// incoming `DesiredStoragePool` and registers it on the satellite's
// reconciler. Phase 10.5: replaces the previous Ok=true stub —
// pool config now flows controller→satellite at runtime, no
// DaemonSet rollout for "add a new pool". Per-pool failures (bad
// kind, missing config key) surface via Ok=false in the response so
// a single broken pool doesn't sink the rest of the batch.
//
// DISKLESS pools deregister any existing same-named registration
// (the kind has no underlying storage but the name is still valid
// as an allocator target placeholder).
func (g *GRPCServer) ApplyStoragePools(_ context.Context, req *satellitepb.ApplyStoragePoolsRequest) (*satellitepb.ApplyStoragePoolsResponse, error) {
	pools := req.GetPools()
	results := make([]*satellitepb.StoragePoolApplyResult, 0, len(pools))

	for _, pool := range pools {
		name := pool.GetName()
		kind := pool.GetProviderKind()

		provider, err := NewProviderFromKind(kind, pool.GetProps(), g.exec)
		if err != nil {
			results = append(results, &satellitepb.StoragePoolApplyResult{
				Name:    name,
				Ok:      false,
				Message: err.Error(),
			})

			continue
		}

		g.rec.RegisterProvider(name, provider)
		results = append(results, &satellitepb.StoragePoolApplyResult{
			Name: name,
			Ok:   true,
		})
	}

	return &satellitepb.ApplyStoragePoolsResponse{Results: results}, nil
}

// DeleteResource tears down a resource (drbdadm down → DeleteVolume
// → remove .res). Per-step errors land in the response Ok=false.
func (g *GRPCServer) DeleteResource(ctx context.Context, req *satellitepb.DeleteResourceRequest) (*satellitepb.DeleteResourceResponse, error) {
	return g.rec.DeleteResource(ctx, req)
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
