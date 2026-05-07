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

package satellitecontroller

import (
	"context"
	"net"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
)

// Runnable adapts the satellite-facing gRPC Server into a
// controller-runtime manager.Runnable so it shuts down with the manager
// (same lifecycle as pkg/rest.Server).
type Runnable struct {
	// Addr is the gRPC bind address (e.g. ":7000").
	Addr string

	// Server is the SatelliteServer this Runnable serves.
	Server *Server
}

// NeedLeaderElection — the satellite gRPC endpoint must be reachable on
// every replica so satellites can connect to whichever pod they hit; we
// don't gate it on leader election.
func (r *Runnable) NeedLeaderElection() bool { return false }

// Start implements manager.Runnable.
func (r *Runnable) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("satellite-grpc")

	lc := &net.ListenConfig{}

	ln, err := lc.Listen(ctx, "tcp", r.Addr)
	if err != nil {
		return errors.Wrapf(err, "listen %q", r.Addr)
	}

	gs := grpc.NewServer()
	satellitepb.RegisterSatelliteServer(gs, r.Server)

	logger.Info("satellite gRPC listening", "addr", ln.Addr().String())

	errCh := make(chan error, 1)

	go func() { errCh <- gs.Serve(ln) }()

	select {
	case <-ctx.Done():
		stopped := make(chan struct{})

		go func() {
			gs.GracefulStop()
			close(stopped)
		}()

		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			gs.Stop()
		}

		return nil
	case err := <-errCh:
		if err != nil {
			return errors.Wrap(err, "gRPC server")
		}

		return nil
	}
}

// Compile-time check.
var _ manager.Runnable = (*Runnable)(nil)
