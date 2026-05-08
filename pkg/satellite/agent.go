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

// Package satellite is the per-node agent runtime. It encapsulates the
// gRPC client connection to the controller, the local state machine that
// owns DRBD/LVM/ZFS bookkeeping, and the observed-state stream that ships
// runtime state back to the controller.
//
// Phase 3 first slice: connect, hello, idle. Reconcile bodies (DRBD .res
// generation, drbdadm, LVM/ZFS provisioning) come in subsequent slices.
package satellite

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	"github.com/cozystack/blockstor/pkg/drbd"
	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/version"
)

// Config holds the parameters that come in from the satellite binary's
// command-line flags or its container env.
type Config struct {
	// NodeName is the name this satellite registers under. Required.
	NodeName string

	// ControllerAddr is the gRPC dial address of the blockstor-controller.
	ControllerAddr string

	// ListenAddr is the bind address for the satellite's own gRPC server
	// (the side that the controller dials for ApplyResources, snapshot
	// RPCs, ship). Empty disables the server (useful for unit tests).
	ListenAddr string

	// AdvertisedEndpoint is the host:port the satellite tells the
	// controller to dial back at. Differs from ListenAddr when the
	// satellite binds to 0.0.0.0:7000 but is reachable from the
	// controller as <pod-ip>:7000.
	AdvertisedEndpoint string

	// StateDir is the on-disk directory the satellite uses for DRBD .res
	// files and per-resource state. Required.
	StateDir string

	// Providers maps storage-pool name → provider implementation. The
	// satellite reconciler uses this to resolve which backend a
	// DesiredVolume's StoragePool refers to. Seeded at startup from CLI
	// flags / env (LVM-thin, ZFS, file).
	Providers map[string]storage.Provider

	// DialTimeout caps gRPC connection establishment per attempt.
	DialTimeout time.Duration

	// Logger is the structured logger used by the agent and its sub-loops.
	// Defaults to slog.Default if nil.
	Logger *slog.Logger
}

// Agent is the satellite runtime. It is constructed with NewAgent and run
// with Run; Run returns when the parent context is cancelled.
type Agent struct {
	cfg    Config
	logger *slog.Logger
}

// NewAgent constructs an Agent without yet dialling the controller.
//
//nolint:gocritic // value receiver is the ergonomic public API; Config is the binary's flag bundle.
func NewAgent(cfg Config) *Agent {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Agent{cfg: cfg, logger: logger}
}

// Run is the agent's main loop. It dials the controller, performs the
// hello handshake to register the node, starts the satellite-side
// gRPC server (so the controller can push desired state), then waits
// for ctx to cancel.
func (a *Agent) Run(ctx context.Context) error {
	if a.cfg.NodeName == "" {
		return errors.New("NodeName is required")
	}

	a.logger.Info("agent starting",
		"node", a.cfg.NodeName,
		"blockstor_version", version.Version,
		"controller", a.cfg.ControllerAddr,
		"listen", a.cfg.ListenAddr)

	conn, err := a.dial(ctx)
	if err != nil {
		return errors.Wrap(err, "dial controller")
	}
	defer func() { _ = conn.Close() }()

	client := satellitepb.NewControllerClient(conn)

	err = a.hello(ctx, client)
	if err != nil {
		return errors.Wrap(err, "hello")
	}

	// Bring up the satellite-side gRPC server so the controller can push
	// ApplyResources / snapshot RPCs at us. The Reconciler is wired with
	// the configured providers + drbdadm wrapper + state dir.
	srv, stop, err := a.startGRPCServer(ctx)
	if err != nil {
		return errors.Wrap(err, "start gRPC server")
	}
	defer stop()

	a.logger.Info("satellite gRPC ready", "addr", srv)

	<-ctx.Done()

	a.logger.Info("agent stopping", "node", a.cfg.NodeName)

	return ctx.Err() //nolint:wrapcheck // bubbling ctx.Err() unwrapped is the convention
}

// startGRPCServer binds the satellite's `service Satellite` listener.
// Empty cfg.ListenAddr disables the server (returns a no-op stop) so
// unit tests that only exercise Hello don't need a free port.
func (a *Agent) startGRPCServer(ctx context.Context) (string, func(), error) {
	if a.cfg.ListenAddr == "" {
		return "<disabled>", func() {}, nil
	}

	rec := NewReconciler(ReconcilerConfig{
		Providers:    a.cfg.Providers,
		Adm:          drbd.NewAdm(storage.RealExec{}),
		StateDir:     a.cfg.StateDir,
		NodeName:     a.cfg.NodeName,
		LocalAddress: hostFromEndpoint(a.cfg.AdvertisedEndpoint),
	})

	listenCfg := &net.ListenConfig{}

	listener, err := listenCfg.Listen(ctx, "tcp", a.cfg.ListenAddr)
	if err != nil {
		return "", nil, errors.Wrapf(err, "listen %s", a.cfg.ListenAddr)
	}

	gs := grpc.NewServer()
	satellitepb.RegisterSatelliteServer(gs, NewGRPCServer(rec))
	reflection.Register(gs)

	done := make(chan struct{})

	go func() {
		defer close(done)

		err := gs.Serve(listener)
		if err != nil {
			a.logger.Error("gRPC Serve returned", "err", err)
		}
	}()

	stop := func() {
		gs.GracefulStop()
		<-done
	}

	return listener.Addr().String(), stop, nil
}

// dial opens an insecure gRPC connection to the controller. TLS comes in
// Phase 6 alongside the rest of the encryption work; cluster traffic is
// expected to ride a private k8s network until then.
func (a *Agent) dial(ctx context.Context) (*grpc.ClientConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, a.cfg.DialTimeout)
	defer cancel()

	conn, err := grpc.NewClient(a.cfg.ControllerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.Wrapf(err, "grpc dial %q", a.cfg.ControllerAddr)
	}

	// NewClient is non-blocking; we surface dial-time problems by issuing
	// the first RPC under DialTimeout. Tests rely on this so they fail
	// fast when the server is misconfigured.
	_ = dialCtx

	return conn, nil
}

// hostFromEndpoint trims the trailing :port off an endpoint string.
// Returns the input unchanged when it has no port (e.g. plain host or
// already host-only) or is empty. We don't use net.SplitHostPort
// because the leniency the helper needs (returning sane fallbacks)
// makes a one-line strings split simpler.
func hostFromEndpoint(endpoint string) string {
	if endpoint == "" {
		return ""
	}

	idx := strings.LastIndex(endpoint, ":")
	if idx <= 0 {
		return endpoint
	}

	return endpoint[:idx]
}

// hello is the registration handshake. The satellite tells the controller
// who it is and what layers / providers it can drive; the controller
// upserts the corresponding Node CRD and replies with the cluster id.
func (a *Agent) hello(ctx context.Context, client satellitepb.ControllerClient) error {
	rpcCtx, cancel := context.WithTimeout(ctx, a.cfg.DialTimeout)
	defer cancel()

	resp, err := client.Hello(rpcCtx, &satellitepb.HelloRequest{
		NodeName:          a.cfg.NodeName,
		BlockstorVersion:  version.Version,
		LayerKinds:        []string{"DRBD", "STORAGE", "LUKS"},
		ProviderKinds:     []string{"LVM", "LVM_THIN", "ZFS", "ZFS_THIN", "FILE"},
		SatelliteEndpoint: a.cfg.AdvertisedEndpoint,
	})
	if err != nil {
		return errors.Wrap(err, "Hello RPC")
	}

	a.logger.Info("hello complete",
		"node", a.cfg.NodeName,
		"cluster_id", resp.GetClusterId())

	return nil
}
