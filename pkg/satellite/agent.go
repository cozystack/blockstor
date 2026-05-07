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
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
	"github.com/cozystack/blockstor/pkg/version"
)

// Config holds the parameters that come in from the satellite binary's
// command-line flags or its container env.
type Config struct {
	// NodeName is the name this satellite registers under. Required.
	NodeName string

	// ControllerAddr is the gRPC dial address of the blockstor-controller.
	ControllerAddr string

	// StateDir is the on-disk directory the satellite uses for DRBD .res
	// files and per-resource state. Required.
	StateDir string

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
func NewAgent(cfg Config) *Agent {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Agent{cfg: cfg, logger: logger}
}

// Run is the agent's main loop. It dials the controller, performs the
// hello handshake to register the node, then keeps long-running apply
// and observe loops supervised. When ctx is cancelled the loops drain
// and Run returns ctx.Err().
//
// Phase 3.1: hello is the only real RPC; reconcile loops still no-op.
func (a *Agent) Run(ctx context.Context) error {
	if a.cfg.NodeName == "" {
		return errors.New("NodeName is required")
	}

	a.logger.Info("agent starting",
		"node", a.cfg.NodeName,
		"blockstor_version", version.Version,
		"controller", a.cfg.ControllerAddr)

	conn, err := a.dial(ctx)
	if err != nil {
		return errors.Wrap(err, "dial controller")
	}
	defer func() { _ = conn.Close() }()

	client := satellitepb.NewSatelliteClient(conn)

	err = a.hello(ctx, client)
	if err != nil {
		return errors.Wrap(err, "hello")
	}

	<-ctx.Done()

	a.logger.Info("agent stopping", "node", a.cfg.NodeName)

	return ctx.Err() //nolint:wrapcheck // bubbling ctx.Err() unwrapped is the convention
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

// hello is the registration handshake. The satellite tells the controller
// who it is and what layers / providers it can drive; the controller
// upserts the corresponding Node CRD and replies with the cluster id.
func (a *Agent) hello(ctx context.Context, client satellitepb.SatelliteClient) error {
	rpcCtx, cancel := context.WithTimeout(ctx, a.cfg.DialTimeout)
	defer cancel()

	resp, err := client.Hello(rpcCtx, &satellitepb.HelloRequest{
		NodeName:         a.cfg.NodeName,
		BlockstorVersion: version.Version,
		LayerKinds:       []string{"DRBD", "STORAGE", "LUKS"},
		ProviderKinds:    []string{"LVM", "LVM_THIN", "ZFS", "ZFS_THIN", "FILE"},
	})
	if err != nil {
		return errors.Wrap(err, "Hello RPC")
	}

	a.logger.Info("hello complete",
		"node", a.cfg.NodeName,
		"cluster_id", resp.GetClusterId())

	return nil
}
