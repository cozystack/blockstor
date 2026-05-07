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

// Run is the agent's main loop. It performs the hello handshake, then
// keeps two long-running loops (apply and observe) supervised. When ctx
// is cancelled the loops drain and Run returns ctx.Err().
//
// The body intentionally stays simple in Phase 3.0: connect → hello →
// idle. Reconcile loops attach in subsequent slices behind the same
// supervisor.
func (a *Agent) Run(ctx context.Context) error {
	a.logger.Info("agent starting",
		"node", a.cfg.NodeName,
		"blockstor_version", version.Version,
		"controller", a.cfg.ControllerAddr)

	err := a.hello(ctx)
	if err != nil {
		return errors.Wrap(err, "hello")
	}

	<-ctx.Done()

	a.logger.Info("agent stopping", "node", a.cfg.NodeName)

	return ctx.Err() //nolint:wrapcheck // bubbling ctx.Err() unwrapped is the convention
}

// hello is the registration handshake. It is split out so tests can drive
// it with a fake controller without spinning up the whole agent loop.
//
// Phase 3.0 stub: log what we *would* send and respect ctx cancellation.
// The actual gRPC client lands once we run protoc and import the generated
// bindings; the signature already returns error so the caller does not need
// to change.
func (a *Agent) hello(ctx context.Context) error {
	a.logger.Info("hello (stub)",
		"node", a.cfg.NodeName,
		"layer_kinds", []string{"DRBD", "STORAGE", "LUKS"},
		"provider_kinds", []string{"LVM", "LVM_THIN", "ZFS", "ZFS_THIN", "FILE"})

	err := ctx.Err()
	if err != nil {
		return errors.Wrap(err, "context cancelled before hello")
	}

	return nil
}
