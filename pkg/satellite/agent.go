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
	"fmt"
	"log/slog"

	"github.com/cockroachdb/errors"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/luks"
	"github.com/cozystack/blockstor/pkg/satellite/stream"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/version"
)

// ManagerFactory builds a controller-runtime manager wired with
// the satellite's per-CRD reconcilers. Injected by
// cmd/satellite/main.go so pkg/satellite stays free of an
// import on pkg/satellite/controllers (which itself imports
// pkg/satellite for *Reconciler — direct import would cycle).
type ManagerFactory func(restCfg *rest.Config, nodeName string, rec *Reconciler) (manager.Manager, error)

// Config holds the parameters that come in from the satellite binary's
// command-line flags or its container env. Phase 10.6 retired every
// gRPC wire to the controller — the surviving fields drive the
// local apply chain and the controller-runtime manager.
type Config struct {
	// NodeName is the name this satellite registers under. Required.
	NodeName string

	// StateDir is the on-disk directory the satellite uses for DRBD .res
	// files and per-resource state. Required.
	StateDir string

	// Providers maps storage-pool name → provider implementation. The
	// satellite reconciler uses this to resolve which backend a
	// DesiredVolume's StoragePool refers to. The c-r
	// `StoragePoolReconciler` registers new entries here as it
	// observes StoragePool CRDs; the startup map is typically empty.
	Providers map[string]storage.Provider

	// LocalAddress is the IP the satellite renders into the local
	// half of every DRBD `on <node>` block. Typically the pod IP
	// (`$POD_IP`) so peers can reach this satellite. Empty falls
	// back to the kernel's default routing decision at drbdadm
	// time, which works on the dev stand but may surprise on
	// multi-NIC hosts.
	LocalAddress string

	// Logger is the structured logger used by the agent and its sub-loops.
	// Defaults to slog.Default if nil.
	Logger *slog.Logger

	// RESTConfig + ManagerFactory drive the controller-runtime
	// manager that owns all four CRD reconcilers + the observer
	// Runnable. Both are required — the gRPC fallback path is gone.
	RESTConfig     *rest.Config
	ManagerFactory ManagerFactory
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

// Run is the agent's main loop. Builds the satellite Reconciler,
// hands it to the controller-runtime manager (which owns the
// CRD-watch reconcilers + the events2 observer Runnable), and
// blocks until ctx cancels. Phase 10.6 retired the gRPC client
// + observer streaming entirely — every interaction with the
// controller now flows through the apiserver.
func (a *Agent) Run(ctx context.Context) error {
	if a.cfg.NodeName == "" {
		return errors.New("NodeName is required")
	}

	if a.cfg.RESTConfig == nil || a.cfg.ManagerFactory == nil {
		return errors.New("RESTConfig + ManagerFactory are required (Phase 10.6 retired the gRPC path)")
	}

	a.logger.Info("agent starting",
		"node", a.cfg.NodeName,
		"blockstor_version", version.Version)

	rec := a.newReconciler()

	err := a.startControllerRuntime(ctx, rec)
	if err != nil {
		return errors.Wrap(err, "start controller-runtime manager")
	}

	a.logger.Info("satellite controller-runtime manager ready")

	a.startStreamServer(ctx, rec)

	<-ctx.Done()

	a.logger.Info("agent stopping", "node", a.cfg.NodeName)

	return ctx.Err() //nolint:wrapcheck // bubbling ctx.Err() unwrapped is the convention
}

// newReconciler builds the satellite's apply-chain Reconciler
// from the Agent's config. Single instance per Agent — both the
// controller-runtime CRD reconcilers and the events2 observer
// Runnable share it.
func (a *Agent) newReconciler() *Reconciler {
	return NewReconciler(ReconcilerConfig{
		Providers:    a.cfg.Providers,
		Adm:          drbd.NewAdm(storage.RealExec{}),
		Cryptsetup:   luks.NewCryptsetup(storage.RealExec{}),
		StateDir:     a.cfg.StateDir,
		NodeName:     a.cfg.NodeName,
		LocalAddress: a.cfg.LocalAddress,
	})
}

// poolResolverAdapter wires the satellite Reconciler's private
// resource→provider lookup into the public stream.PoolResolver
// interface. Adapter pattern (rather than exporting the method
// directly) keeps the Reconciler's unexported funcorder layout
// intact.
type poolResolverAdapter struct {
	rec *Reconciler
}

// ProviderForResource implements stream.PoolResolver.
func (a poolResolverAdapter) ProviderForResource(name string) (storage.Provider, error) {
	return a.rec.providerForResource(name)
}

// startStreamServer launches the satellite-to-satellite snapshot
// stream HTTP server in a goroutine. Bound to 0.0.0.0:stream.Port
// — the DaemonSet runs on hostNetwork so this is the node's IP.
//
// A bind failure logs but does not abort the agent — without the
// stream server, cross-node snapshot-restore on this satellite
// falls back to "peer has no snapshot here" and the receiving
// satellite tries other peers. Same-node clone keeps working.
func (a *Agent) startStreamServer(ctx context.Context, rec *Reconciler) {
	addr := fmt.Sprintf("0.0.0.0:%d", stream.Port)
	srv := stream.NewServer(poolResolverAdapter{rec: rec})

	go func() {
		err := stream.ListenAndServe(ctx, addr, srv)
		if err != nil && !errors.Is(err, context.Canceled) {
			a.logger.Error("snapshot stream server exited", "addr", addr, "err", err)
		}
	}()

	a.logger.Info("snapshot stream server listening", "addr", addr)
}

// startControllerRuntime launches a controller-runtime manager
// that wires the four per-CRD satellite reconcilers (Resource,
// StoragePool, Snapshot, ResourceDefinition) onto the shared
// apply chain `rec`. Manager runs in a goroutine and exits when
// ctx cancels; Serve errors log but do not abort the agent —
// gRPC stays primary until Phase 10.6 retires it.
//
// Returns once `mgr.Start` is in flight. Constructed via the
// `controllers.NewManager` factory so this stays a one-liner if
// the manager's scheme / leader-election story changes later.
func (a *Agent) startControllerRuntime(ctx context.Context, rec *Reconciler) error {
	mgr, err := a.cfg.ManagerFactory(a.cfg.RESTConfig, a.cfg.NodeName, rec)
	if err != nil {
		return errors.Wrap(err, "build manager")
	}

	go func() {
		err := mgr.Start(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			a.logger.Error("controller-runtime manager exited", "err", err)
		}
	}()

	return nil
}
