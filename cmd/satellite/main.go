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

// Command satellite is the per-node agent that owns local DRBD/LVM/ZFS
// state and reconciles it against the Resource / StoragePool /
// Snapshot / PhysicalDevice CRDs via a controller-runtime manager.
// Phase 10.6 retired the gRPC wire — every interaction with the
// controller now flows through the apiserver.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/storage"
)

func main() {
	os.Exit(run())
}

// run is split out so deferred cancellation actually runs before exit; main
// only ever calls os.Exit(run()) so there are no defers in the same frame
// as the os.Exit call.
func run() int {
	var (
		nodeName  string
		stateDir  string
		probeAddr string
	)

	flag.StringVar(&nodeName, "node-name", os.Getenv("NODE_NAME"),
		"name this satellite registers under (defaults to NODE_NAME env)")
	flag.StringVar(&stateDir, "state-dir", "/var/lib/blockstor-satellite",
		"directory the satellite uses to persist DRBD .res files and per-resource state")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081",
		"The address the /healthz and /readyz probe endpoints bind to.")

	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// Bridge controller-runtime's logr into our slog so every reconcile
	// log from the c-r manager / per-CRD reconcilers shows up next to
	// the satellite's own startup events. Without this c-r silently
	// drops every log call (its `log.SetLogger(...) was never called`
	// goroutine dump prints once on startup and reconciler errors
	// disappear).
	ctrl.SetLogger(logr.FromSlogHandler(logger.Handler()))

	if nodeName == "" {
		logger.Error("node-name is required (pass --node-name or set NODE_NAME)")

		return 1
	}

	// LocalAddress = $POD_IP under the standard DaemonSet downward-API
	// injection. Empty falls back to drbdadm's default routing, which
	// is fine on a single-NIC host.
	localAddress := os.Getenv("POD_IP")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Providers map starts empty — the c-r `StoragePoolReconciler`
	// registers entries as it observes StoragePool CRDs (Phase 10.5
	// retired the bootstrap-from-flags path; Phase 10.6 retired the
	// gRPC `ApplyStoragePools` fallback).
	providers := map[string]storage.Provider{}

	// Wipe stale .res files from previous incarnations of this
	// satellite process. Each pod restart should hand drbdadm a
	// clean slate — the c-r reconciler will re-render every
	// Resource CRD on this node shortly after startup.
	cleanStateDir(stateDir, logger)

	// Best-effort: tear down every DRBD resource the kernel
	// still has loaded from the previous satellite incarnation.
	// Without this, a reconcile that re-allocates a node-id
	// (e.g. the dispatcher picked a different id after a peer
	// joined / left) hits "peer node id cannot be my own node
	// id" because the old node-id is still pinned in kernel
	// state. The c-r reconciler re-renders + `drbdadm adjust`s
	// every Resource shortly after, so a transient down is safe.
	cleanKernelState(ctx, logger)

	restCfg, err := loadRESTConfig()
	if err != nil {
		logger.Error("no Kubernetes config", "err", err)

		return 1
	}

	// readyState gates the /readyz probe: not-ready until the
	// controller-runtime manager's cache has completed its initial
	// sync, then flipped permanently. Bug 207: without this, a
	// satellite pod whose CRD view is stale would still pass
	// readiness and route traffic. The agent fires OnCacheSynced
	// from a goroutine off mgr.GetCache().WaitForCacheSync.
	ready := newReadyState()

	agent := satellite.NewAgent(satellite.Config{
		NodeName:               nodeName,
		StateDir:               stateDir,
		Providers:              providers,
		LocalAddress:           localAddress,
		Logger:                 logger,
		RESTConfig:             restCfg,
		ManagerFactory:         mgrFactory(ready, logger),
		HealthProbeBindAddress: probeAddr,
		OnCacheSynced: func() {
			logger.Info("satellite cache sync complete, marking ready")
			ready.MarkReady()
		},
	})

	logger.Info("blockstor-satellite starting",
		"node_name", nodeName,
		"state_dir", stateDir,
		"local_address", localAddress,
		"health_probe_addr", probeAddr)

	err = agent.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("satellite exited", "err", err)

		return 1
	}

	return 0
}

// loadRESTConfig returns the in-cluster Kubernetes config the
// satellite's DaemonSet pod gets from its ServiceAccount. Phase
// 10.6 made the c-r path mandatory; failing to load the config
// now aborts startup rather than silently falling back to the
// (removed) gRPC path.
func loadRESTConfig() (*rest.Config, error) {
	cfg, err := ctrl.GetConfig()
	if err != nil {
		return nil, errors.Wrap(err, "load Kubernetes config")
	}

	return cfg, nil
}

// mgrFactory returns a satellite.ManagerFactory closure that builds
// the controller-runtime manager and wires the /healthz + /readyz
// endpoints. /healthz returns 200 once the manager is alive
// (healthz.Ping); /readyz is gated by `ready` which the agent flips
// once the cache has completed its first sync (Bug 207).
//
// The factory shape lets the agent stay ignorant of the readyState
// + logger plumbing — it only sees the standard
// satellite.ManagerFactory signature.
func mgrFactory(ready *readyState, logger *slog.Logger) satellite.ManagerFactory {
	return func(restCfg *rest.Config, nodeName, probeAddr string, rec *satellite.Reconciler) (manager.Manager, error) {
		mgr, err := controllers.NewManager(restCfg, controllers.Config{
			NodeName:               nodeName,
			Apply:                  rec,
			Exec:                   storage.RealExec{},
			HealthProbeBindAddress: probeAddr,
		})
		if err != nil {
			return nil, err //nolint:wrapcheck // controllers.NewManager already wraps
		}

		err = mgr.AddHealthzCheck("healthz", healthz.Ping)
		if err != nil {
			return nil, errors.Wrap(err, "add healthz check")
		}

		err = mgr.AddReadyzCheck("readyz", ready.Check)
		if err != nil {
			return nil, errors.Wrap(err, "add readyz check")
		}

		logger.Info("health probe endpoints registered", "addr", probeAddr)

		return mgr, nil
	}
}

// cleanStateDir wipes every *.res file in dir on satellite startup.
// The c-r reconciler re-renders every Resource CRD on this node
// shortly after startup, so the contents are reproducible — we
// don't persist satellite-side state across restarts. Best-effort:
// log and continue on errors so a single missing dir doesn't stall
// the whole startup.
func cleanStateDir(dir string, logger *slog.Logger) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		// Missing dir is fine — the satellite's first Apply will
		// create it on demand.
		return
	}

	removed := 0

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".res") {
			// Leave global_common.conf and any operator-supplied
			// non-rendered files alone.
			continue
		}

		path := filepath.Join(dir, name)
		if err := os.Remove(path); err != nil {
			logger.Warn("clean state-dir entry", "path", path, "err", err)

			continue
		}

		removed++
	}

	if removed > 0 {
		logger.Info("wiped stale .res files on startup", "dir", dir, "removed", removed)
	}
}

// cleanKernelState runs `drbdadm down all` to detach every resource
// the kernel still holds from the previous satellite incarnation.
// Best-effort: failures are logged + ignored. The c-r reconciler will
// re-render and `drbdadm adjust` each Resource CRD shortly after.
//
// Why: a reconcile cycle that re-allocates a node-id (different
// dispatcher run after a peer left/joined) hits `peer node id cannot
// be my own node id` because the kernel still has the old id pinned
// for that resource. `drbdadm down` clears that.
func cleanKernelState(ctx context.Context, logger *slog.Logger) {
	cmd := exec.CommandContext(ctx, "drbdadm", "down", "all")

	out, err := cmd.CombinedOutput()
	if err != nil {
		// "no resources defined" / "no kernel module loaded" / etc.
		// are routine on the first satellite start of a fresh node,
		// so don't escalate — just trace at INFO with the output.
		logger.Info("drbdadm down all (best-effort)",
			"err", err.Error(),
			"output", strings.TrimSpace(string(out)))

		return
	}

	if trimmed := strings.TrimSpace(string(out)); trimmed != "" {
		logger.Info("drbdadm down all", "output", trimmed)
	}
}
