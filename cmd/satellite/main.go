// SPDX-License-Identifier: Apache-2.0

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
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/uevent"
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

	// Optional: NETLINK_KOBJECT_UEVENT listener that wakes the
	// PhysicalDeviceDiscoveryRunnable on every block-device
	// mutation. The DaemonSet runs `privileged: true` so
	// CAP_NET_ADMIN is implicit — but if the open fails for any
	// reason (older kernel, non-standard runtime, dev binary
	// running on a non-Linux host), log it and continue with pure-
	// polling discovery. The udev path is an optimisation, not a
	// correctness requirement.
	//
	// `ueventListener` is typed as the controllers interface (not
	// the concrete *uevent.Listener) so the "open failed" path can
	// leave it as a true interface-nil. Assigning a typed-nil
	// *uevent.Listener into an interface variable produces a
	// non-nil interface holding a nil pointer — the discovery
	// runnable's `if p.Uevent != nil` guard would mis-fire and
	// dereference the nil pointer when calling Events().
	var ueventListener controllers.UeventNotifier

	listener, ueventErr := uevent.New(ctx)
	if ueventErr != nil {
		logger.Warn("uevent listener unavailable; falling back to pure-polling PhysicalDevice discovery",
			"err", ueventErr.Error())
	} else {
		// Bug 341: surface successful listener attach as an INFO log
		// so the operator can confirm the fast-path on every
		// satellite restart. Previously the success branch was
		// silent, making the "listener is up" and "listener was
		// never started" cases indistinguishable from logs alone.
		logger.Info("uevent listener wired into PhysicalDevice discovery")

		ueventListener = listener
	}

	// Providers map starts empty — the c-r `StoragePoolReconciler`
	// registers entries as it observes StoragePool CRDs (Phase 10.5
	// retired the bootstrap-from-flags path; Phase 10.6 retired the
	// gRPC `ApplyStoragePools` fallback).
	providers := map[string]storage.Provider{}

	// Cold-start kernel reconciliation. ORDER MATTERS and the two
	// steps share one decision: which kernel resources are *healthy*
	// (carry recoverable local data or a live peer) and must survive
	// the restart untouched.
	//
	// Bug (P0 production safety): a satellite pod restart (rolling
	// upgrade, OOM, node drain) must NOT tear down replicas that are
	// running fine. The previous unconditional `drbdsetup down` +
	// `.res` wipe ripped down a HEALTHY replica that happened to be
	// mid-initial-sync (local disk Inconsistent as SyncTarget); the
	// teardown then raced the reconciler queue and the replica never
	// re-adjusted. Upstream LINSTOR's DrbdLayer never downs kernel
	// state just because the agent restarted — it relies on `drbdadm
	// adjust` to reconcile. We do the same: reap only genuine orphans
	// (no usable local disk AND no healthy connection) and leave every
	// UpToDate / SyncTarget / SyncSource / Established / Consistent /
	// Outdated replica for the reconciler to adjust.
	healthy := healthyKernelResources(ctx, logger)

	// Tear down only the kernel resources that are NOT in the healthy
	// set. Without this, a reconcile that re-allocates a node-id
	// (e.g. the dispatcher picked a different id after a peer joined /
	// left) would hit "peer node id cannot be my own node id" on a
	// genuinely orphaned slot — but a live replica's node-id is
	// reconciled by `drbdadm adjust`, not by a destructive down.
	cleanKernelState(ctx, logger, healthy)

	// Wipe stale .res files, but PRESERVE the ones backing a healthy
	// kernel resource. The c-r reconciler re-renders every Resource
	// CRD on this node shortly after startup, but a surviving kernel
	// resource needs its .res in place until that first render so an
	// interim `drbdadm adjust` has something to read. Orphan .res
	// files (no matching live kernel resource) are still wiped.
	cleanStateDir(stateDir, healthy, logger)

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
		ManagerFactory:         mgrFactory(ready, logger, ueventListener),
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
func mgrFactory(ready *readyState, logger *slog.Logger, ueventListener controllers.UeventNotifier) satellite.ManagerFactory {
	return func(restCfg *rest.Config, nodeName, probeAddr string, rec *satellite.Reconciler) (manager.Manager, error) {
		mgr, err := controllers.NewManager(restCfg, &controllers.Config{
			NodeName:               nodeName,
			Apply:                  rec,
			Exec:                   storage.RealExec{},
			HealthProbeBindAddress: probeAddr,
			// Optional: nil when `uevent.New` failed at startup —
			// PhysicalDeviceDiscoveryRunnable falls back to pure-
			// polling discovery.
			UeventListener: ueventListener,
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

// cleanStateDir wipes stale *.res files in dir on satellite startup,
// PRESERVING the ones that back a still-healthy kernel resource (keyed
// by the `<resource>.res` naming convention the dispatcher renders).
// The c-r reconciler re-renders every Resource CRD on this node shortly
// after startup, so wiped contents are reproducible — but a surviving
// kernel resource needs its .res in place until that first render. P0
// safety: wiping the .res of a healthy mid-sync replica left `drbdadm
// adjust` with nothing to read and contributed to the teardown race.
// Best-effort: log and continue on errors so a single missing dir
// doesn't stall the whole startup.
func cleanStateDir(dir string, healthy map[string]struct{}, logger *slog.Logger) {
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

		// Preserve the .res of a healthy kernel resource. The
		// dispatcher renders one file per resource as
		// `<resourceName>.res`, so strip the suffix to recover the
		// resource name and check it against the healthy set.
		resName := strings.TrimSuffix(name, ".res")
		if _, ok := healthy[resName]; ok {
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

// cleanKernelState detaches the genuinely-orphaned DRBD resources the
// kernel still holds from the previous satellite incarnation. Resources
// in the `healthy` set are LEFT ALONE — the c-r reconciler re-renders
// and `drbdadm adjust`s every Resource CRD shortly after, which is the
// non-destructive way to reconcile a stale node-id on a live replica.
// Best-effort: failures are logged + ignored.
//
// P0 safety (cold-start kernel handling): the previous version downed
// *every* kernel resource unconditionally. A satellite restart during
// initial-sync (rolling upgrade) therefore tore down a healthy replica
// that was mid-sync, and the recovery then raced the reconciler queue
// so the replica never re-adjusted. Upstream LINSTOR's DrbdLayer never
// downs kernel state purely because the agent restarted; we now mirror
// that and reap only true orphans.
//
// Why orphans still need reaping: a reconcile cycle that re-allocates a
// node-id (different dispatcher run after a peer left/joined) hits
// `peer node id cannot be my own node id` when a *dead* slot still pins
// the old node-id. A live replica's node-id is reconciled by adjust, so
// only orphan slots — no usable local disk AND no healthy peer — are
// downed here.
//
// Bug 274 (P1): bounded ctx — a wedged DRBD kernel (stuck-state
// pattern documented in `memory/blockstor_drbd_stuck_state.md`)
// hangs forever. Without a deadline the satellite startup never
// reaches `/readyz`, K8s rollout stalls, and the pod restart loop
// reproduces the same hang.
//
// Bug 285 (P1): enumerate via `drbdsetup status` (kernel-state, no
// .res files needed) and `drbdsetup down <name>` per resource with its
// own short timeout so one wedged resource doesn't starve the rest.
func cleanKernelState(ctx context.Context, logger *slog.Logger, healthy map[string]struct{}) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	names, err := listKernelResources(ctx)
	if err != nil {
		logger.Info("drbdsetup status (best-effort)", "err", err.Error())

		return
	}

	if len(names) == 0 {
		return
	}

	var orphans []string

	for _, name := range names {
		if _, ok := healthy[name]; ok {
			continue
		}

		orphans = append(orphans, name)
	}

	if len(orphans) == 0 {
		logger.Info("cold-start: all kernel resources healthy, leaving for reconciler to adjust",
			"resources", names)

		return
	}

	logger.Info("cold-start: tearing down orphan kernel resources (healthy ones preserved)",
		"orphans", orphans,
		"all", names)

	// Per-resource timeout: one stuck resource must not starve the
	// rest. 5 s is generous for a healthy `drbdsetup down` and short
	// enough that the outer 60 s ctx still gets through a dozen
	// orphans before bailing.
	const perResourceTimeout = 5 * time.Second

	for _, name := range orphans {
		downCtx, downCancel := context.WithTimeout(ctx, perResourceTimeout)
		out, err := exec.CommandContext(downCtx, "drbdsetup", "down", name).CombinedOutput()
		downCancel()

		if err != nil {
			logger.Info("drbdsetup down (best-effort)",
				"resource", name,
				"err", err.Error(),
				"output", strings.TrimSpace(string(out)))

			continue
		}

		if trimmed := strings.TrimSpace(string(out)); trimmed != "" {
			logger.Info("drbdsetup down", "resource", name, "output", trimmed)
		}
	}
}

// listKernelResources enumerates every resource name the local DRBD
// kernel currently owns. Mirrors pkg/drbd.Adm.StatusResources but
// re-implemented here to keep cmd/satellite/main.go's startup path
// import-light (it would otherwise pull the whole drbd package +
// its storage.Exec dependency just for this one call).
//
// Output convention: every resource block starts at column 0 with
// `<name> role:<role> [...]`; per-volume / per-peer lines are
// indented. Empty output ("No currently configured DRBD found." +
// non-zero exit) means a fresh kernel and returns nil, nil.
func listKernelResources(ctx context.Context) ([]string, error) {
	out, err := exec.CommandContext(ctx, "drbdsetup", "status").CombinedOutput()
	if err != nil {
		if strings.Contains(string(out), "No currently configured DRBD") {
			return nil, nil
		}

		return nil, errors.Wrap(err, "drbdsetup status")
	}

	var names []string

	for line := range strings.SplitSeq(string(out), "\n") {
		if line == "" || line[0] == ' ' || line[0] == '\t' {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}

		// Skip comment / banner lines (Bug 264 guard).
		name := fields[0]
		if strings.HasPrefix(name, "#") {
			continue
		}

		names = append(names, name)
	}

	return names, nil
}

// healthyKernelResources returns the set of resource names the local
// DRBD kernel holds that are HEALTHY and must survive a satellite
// restart untouched. A resource is healthy when it carries recoverable
// local data (any volume with a disk-state other than
// Diskless/Failed/DUnknown) OR has a live peer connection
// (Connected, or a SyncSource/SyncTarget/Established replication-state).
//
// This is the conservative gate for the P0 cold-start safety fix: the
// reconciler will `drbdadm adjust` every Resource CRD on this node
// shortly after startup, so cold-start should only reap genuine
// orphans. SyncTarget-during-initial-sync (local disk Inconsistent) is
// the canonical replica this protects — it is healthy and recovering,
// and a destructive down would lose the in-flight sync and race the
// reconciler queue.
//
// Best-effort: any probe failure for a resource omits it from the
// healthy set (treated as not-known-healthy). That is the conservative
// direction relative to the *old* behaviour for orphans (still reaped),
// but note a transient `drbdsetup status -j` failure on a genuinely
// healthy resource would leave it eligible for teardown — acceptable
// because the per-resource probe is a local kernel call that does not
// fail in practice, and the alternative (assume-healthy-on-error) would
// let real orphans survive forever, reproducing Bug 285.
func healthyKernelResources(ctx context.Context, logger *slog.Logger) map[string]struct{} {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	names, err := listKernelResources(ctx)
	if err != nil {
		logger.Info("cold-start health probe: drbdsetup status (best-effort)", "err", err.Error())

		return nil
	}

	if len(names) == 0 {
		return nil
	}

	healthy := make(map[string]struct{}, len(names))

	for _, name := range names {
		ok, perr := resourceIsHealthy(ctx, name)
		if perr != nil {
			logger.Info("cold-start health probe failed; treating as orphan",
				"resource", name,
				"err", perr.Error())

			continue
		}

		if ok {
			healthy[name] = struct{}{}
		}
	}

	return healthy
}

// statusVolume / statusConnection / statusPeerDevice / statusResource
// model the subset of `drbdsetup status -j <res>` the cold-start health
// gate reasons about: local per-volume disk-state and per-connection
// runtime. Field names are the verbatim drbd-utils JSON keys.
type statusVolume struct {
	DiskState string `json:"disk-state"`
}

type statusPeerDevice struct {
	ReplicationState string `json:"replication-state"`
}

type statusConnection struct {
	ConnectionState string             `json:"connection-state"`
	PeerDevices     []statusPeerDevice `json:"peer_devices"`
}

type statusResource struct {
	Devices     []statusVolume     `json:"devices"`
	Connections []statusConnection `json:"connections"`
}

// resourceIsHealthy probes one resource via `drbdsetup status -j` and
// classifies it. Returns true when the resource carries recoverable
// local data or has a live peer — see healthyKernelResources for the
// policy rationale.
func resourceIsHealthy(ctx context.Context, name string) (bool, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	out, err := exec.CommandContext(probeCtx, "drbdsetup", "status", "-j", name).CombinedOutput()
	if err != nil {
		// "Unknown resource" / "No currently configured DRBD" means
		// the resource vanished between the enumerate and the probe —
		// not healthy, but not an error worth surfacing.
		text := string(out)
		if strings.Contains(text, "Unknown resource") ||
			strings.Contains(text, "No currently configured DRBD") {
			return false, nil
		}

		return false, errors.Wrapf(err, "drbdsetup status -j %s: %s", name, strings.TrimSpace(text))
	}

	var root []statusResource
	if jerr := json.Unmarshal(out, &root); jerr != nil {
		return false, errors.Wrapf(jerr, "parse drbdsetup status -j %s", name)
	}

	if len(root) == 0 {
		return false, nil
	}

	return resourceHealthyFromStatus(&root[0]), nil
}

// resourceHealthyFromStatus is the pure-data classifier, split out so
// it can be unit-tested against captured JSON without exec.
//
// Healthy when EITHER:
//   - any local volume has recoverable data — disk-state is something
//     other than Diskless/Failed/DUnknown (i.e. UpToDate, Consistent,
//     Outdated, Inconsistent, Negotiating, Attaching), OR
//   - any peer connection is live — Connected, or a peer-device whose
//     replication-state indicates an active session
//     (Established / SyncSource / SyncTarget / and the other resync
//     phases). A live peer means the reconciler's adjust can finish the
//     handshake; tearing it down would flap a working connection.
func resourceHealthyFromStatus(res *statusResource) bool {
	for i := range res.Devices {
		switch res.Devices[i].DiskState {
		case "", "Diskless", "Failed", "DUnknown":
			// No recoverable local data on this volume.
		default:
			return true
		}
	}

	for i := range res.Connections {
		c := &res.Connections[i]
		if c.ConnectionState == "Connected" {
			return true
		}

		for j := range c.PeerDevices {
			if replicationStateIsLive(c.PeerDevices[j].ReplicationState) {
				return true
			}
		}
	}

	return false
}

// replicationStateIsLive reports whether a peer-device replication-state
// indicates an active replication session that the reconciler should be
// allowed to finish rather than have torn down at cold start. Off — the
// "no active session" sentinel — and the empty string are the only
// not-live values.
func replicationStateIsLive(s string) bool {
	switch s {
	case "", "Off":
		return false
	default:
		return true
	}
}
